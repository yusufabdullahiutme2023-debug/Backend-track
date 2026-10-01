"""Shared fakes for the Pump worker tests.

Nothing here touches a provider. It offers a duck-typed store, a scripted websocket, transaction
and notification builders, and a local pair of servers (WebSocket + JSON-RPC over HTTP) that speak
just enough Solana for the real worker loop to run end to end.
"""
import asyncio
import http.server
import json
import threading

import websockets

from pump_replay import DISCRIMINATORS, PUMP_PROGRAM

ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
CREATE_LOGS = [f'Program {PUMP_PROGRAM} invoke [1]', 'Program log: Instruction: CreateV2',
               f'Program {PUMP_PROGRAM} success']
BUY_LOGS = [f'Program {PUMP_PROGRAM} invoke [1]', 'Program log: Instruction: Buy',
            f'Program {PUMP_PROGRAM} success']


def encode58(data):
    number = int.from_bytes(data, 'big')
    text = ''
    while number:
        number, rem = divmod(number, 58)
        text = ALPHABET[rem] + text
    return '1' * (len(data) - len(data.lstrip(b'\0'))) + text


def signature(n, prefix='sig'):
    """A unique 64-character stand-in signature (Launch requires 64-128 characters)."""
    return f'{prefix}{n:05d}'.ljust(64, 'x')


def creation_tx(sig, slot=100):
    """A getTransaction result that parse_launch accepts as a create_v2 launch."""
    mint, pool, user = ('M' + sig)[:40], ('P' + sig)[:40], ('U' + sig)[:40]
    keys = [mint, 'C' * 32, pool, 'A' * 32, 'G' * 32, 'H' * 32, user, 'Y' * 32, PUMP_PROGRAM]
    ix = {'programIdIndex': 8, 'accounts': [0, 3, 2, 4, 5, 6],
          'data': encode58(DISCRIMINATORS['create_v2'] + b'payload')}
    return {'slot': slot,
            'transaction': {'signatures': [sig], 'message': {
                'header': {'numRequiredSignatures': 7}, 'accountKeys': keys,
                'instructions': [ix]}},
            'meta': {'err': None, 'preTokenBalances': [], 'postTokenBalances': []}}


def plain_tx(sig, slot=100):
    """A successful transaction that is not a launch (no Pump instruction)."""
    return {'slot': slot,
            'transaction': {'signatures': [sig], 'message': {
                'header': {'numRequiredSignatures': 1}, 'accountKeys': ['Z' * 32],
                'instructions': []}},
            'meta': {'err': None, 'preTokenBalances': [], 'postTokenBalances': []}}


def notification(sig, logs, err=None, slot=100):
    return json.dumps({'jsonrpc': '2.0', 'method': 'logsNotification', 'params': {
        'result': {'context': {'slot': slot},
                   'value': {'signature': sig, 'err': err, 'logs': logs}},
        'subscription': 1}})


class MemoryStore:
    """Duck-typed stand-in for pump_worker.Store that records what would be written."""

    def __init__(self, cursor=None):
        self.cursor_value = cursor
        self.saves = []          # (signature, launch or None), in write order
        self.beats = []          # one dict per heartbeat
        self.stored = {}         # signature -> launch, like the primary-key table
        self.connects = 1
        self.fail_saves = 0      # raise on the next N saves
        self.prepared = False
        self.closed = False

    def prepare(self):
        self.prepared = True

    def cursor(self):
        return self.cursor_value

    def save(self, signature, launch):
        if self.fail_saves:
            self.fail_saves -= 1
            raise RuntimeError('database unavailable')
        self.saves.append((signature, launch))
        self.cursor_value = signature
        if launch is None or launch.signature in self.stored:
            return False                  # like ON CONFLICT DO NOTHING: a replay is not new
        self.stored[launch.signature] = launch
        return True

    def beat(self, status, started_at, last_notification_at, last_slot, stats):
        self.beats.append({'status': status, 'last_slot': last_slot, 'stats': dict(stats)})

    def close(self):
        self.closed = True

    @property
    def launches(self):
        """Distinct launches in first-write order (what the table would hold)."""
        return list(self.stored.values())


class Done(Exception):
    """Raised by FakeWS once its script is exhausted, to end a stream() call."""


class FakeWS:
    """Websocket stand-in: replays raw messages, then raises Done (or hangs)."""

    def __init__(self, messages, then='done'):
        self.messages = list(messages)
        self.then = then

    async def recv(self):
        if self.messages:
            return self.messages.pop(0)
        if self.then == 'hang':
            await asyncio.sleep(3600)
        raise Done()


class FakeNode:
    """The data a provider would serve over JSON-RPC, plus a log of what was asked."""

    def __init__(self):
        self.txs = {}              # signature -> getTransaction result
        self.listing = []          # newest-first [{'signature', 'err'}] for the program
        self.null_once = set()     # first getTransaction for these signatures returns null
        self.latency = 0.0
        self.calls = []            # (method, first parameter)
        self.lock = threading.Lock()

    def answer(self, method, params):
        with self.lock:
            self.calls.append((method, params[0] if params else None))
        if method == 'getTransaction':
            if self.latency:
                threading.Event().wait(self.latency)
            with self.lock:
                if params[0] in self.null_once:
                    self.null_once.discard(params[0])
                    return None
                return self.txs.get(params[0])
        if method == 'getSignaturesForAddress':
            options = params[1] if len(params) > 1 else {}
            with self.lock:
                listing = list(self.listing)
            start = 0
            if options.get('before'):
                names = [item['signature'] for item in listing]
                start = names.index(options['before']) + 1 if options['before'] in names else len(names)
            return listing[start:start + options.get('limit', 1000)]
        raise AssertionError(f'unexpected RPC method {method}')

    def count(self, method):
        with self.lock:
            return sum(1 for name, _ in self.calls if name == method)


class FakeRpcServer:
    """JSON-RPC over HTTP on a local port, backed by a FakeNode."""

    def __init__(self, node):
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                payload = json.dumps({'jsonrpc': '2.0', 'id': body['id'],
                                      'result': outer.node.answer(body['method'], body['params'])})
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload.encode())

            def log_message(self, *args):
                pass

        self.node = node
        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self):
        return f'http://127.0.0.1:{self.server.server_address[1]}/?api-key=test'

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()


class FakeStream:
    """A logsSubscribe endpoint. ``scripts`` holds one script per connection (the last one
    is reused); a script is a list of raw messages, callables (run for their side effects) and
    an optional trailing ``'close'``, which drops the connection after the last message.
    An ``asyncio.Event`` in a script is a gate: the script pauses there until the test sets it."""

    def __init__(self, scripts):
        self.scripts = list(scripts)
        self.connections = 0
        self.server = None

    async def handler(self, ws):
        script = self.scripts[min(self.connections, len(self.scripts) - 1)]
        self.connections += 1
        request = json.loads(await ws.recv())
        assert request['method'] == 'logsSubscribe'
        await ws.send(json.dumps({'jsonrpc': '2.0', 'result': self.connections, 'id': request['id']}))
        for message in script:
            if message == 'close':
                return
            if isinstance(message, asyncio.Event):
                await message.wait()      # a gate the test opens when it wants the script to go on
            elif callable(message):
                message()
            else:
                await ws.send(message)
        await ws.wait_closed()          # stay attached until the client leaves

    async def __aenter__(self):
        self.server = await websockets.serve(self.handler, '127.0.0.1', 0)
        return self

    async def __aexit__(self, *exc):
        self.server.close()
        await self.server.wait_closed()

    @property
    def url(self):
        return f'ws://127.0.0.1:{self.server.sockets[0].getsockname()[1]}'


async def eventually(predicate, timeout=5.0, interval=0.02):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError('condition not reached in time')
        await asyncio.sleep(interval)
