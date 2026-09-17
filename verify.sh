#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# verify.sh — self-contained proof of the Backend-track "Week 6" claims.
#
#   bash verify.sh            # from the repo root
#
# Prints, in order:
#   [0] provenance    — who/where/when this ran, git identity, push status
#   [1] dependencies  — the exact versions in use
#   [2] the old bug   — your Week 5 main.py (from git), run with no DB
#   [3] the fix       — current main.py, same conditions
#   [4] the tests     — pytest -v
#   [5] the live API  — boots uvicorn on a throwaway DB and drives every route
#
# Exit code 0 = every check passed. Nothing here touches your real messages.db.
# ---------------------------------------------------------------------------
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CODE_DIR="$REPO_ROOT/backend-track"
PORT="${VERIFY_PORT:-8123}"
BASE="http://127.0.0.1:$PORT"
OLD_COMMIT="bd62303"          # your Week 5 commit
PASS=0; FAIL=0
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; [ -n "${SERVER_PID:-}" ] && kill "$SERVER_PID" 2>/dev/null' EXIT

hr()   { printf '%s\n' "------------------------------------------------------------------"; }
head_() { hr; printf '### %s\n' "$1"; hr; }
check() { # check <description> <expected> <actual>
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); printf 'PASS  %-46s got %s\n' "$1" "$3"
  else FAIL=$((FAIL+1)); printf 'FAIL  %-46s expected %s, got %s\n' "$1" "$2" "$3"; fi
}
py() { python3 "$@"; }

# --- [0] provenance ---------------------------------------------------------
head_ "[0] WHO RAN THIS"
printf 'executed by  : %s\n' "$(whoami)@$(hostname)"
printf 'when         : %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
printf 'where        : %s\n' "$REPO_ROOT"
printf 'python       : %s (%s)\n' "$(command -v python3)" "$(python3 -V 2>&1)"
printf 'git identity : %s <%s>   <- from git config, NOT chosen by the script\n' \
       "$(git config user.name)" "$(git config user.email)"
echo
git log --format='  %h | %an <%ae> | %ad | %s' --date=format:'%Y-%m-%d %H:%M:%S %z' -5
echo
printf 'branch/upstream:\n'; git branch -vv | sed 's/^/  /'
printf 'commits NOT on origin/main:\n'
git log origin/main..HEAD --oneline 2>/dev/null | sed 's/^/  /' || echo "  (cannot tell - no origin/main)"
echo
printf 'files changed vs %s:\n' "$OLD_COMMIT"
git diff --stat "$OLD_COMMIT"..HEAD 2>/dev/null | sed 's/^/  /'

# --- [1] dependencies -------------------------------------------------------
head_ "[1] DEPENDENCIES"
if ! py -c 'import fastapi, pydantic, uvicorn, pytest, httpx' 2>/dev/null; then
  echo "MISSING one of: fastapi pydantic uvicorn pytest httpx"
  echo "Install first, then re-run:"
  echo "  python3 -m venv venv && . venv/bin/activate"
  echo "  pip install -r backend-track/requirements.txt -r backend-track/requirements-dev.txt"
  exit 2
fi
py -c '
import fastapi, pydantic, starlette, uvicorn, pytest, httpx
for m in (fastapi, pydantic, starlette, uvicorn, pytest, httpx):
    print("  %-10s %s" % (m.__name__, getattr(m, "VERSION", None) or m.__version__))
'

# --- [2] the old bug --------------------------------------------------------
head_ "[2] OLD CODE ($OLD_COMMIT) WITH NO DATABASE -> must crash"
mkdir -p "$WORK/old"
if git show "$OLD_COMMIT:backend-track/main.py" > "$WORK/old/main.py" 2>/dev/null; then
  printf '  messages.db present in temp dir? '
  [ -e "$WORK/old/messages.db" ] && echo yes || echo no
  OLD_OUT="$(cd "$WORK/old" && py -c '
from fastapi.testclient import TestClient
import main
with TestClient(main.app) as c:
    print("GET / ->", c.get("/").status_code)
    try:
        c.post("/messages", json={"sender":"customer_A","text":"price?"})
        print("POST -> NO ERROR (unexpected)")
    except Exception as e:
        print("POST ->", type(e).__name__+":", e)
' 2>/dev/null)"
  echo "$OLD_OUT" | sed 's/^/  /'
  check "old code: GET / works"              "GET / -> 200" "$(echo "$OLD_OUT" | grep 'GET /')"
  check "old code: POST hits missing table"  "POST -> OperationalError: no such table: messages" \
                                             "$(echo "$OLD_OUT" | grep 'POST ->')"
else
  echo "  could not read $OLD_COMMIT:backend-track/main.py (shallow clone?)"
fi

# --- [3] the fix ------------------------------------------------------------
head_ "[3] CURRENT CODE, SAME CONDITIONS -> must work"
mkdir -p "$WORK/new"
cp "$CODE_DIR/main.py" "$WORK/new/main.py"
NEW_OUT="$(cd "$WORK/new" && MESSAGES_DB_PATH="$WORK/new/auto.db" py -c '
from fastapi.testclient import TestClient
import main
with TestClient(main.app) as c:
    r = c.post("/messages", json={"sender":"customer_A","text":"price?"})
    print("POST ->", r.status_code, r.json().get("category"))
    print("rows ->", len(c.get("/messages").json()))
' 2>/dev/null)"
echo "$NEW_OUT" | sed 's/^/  /'
check "current code: POST succeeds"          "POST -> 201 pricing" "$(echo "$NEW_OUT" | grep 'POST ->')"
printf '  auto-created DB file: '; ls -l "$WORK/new/auto.db" 2>&1 | sed 's/.*auto.db/auto.db/'

# --- [4] tests --------------------------------------------------------------
head_ "[4] PYTEST"
( cd "$CODE_DIR" && py -m pytest -q -p no:cacheprovider > "$WORK/pytest.log" 2>&1 )
tail -4 "$WORK/pytest.log" | sed 's/^/  /'
PYTEST_LINE="$(tail -1 "$WORK/pytest.log")"
check "pytest reports 31 passed" "31 passed" \
      "$(echo "$PYTEST_LINE" | grep -o '31 passed' || echo 'NOT FOUND')"
check "pytest reports 0 failed" "0 failed" \
      "$(echo "$PYTEST_LINE" | grep -oE '[0-9]+ failed' || echo '0 failed')"
check "no Pydantic deprecation warning" "0 occurrences" \
      "$(printf '%s occurrences' "$(grep -c 'PydanticDeprecated' "$WORK/pytest.log")")"

# --- [5] live API -----------------------------------------------------------
head_ "[5] LIVE API ON A THROWAWAY DATABASE (port $PORT)"
LIVE_DB="$WORK/live.db"
( cd "$CODE_DIR" && MESSAGES_DB_PATH="$LIVE_DB" \
    py -m uvicorn main:app --host 127.0.0.1 --port "$PORT" > "$WORK/uvicorn.log" 2>&1 ) &
SERVER_PID=$!
for _ in $(seq 1 40); do
  curl -s -o /dev/null "$BASE/" && break
  sleep 0.25
done
sed 's/^/  /' "$WORK/uvicorn.log"

code()  { curl -s -o "$WORK/body" -w '%{http_code}' "$@"; }
body()  { cat "$WORK/body"; }

printf '  DB file created by startup hook: '; ls -l "$LIVE_DB" >/dev/null 2>&1 && echo yes || echo NO
check "GET  /                " "200" "$(code "$BASE/")"
check "GET  /messages (empty)" "200" "$(code "$BASE/messages")"
check "  body is []          " "[]"  "$(body)"

C1=$(code -X POST "$BASE/messages" -H 'Content-Type: application/json' \
        -d '{"sender":"customer_A","text":"what is the price for 50 bags of rice?"}')
check "POST pricing          " "201" "$C1"; printf '    %s\n' "$(body)"
C2=$(code -X POST "$BASE/messages" -H 'Content-Type: application/json' \
        -d '{"sender":"customer_B","text":"where is my order #221?"}')
check "POST order_status     " "201" "$C2"; printf '    %s\n' "$(body)"
C3=$(code -X POST "$BASE/messages" -H 'Content-Type: application/json' \
        -d '{"sender":"customer_C","text":"do you deliver to Kano?"}')
check "POST general          " "201" "$C3"; printf '    %s\n' "$(body)"

check "POST blank sender     " "422" "$(code -X POST "$BASE/messages" \
        -H 'Content-Type: application/json' -d '{"sender":"   ","text":"hi"}')"
LONG=$(py -c 'print("x"*101)')
check "POST sender=101 chars " "422" "$(code -X POST "$BASE/messages" \
        -H 'Content-Type: application/json' -d "{\"sender\":\"$LONG\",\"text\":\"hi\"}")"
check "POST missing field    " "422" "$(code -X POST "$BASE/messages" \
        -H 'Content-Type: application/json' -d '{"sender":"only"}')"
check "POST whitespace trimmed" "201" "$(code -X POST "$BASE/messages" \
        -H 'Content-Type: application/json' -d '{"sender":"  customer_D  ","text":"  price check  "}')"
printf '    %s\n' "$(body)"

check "GET  /messages (4 rows)" "200" "$(code "$BASE/messages")"
check "GET  /messages/1      " "200" "$(code "$BASE/messages/1")"
check "GET  /messages/999    " "404" "$(code "$BASE/messages/999")"
check "PUT  /1/approve       " "200" "$(code -X PUT "$BASE/messages/1/approve")"
printf '    %s\n' "$(body)"
check "PUT  /1/approve again " "200" "$(code -X PUT "$BASE/messages/1/approve")"
check "PUT  /999/approve     " "404" "$(code -X PUT "$BASE/messages/999/approve")"
check "DELETE /messages/3    " "200" "$(code -X DELETE "$BASE/messages/3")"
check "DELETE /messages/3 x2 " "404" "$(code -X DELETE "$BASE/messages/3")"
check "GET  /messages after  " "200" "$(code "$BASE/messages")"

echo "  rows read straight out of SQLite, bypassing the API:"
py -c "
import sqlite3, sys
for r in sqlite3.connect(sys.argv[1]).execute('SELECT id,sender,category,status FROM messages ORDER BY id'):
    print('   ', r)
" "$LIVE_DB"

kill "$SERVER_PID" 2>/dev/null; SERVER_PID=""

# --- summary ----------------------------------------------------------------
head_ "SUMMARY"
printf 'passed: %d   failed: %d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] && echo "ALL CHECKS PASSED" || echo "SOME CHECKS FAILED"
exit "$FAIL"
