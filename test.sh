#!/bin/sh
# Manual test script for the web.py error-handling / output-chain fixes.
#
# Runs the new focused test module first, then the related existing
# regression suites. Requires permission to bind local TCP ports
# (the Integration* tests use real sockets).
set -e
cd "$(dirname "$0")"

PYTHON="${PYTHON:-python3}"

echo "=== [1/4] web error-handling tests (new) ==="
"$PYTHON" -m tornado.test.runtests tornado.test.web_error_handling_test

echo "=== [2/4] web_test (regression) ==="
"$PYTHON" -m tornado.test.runtests tornado.test.web_test

echo "=== [3/4] http1connection_test (regression) ==="
"$PYTHON" -m tornado.test.runtests tornado.test.http1connection_test

echo "=== [4/4] httpserver_test (regression) ==="
"$PYTHON" -m tornado.test.runtests tornado.test.httpserver_test

echo "All test suites passed."
