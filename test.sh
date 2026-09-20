#!/bin/sh
# Run all Tornado unit test modules one by one (manual test script).
#
# Usage:
#   ./test.sh                run every unit test module
#   ./test.sh web_test       run only the given module(s)
#
# Exits non-zero if any module fails.  Modules that need optional
# dependencies (pycurl, twisted) are skipped automatically by the
# test runner.

cd $(dirname $0)

PYTHON=${PYTHON:-python}

ALL_MODULES="
asyncio_test
auth_test
autoreload_test
circlerefs_test
concurrent_test
curl_httpclient_test
escape_test
gen_test
http1connection_test
httpclient_test
httpserver_test
httputil_test
import_test
ioloop_test
iostream_test
locale_test
locks_test
log_test
netutil_test
options_test
process_test
queues_test
routing_test
simple_httpclient_test
tcpclient_test
tcpserver_test
template_test
testing_test
twisted_test
util_test
web_test
websocket_test
wsgi_test
"

if [ $# -gt 0 ]; then
    MODULES="$@"
else
    MODULES="$ALL_MODULES"
fi

FAILED=""
for m in $MODULES; do
    case "$m" in
        tornado.test.*) mod="$m" ;;
        *)              mod="tornado.test.$m" ;;
    esac
    echo "======================================================================"
    echo "Running $mod"
    echo "======================================================================"
    if ! $PYTHON -m tornado.test.runtests "$mod"; then
        FAILED="$FAILED $mod"
    fi
done

echo "======================================================================"
if [ -n "$FAILED" ]; then
    echo "FAILED modules:$FAILED"
    exit 1
fi
echo "All unit test modules passed."
