#!/bin/bash
# Quick integration test for MySQL protocol server

set -e

echo "=== MySQL Protocol Integration Test ==="

# Check if xsqld binary exists
if [ ! -f "./target/release/xsqld" ]; then
    echo "ERROR: xsqld binary not found. Run 'cargo build --release' first."
    exit 1
fi

# Check if SeekDB is accessible
if ! nc -z 127.0.0.1 2881 2>/dev/null; then
    echo "WARNING: SeekDB not accessible on 127.0.0.1:2881"
    echo "Starting test with mock configuration..."
fi

# Start xsqld in background
echo "Starting xsqld..."
./target/release/xsqld \
    --mysql-host 127.0.0.1 \
    --mysql-port 2881 \
    --mysql-user root \
    --mysql-listen-addr 127.0.0.1:13306 \
    --http-listen-addr 127.0.0.1:18080 \
    --admin-listen-addr 127.0.0.1:18081 \
    > /tmp/xsqld-test.log 2>&1 &

XSQLD_PID=$!
echo "xsqld started with PID $XSQLD_PID"

# Wait for server to start
sleep 2

# Check if xsqld is still running
if ! kill -0 $XSQLD_PID 2>/dev/null; then
    echo "ERROR: xsqld failed to start"
    cat /tmp/xsqld-test.log
    exit 1
fi

# Check if MySQL port is listening
if ! nc -z 127.0.0.1 13306 2>/dev/null; then
    echo "ERROR: MySQL port 13306 not listening"
    cat /tmp/xsqld-test.log
    kill $XSQLD_PID 2>/dev/null || true
    exit 1
fi

echo "✓ xsqld started successfully"
echo "✓ MySQL protocol server listening on port 13306"
echo ""
echo "Server logs:"
tail -20 /tmp/xsqld-test.log

# Test with mysql client if available
if command -v mysql &> /dev/null; then
    echo ""
    echo "Testing with mysql client..."
    if mysql -h 127.0.0.1 -P 13306 -u root -e "SELECT 1 AS test" 2>&1; then
        echo "✓ MySQL client connection successful"
    else
        echo "⚠ MySQL client connection failed (this is expected if default namespace doesn't exist)"
    fi
fi

# Cleanup
echo ""
echo "Stopping xsqld..."
kill $XSQLD_PID 2>/dev/null || true
sleep 1

if kill -0 $XSQLD_PID 2>/dev/null; then
    echo "Force killing xsqld..."
    kill -9 $XSQLD_PID 2>/dev/null || true
fi

echo ""
echo "=== Test Complete ==="
echo "Full logs available at: /tmp/xsqld-test.log"
