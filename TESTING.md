# xsqld MySQL Protocol Testing Guide

## Status

✅ **Compilation**: Successful - binary built at `./target/release/xsqld`  
✅ **Code Structure**: All modules compile without errors  
⚠️ **Runtime Testing**: Requires SeekDB/MySQL backend running

## Current Build

```bash
$ cargo build --release
   Compiling xsqld v1.1.0
    Finished `release` profile [optimized] target(s) in 43.57s

$ ./target/release/xsqld --help | grep mysql-listen
      --mysql-listen-addr <MYSQL_LISTEN_ADDR>
          MySQL protocol server for direct MySQL client connections [default: 127.0.0.1:3306]
```

## Test Requirements

### Prerequisites

1. **SeekDB/MySQL Backend**: Must be running on `127.0.0.1:2881`
   - Or any MySQL-compatible server
   - Configure with `--mysql-host`, `--mysql-port`, `--mysql-user`, `--mysql-password`

2. **MySQL Client** (optional, for manual testing):
   ```bash
   brew install mysql-client
   # or
   apt-get install mysql-client
   ```

3. **Python 3** (for automated tests):
   ```bash
   python3 --version  # Should be 3.7+
   ```

## Testing Procedures

### 1. Start SeekDB/MySQL Backend

```bash
# Example with Docker MySQL
docker run -d --name test-mysql \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_ALLOW_EMPTY_PASSWORD=yes \
  -p 2881:3306 \
  mysql:5.7

# Wait for startup
sleep 5

# Create default namespace
mysql -h 127.0.0.1 -P 2881 -u root -e "CREATE DATABASE IF NOT EXISTS \`default\`"
```

### 2. Start xsqld

```bash
./target/release/xsqld \
  --mysql-host 127.0.0.1 \
  --mysql-port 2881 \
  --mysql-user root \
  --mysql-password root \
  --mysql-listen-addr 127.0.0.1:3306 \
  --http-listen-addr 127.0.0.1:8080 \
  --admin-listen-addr 127.0.0.1:8081
```

Expected output:
```
INFO xsqld: connected to SeekDB / MySQL backend
INFO xsqld: xsqld started version="1.1.0" http=127.0.0.1:8080 admin=127.0.0.1:8081 mysql=127.0.0.1:3306
```

### 3. Test MySQL Protocol

#### Manual Test with mysql Client

```bash
# Connect via xsqld
mysql -h 127.0.0.1 -P 3306 -u root

# Run test queries
mysql> SELECT 1 AS test;
mysql> SELECT VERSION();
mysql> SHOW DATABASES;
mysql> USE `default`;
mysql> CREATE TABLE test (id INT, name VARCHAR(100));
mysql> INSERT INTO test VALUES (1, 'hello'), (2, 'world');
mysql> SELECT * FROM test;
```

#### Automated Protocol Test

```bash
# Protocol handshake test (doesn't require queries to work)
python3 tests/protocol_only_test.py

# Full integration test (requires working backend)
python3 tests/test_mysql_protocol.py
```

### 4. Test DuckDB Mirror Routing

```bash
# Start with mirror enabled
./target/release/xsqld \
  --mysql-host 127.0.0.1 \
  --mysql-port 2881 \
  --mysql-user root \
  --mirror-enabled \
  --mysql-listen-addr 127.0.0.1:3306

# In another terminal, populate data
mysql -h 127.0.0.1 -P 3306 -u root << EOF
CREATE DATABASE IF NOT EXISTS \`default\`;
USE \`default\`;
CREATE TABLE large_table (id INT, value VARCHAR(100));
INSERT INTO large_table SELECT n, CONCAT('value_', n) FROM (
  SELECT a.N + b.N * 10 + c.N * 100 AS n
  FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
       (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
       (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c
) nums LIMIT 1000;
EOF

# Wait for mirror sync (~1-2 seconds)
sleep 2

# Query should route to DuckDB (check logs for "DuckDB query executed")
mysql -h 127.0.0.1 -P 3306 -u root -e "SELECT COUNT(*) FROM large_table"
```

## Verification Checklist

- [ ] xsqld binary compiles successfully
- [ ] xsqld starts and connects to backend
- [ ] MySQL client can connect to xsqld port 3306
- [ ] Protocol handshake completes successfully
- [ ] Authentication succeeds (OK packet received)
- [ ] Simple SELECT query returns results
- [ ] INSERT/UPDATE/DELETE queries execute
- [ ] Column definitions properly serialized
- [ ] NULL values handled correctly
- [ ] Error messages returned as MySQL error packets
- [ ] Multiple concurrent connections work
- [ ] COM_QUIT gracefully closes connection
- [ ] Mirror routing works (when enabled)
- [ ] Fallback to SeekDB works when mirror unavailable

## Known Limitations

1. **Authentication**: Simplified - doesn't validate passwords
2. **Prepared Statements**: Not supported (COM_PREPARE, COM_EXECUTE)
3. **Transactions**: Limited support
4. **Data Types**: Most types converted to VARCHAR in protocol
5. **Result Sets**: Large result sets may be slow (no streaming yet)

## Troubleshooting

### "Connection refused" on startup

**Cause**: SeekDB/MySQL backend not accessible

**Solution**:
```bash
# Check if backend is running
nc -zv 127.0.0.1 2881

# Check xsqld logs
tail -f /path/to/xsqld.log
```

### "Access denied" errors

**Cause**: Backend authentication failed or namespace doesn't exist

**Solution**:
```bash
# Create default namespace in backend
mysql -h 127.0.0.1 -P 2881 -u root -e "CREATE DATABASE \`default\`"

# Or configure xsqld with correct credentials
./target/release/xsqld --mysql-password <password>
```

### Protocol handshake fails

**Cause**: MySQL client incompatible or protocol bug

**Solution**:
```bash
# Try with different MySQL client versions
mysql --version

# Check xsqld debug logs
RUST_LOG=debug ./target/release/xsqld ...
```

### DuckDB routing not working

**Cause**: Mirror not enabled or not synced yet

**Solution**:
```bash
# Ensure mirror is enabled
./target/release/xsqld --mirror-enabled

# Check mirror sync logs
grep "sync cycle" xsqld.log

# Verify table row counts meet threshold
# Default: --route-min-rows 200000
```

## Performance Testing

```bash
# Benchmark SELECT performance
time mysql -h 127.0.0.1 -P 3306 -u root -e "SELECT COUNT(*) FROM large_table"

# Compare with direct SeekDB access
time mysql -h 127.0.0.1 -P 2881 -u root -e "SELECT COUNT(*) FROM large_table"

# Test concurrent connections
for i in {1..10}; do
  mysql -h 127.0.0.1 -P 3306 -u root -e "SELECT $i" &
done
wait
```

## Next Steps

1. Set up CI/CD pipeline with Docker-based tests
2. Add more comprehensive test coverage
3. Performance benchmarking suite
4. Implement prepared statement support
5. Add SSL/TLS for MySQL protocol
6. Improve type marshalling for numeric/date types
