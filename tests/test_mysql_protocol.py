#!/usr/bin/env python3
"""Checks the MySQL wire protocol server with a hand-written client.

Deliberately uses no driver: what matters here is the framing a driver would hide -- capability
negotiation, sequence numbers, the DEPRECATE_EOF swap of EOF for OK, packet reassembly past 2^24-1
bytes and the errno/SQLSTATE a client branches on.

Needs xsqld listening with --mysql-listen-addr and a backend reachable from it:
    cargo run -- --mysql-host 127.0.0.1 --mysql-port 3307 --mysql-user root \
        --http-listen-addr 127.0.0.1:8090 --mysql-listen-addr 127.0.0.1:3308
    python3 tests/test_mysql_protocol.py
"""

import os
import socket
import struct
import sys

HOST = os.environ.get("XS_MYSQL_HOST", "127.0.0.1")
PORT = int(os.environ.get("XS_MYSQL_PORT", "3308"))
NAMESPACE = os.environ.get("XS_NAMESPACE", "default")

CLIENT_PROTOCOL_41 = 0x00000200
CLIENT_SECURE_CONNECTION = 0x00008000
CLIENT_PLUGIN_AUTH = 0x00080000
CLIENT_CONNECT_WITH_DB = 0x00000008
CLIENT_DEPRECATE_EOF = 0x01000000

failures = []


def check(name, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {name}{'  -> ' + detail if detail else ''}")
    if not ok:
        failures.append(name)


def send(sock, payload, seq):
    sock.sendall(struct.pack("<I", len(payload))[:3] + bytes([seq]) + payload)


def recv(sock):
    """Returns (payload, sequence, continued) -- continued is set by a full-size payload."""
    header = _exactly(sock, 4)
    if header is None:
        return None, None, False
    length = struct.unpack("<I", header[:3] + b"\x00")[0]
    payload = b"" if length == 0 else _exactly(sock, length)
    if payload is None:
        return None, None, False
    return payload, header[3], length == 0xFFFFFF


def _exactly(sock, count):
    buf = bytearray()
    while len(buf) < count:
        chunk = sock.recv(count - len(buf))
        if not chunk:
            return None
        buf += chunk
    return bytes(buf)


def read_message(sock):
    """Reassembles a payload split across packets of the maximum size."""
    payload, seq, continued = recv(sock)
    if payload is None:
        return None, None
    while continued:
        more, _, continued = recv(sock)
        if more is None:
            return None, None
        payload += more
    return payload, seq


def auth_packet(caps, user, database=None):
    pkt = bytearray()
    pkt += struct.pack("<I", caps)
    pkt += struct.pack("<I", 16 * 1024 * 1024)
    pkt += bytes([45])  # utf8mb4_general_ci
    pkt += bytes(23)
    pkt += user.encode() + b"\x00"
    pkt += b"\x14" + bytes(20)  # a native-password scramble of the right length
    if database is not None and caps & CLIENT_CONNECT_WITH_DB:
        pkt += database.encode() + b"\x00"
    if caps & CLIENT_PLUGIN_AUTH:
        pkt += b"mysql_native_password\x00"
    return bytes(pkt)


def drain(sock):
    """Reads past a result set until its terminating EOF or OK."""
    while True:
        payload, _ = read_message(sock)
        if payload is None:
            return None
        if payload[0] in (0x00, 0xFE) and len(payload) < 16:
            return payload


def parse_handshake(hs):
    """Protocol::HandshakeV10, field by field, so the layout itself is under test."""
    end = hs.index(b"\x00", 1)
    p = end + 1
    conn_id = struct.unpack("<I", hs[p:p + 4])[0]
    scramble1 = hs[p + 4:p + 12]
    filler = hs[p + 12]
    caps = struct.unpack("<H", hs[p + 13:p + 15])[0]
    charset = hs[p + 15]
    status = struct.unpack("<H", hs[p + 16:p + 18])[0]
    caps |= struct.unpack("<H", hs[p + 18:p + 20])[0] << 16
    salt_len = hs[p + 20]
    p += 31  # one byte length, ten reserved bytes
    part2 = 12 if caps & CLIENT_PLUGIN_AUTH else max(salt_len - 8, 0)
    plugin = hs[p + part2 + 1:].split(b"\x00")[0].decode() if caps & CLIENT_PLUGIN_AUTH else ""
    return {
        "version": hs[1:end].decode(),
        "conn_id": conn_id,
        "salt": scramble1 + hs[p:p + part2],
        "filler": filler,
        "caps": caps,
        "charset": charset,
        "status": status,
        "salt_len": salt_len,
        "plugin": plugin,
    }


def raw_connect(caps, user=NAMESPACE, database=None):
    """Handshake plus auth, returning the reply instead of asserting on it."""
    sock = socket.create_connection((HOST, PORT), timeout=60)
    handshake, seq = read_message(sock)
    if handshake is None or handshake[0] != 10:
        raise AssertionError(f"no HandshakeV10: {handshake!r}")
    info = parse_handshake(handshake)
    send(sock, auth_packet(caps & info["caps"], user, database), seq + 1)
    reply, _ = read_message(sock)
    return sock, info, reply


def connect(caps, user=NAMESPACE, database=None):
    sock, info, reply = raw_connect(caps, user, database)
    if reply is None or reply[0] != 0x00:
        sock.close()
        raise AssertionError(f"auth was not accepted: {reply!r}")
    return sock, info


def query(sock, sql):
    send(sock, b"\x03" + sql.encode(), 0)
    return read_message(sock)


def main():
    try:
        sock, info = connect(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH,
                             database=None)
    except OSError as e:
        print(f"cannot reach xsqld's MySQL listener on {HOST}:{PORT}: {e}", file=sys.stderr)
        print("start it with --mysql-listen-addr and set XS_MYSQL_PORT if it differs", file=sys.stderr)
        return 2

    check("handshake reports the backend version", info["version"].count(".") >= 2, info["version"])
    check("handshake names the native password plugin",
          info["plugin"] == "mysql_native_password", repr(info["plugin"]))
    check("handshake advertises a 21-byte scramble and delivers 20 bytes of it",
          info["salt_len"] == 21 and len(info["salt"]) == 20,
          f"salt_len={info['salt_len']} salt={len(info['salt'])}")
    check("capabilities are split across both halves of the packet",
          info["caps"] & CLIENT_PROTOCOL_41 and info["caps"] & CLIENT_PLUGIN_AUTH
          and info["caps"] & CLIENT_DEPRECATE_EOF, hex(info["caps"]))
    check("the filler byte is zero, as clients require", info["filler"] == 0, str(info["filler"]))
    check("no SSL and no compression are offered", not info["caps"] & (0x00000800 | 0x40000000),
          hex(info["caps"]))

    # ---------- the namespace is settled during the handshake ----------
    refused, _, reply = raw_connect(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION,
                                    user="no_such_namespace_zz")
    errno = struct.unpack("<H", reply[1:3])[0] if reply and reply[0] == 0xFF else None
    check("an unknown namespace is refused during the handshake", errno == 1049, repr(reply[:16]))
    refused.close()

    with_db, _ = connect(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB,
                         user="whoever", database=NAMESPACE)
    payload, _ = query(with_db, "SELECT 1 AS one")
    check("the -D database selects the namespace, the user does not have to",
          payload == b"\x01", repr(payload))
    drain(with_db)

    # ---------- a client that leads with an SSL request is refused, not mis-parsed ----------
    try:
        tls = socket.create_connection((HOST, PORT), timeout=20)
        hs, seq = read_message(tls)
        send(tls, struct.pack("<I", CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | 0x00000800)
             + struct.pack("<I", 16 * 1024 * 1024) + bytes([45]) + bytes(23), seq + 1)
        reply, _ = read_message(tls)
        errno = struct.unpack("<H", reply[1:3])[0] if reply and reply[0] == 0xFF else None
        check("an SSL request gets ER_NOT_SUPPORTED_YET, not a garbled handshake",
              errno == 1251, repr(reply[:14]))
        tls.close()
    except OSError as e:
        check("an SSL request gets ER_NOT_SUPPORTED_YET, not a garbled handshake", False, str(e))

    # ---------- text result set, EOF framing ----------
    payload, seq = query(sock, "SELECT 1 AS one")
    check("column count comes first", payload == b"\x01", repr(payload))
    coldef, _ = read_message(sock)
    # def / schema / table / org_table / name / org_name then a 12-byte fixed block
    fixed = coldef[coldef.index(b"\x0c"):]
    check("column definition fixed block is 0x0c then 12 bytes",
          len(fixed) == 13 and fixed[1:3] == struct.pack("<H", 45) and fixed[3:7] != b"",
          f"{fixed.hex()}")
    check("an integer column is not sized smaller than the value", fixed[7] in (0x08, 0x03),
          f"type={fixed[7]}")
    eof, _ = read_message(sock)
    check("EOF terminates the columns when DEPRECATE_EOF is off", eof[0] == 0xFE, repr(eof[:8]))
    row, _ = read_message(sock)
    check("the row arrives as one length-encoded value", row == b"\x011", repr(row))
    term, _ = read_message(sock)
    check("EOF terminates the rows", term[0] == 0xFE, repr(term[:8]))

    # ---------- sequence numbers ----------
    send(sock, b"\x0e", 0)
    _, ping_seq = read_message(sock)
    check("reply sequence follows the command", ping_seq == 1, str(ping_seq))

    # ---------- DEPRECATE_EOF swaps both EOF packets for OK ----------
    deof = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_DEPRECATE_EOF
    sock2, _ = connect(deof)
    payload, _ = query(sock2, "SELECT 1 AS one")
    check("DEPRECATE_EOF keeps the column count", payload == b"\x01", repr(payload))
    read_message(sock2)  # column definition
    after_col, _ = read_message(sock2)
    check("DEPRECATE_EOF replaces the column EOF with OK", after_col[0] == 0x00, repr(after_col[:8]))
    row, _ = read_message(sock2)
    after_row, _ = read_message(sock2)
    check("DEPRECATE_EOF replaces the row EOF with OK", after_row[0] == 0x00 and row == b"\x011",
          f"row={row!r} term={after_row[:6]!r}")

    # ---------- errors carry the backend errno ----------
    payload, _ = query(sock, "SELECT * FROM no_such_table_zz")
    errno = struct.unpack("<H", payload[1:3])[0]
    check("a missing table is ER_NO_SUCH_TABLE", payload[0] == 0xFF and errno == 1146, str(errno))
    check("the error packet carries a SQLSTATE", payload[3:4] == b"#" and payload[4:9] == b"42S02",
          payload[3:9].decode("ascii", "replace"))

    # ---------- a value larger than one packet ----------
    payload, _ = query(sock2, "SELECT REPEAT('a', 4000000) AS big")
    read_message(sock2)  # column definition
    read_message(sock2)  # the OK that replaces the column EOF
    row, _ = read_message(sock2)
    check("a row over 2^24-1 bytes is reassembled", len(row) > 4_000_000, str(len(row)))
    drain(sock2)

    # ---------- namespace selection ----------
    send(sock2, b"\x02" + b"no_such_namespace_zz", 0)
    payload, _ = read_message(sock2)
    check("COM_INIT_DB refuses an unknown namespace", payload[0] == 0xFF
          and struct.unpack("<H", payload[1:3])[0] == 1049, repr(payload[:20]))

    send(sock, b"\x01", 0)
    send(sock2, b"\x01", 0)
    sock.close()
    sock2.close()

    print()
    if failures:
        print(f"{len(failures)} check(s) failed: {', '.join(failures)}")
        return 1
    print("all protocol checks passed")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as e:
        print(f"FAIL  the listener did not speak the protocol: {e}")
        sys.exit(1)
