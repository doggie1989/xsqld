#!/usr/bin/env python3
"""Test MySQL protocol server in xsqld.

Requires:
  - xsqld running with --mysql-listen-addr 127.0.0.1:3306
  - SeekDB running on port 2881
"""

import socket
import struct
import sys
import time

def encode_lenenc_int(val):
    """Encode length-encoded integer."""
    if val < 251:
        return bytes([val])
    elif val < 2**16:
        return bytes([0xfc]) + struct.pack('<H', val)
    elif val < 2**24:
        return bytes([0xfd]) + struct.pack('<I', val)[:3]
    else:
        return bytes([0xfe]) + struct.pack('<Q', val)

def encode_lenenc_str(s):
    """Encode length-encoded string."""
    if isinstance(s, str):
        s = s.encode('utf-8')
    return encode_lenenc_int(len(s)) + s

def build_handshake_response(username, password):
    """Build MySQL handshake response (AUTH)."""
    pkt = bytearray()

    # Client capabilities
    pkt += struct.pack('<I', 0x00008205)  # CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | ...

    # Max packet size
    pkt += struct.pack('<I', 16 * 1024 * 1024)

    # Charset
    pkt += bytes([33])  # utf8mb4_unicode_ci

    # Reserved
    pkt += bytes(23)

    # Username
    pkt += username.encode('utf-8') + b'\x00'

    # Auth response (empty for no password)
    if password:
        auth = bytes.fromhex('00' * 20)  # Simplified: empty auth
        pkt += encode_lenenc_int(len(auth)) + auth
    else:
        pkt += b'\x00'

    # Database
    pkt += b'\x00'

    return bytes(pkt)

def build_query_packet(query):
    """Build COM_QUERY packet."""
    pkt = bytearray()
    pkt += bytes([3])  # COM_QUERY
    pkt += query.encode('utf-8')
    return bytes(pkt)

def build_quit_packet():
    """Build COM_QUIT packet."""
    return bytes([1])

def send_packet(sock, data, seq):
    """Send a MySQL packet."""
    pkt = struct.pack('<I', len(data))[:3] + bytes([seq]) + data
    sock.sendall(pkt)

def recv_packet(sock):
    """Receive a MySQL packet."""
    header = sock.recv(4)
    if len(header) < 4:
        return None, None

    length = struct.unpack('<I', header[:3] + b'\x00')[0]
    seq = header[3]

    data = b''
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            break
        data += chunk

    return data, seq

def test_basic_query():
    """Test basic MySQL query execution."""
    print("Connecting to xsqld MySQL server...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 3306))

    try:
        # Receive handshake
        print("Receiving handshake...")
        handshake, _ = recv_packet(sock)
        if not handshake or handshake[0] != 10:
            print(f"ERROR: Expected handshake, got {handshake[:20]}")
            return False

        # Send auth response
        print("Sending auth response...")
        auth = build_handshake_response('root', '')
        send_packet(sock, auth, 1)

        # Receive OK
        print("Receiving OK...")
        ok_pkt, _ = recv_packet(sock)
        if not ok_pkt or ok_pkt[0] != 0x00:
            print(f"ERROR: Expected OK, got {ok_pkt}")
            return False

        # Send query
        query = "SELECT 1 AS test_col"
        print(f"Sending query: {query}")
        query_pkt = build_query_packet(query)
        send_packet(sock, query_pkt, 2)

        # Receive column count
        print("Receiving column count...")
        col_count_pkt, _ = recv_packet(sock)
        if not col_count_pkt:
            print("ERROR: No response to query")
            return False

        col_count = col_count_pkt[0]
        print(f"Column count: {col_count}")

        # Receive column definitions
        for i in range(col_count):
            col_def, _ = recv_packet(sock)
            print(f"  Column {i}: {col_def[:50]}")

        # Receive EOF after columns
        eof_pkt, _ = recv_packet(sock)
        if not eof_pkt or eof_pkt[0] != 0xfe:
            print(f"ERROR: Expected EOF, got {eof_pkt}")
            return False

        # Receive rows
        print("Receiving rows...")
        row_count = 0
        while True:
            row_pkt, _ = recv_packet(sock)
            if not row_pkt:
                break
            if row_pkt[0] == 0xfe:  # EOF
                print(f"EOF after {row_count} rows")
                break
            row_count += 1
            print(f"  Row {row_count}: {row_pkt[:50]}")

        print(f"✓ Query executed successfully, {row_count} rows returned")

        # Send quit
        print("Sending QUIT...")
        send_packet(sock, build_quit_packet(), 3)

        return True

    finally:
        sock.close()

if __name__ == '__main__':
    try:
        if test_basic_query():
            print("\n✓ MySQL protocol test PASSED")
            sys.exit(0)
        else:
            print("\n✗ MySQL protocol test FAILED")
            sys.exit(1)
    except Exception as e:
        print(f"\n✗ MySQL protocol test ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
