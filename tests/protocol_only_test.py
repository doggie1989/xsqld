#!/usr/bin/env python3
"""
Standalone MySQL protocol test - doesn't require SeekDB running.
Tests the protocol handshake, packet parsing, and basic communication.
"""

import socket
import struct
import sys

def encode_lenenc_int(val):
    if val < 251:
        return bytes([val])
    elif val < 2**16:
        return bytes([0xfc]) + struct.pack('<H', val)
    elif val < 2**24:
        return bytes([0xfd]) + struct.pack('<I', val)[:3]
    else:
        return bytes([0xfe]) + struct.pack('<Q', val)

def encode_lenenc_str(s):
    if isinstance(s, str):
        s = s.encode('utf-8')
    return encode_lenenc_int(len(s)) + s

def build_auth_response():
    """Build minimal MySQL auth response."""
    pkt = bytearray()
    pkt += struct.pack('<I', 0x00008205)  # capabilities
    pkt += struct.pack('<I', 16 * 1024 * 1024)  # max packet
    pkt += bytes([33])  # charset
    pkt += bytes(23)  # reserved
    pkt += b'root\x00'  # username
    pkt += b'\x00'  # no password
    return bytes(pkt)

def send_packet(sock, data, seq):
    pkt = struct.pack('<I', len(data))[:3] + bytes([seq]) + data
    sock.sendall(pkt)

def recv_packet(sock, timeout=5):
    sock.settimeout(timeout)
    try:
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
    except socket.timeout:
        return None, None

def test_protocol():
    """Test MySQL protocol handshake and connection."""
    print("=" * 60)
    print("MySQL Protocol Standalone Test")
    print("=" * 60)

    # Try to connect
    print("\n[1/5] Connecting to xsqld MySQL server on 127.0.0.1:13306...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', 13306))
        print("✓ Connection established")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        print("\nMake sure xsqld is running with:")
        print("  ./target/release/xsqld --mysql-listen-addr 127.0.0.1:13306")
        return False

    try:
        # Receive handshake
        print("\n[2/5] Reading handshake packet...")
        handshake, _ = recv_packet(sock)
        if not handshake:
            print("✗ No handshake received")
            return False

        if handshake[0] != 10:
            print(f"✗ Invalid protocol version: {handshake[0]} (expected 10)")
            return False

        # Extract server version
        version_end = handshake.find(b'\x00', 1)
        server_version = handshake[1:version_end].decode('utf-8', errors='ignore')
        print(f"✓ Handshake received (protocol v10, server: {server_version})")

        # Send auth response
        print("\n[3/5] Sending authentication response...")
        auth = build_auth_response()
        send_packet(sock, auth, 1)
        print("✓ Auth packet sent")

        # Receive OK or error
        print("\n[4/5] Waiting for auth response...")
        auth_resp, _ = recv_packet(sock)
        if not auth_resp:
            print("✗ No auth response received")
            return False

        if auth_resp[0] == 0x00:
            print("✓ Authentication successful (OK packet received)")
        elif auth_resp[0] == 0xff:
            error_code = struct.unpack('<H', auth_resp[1:3])[0]
            error_msg = auth_resp[9:].decode('utf-8', errors='ignore')
            print(f"✗ Authentication failed: {error_code} - {error_msg}")
            return False
        else:
            print(f"✗ Unexpected response: 0x{auth_resp[0]:02x}")
            return False

        # Send QUIT command
        print("\n[5/5] Sending COM_QUIT...")
        quit_pkt = bytes([1])  # COM_QUIT
        send_packet(sock, quit_pkt, 2)
        print("✓ QUIT command sent")

        sock.close()
        print("\n" + "=" * 60)
        print("✓ ALL TESTS PASSED - MySQL protocol working correctly")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"\n✗ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        try:
            sock.close()
        except:
            pass

if __name__ == '__main__':
    success = test_protocol()
    sys.exit(0 if success else 1)
