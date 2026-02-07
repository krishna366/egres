import socket
import struct
import threading

SERVER = ("127.0.0.1", 8080)


def send_message(msg: bytes) -> bytes:
    with socket.create_connection(SERVER) as s:
        s.sendall(struct.pack(">I", len(msg)) + msg)

        # Read 4-byte length prefix
        header = recv_exact(s, 4)
        resp_len = struct.unpack(">I", header)[0]
        print(f"got length {resp_len}\n")
        return recv_exact(s, resp_len)


def recv_exact(s: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(f"expected {n} bytes, got {len(buf)}")
        buf += chunk
    return buf


def test_message(msg: str):
    raw = msg.encode()
    resp = send_message(raw)
    expected = raw.upper()
    assert resp == expected, f"expected {expected!r}, got {resp!r}"
    if len(msg) <= 80:
        print(f"  PASS: {msg!r} -> {resp.decode()!r}")
    else:
        print(f"  PASS: ({len(msg)} bytes) -> ({len(resp)} bytes)")


def test_large(size: int):
    msg = bytes([ord("a") + i % 26 for i in range(size)])
    resp = send_message(msg)
    expected = msg.upper()
    assert resp == expected, f"mismatch at byte {next(i for i,(a,b) in enumerate(zip(resp,expected)) if a!=b)}"
    print(f"  PASS: ({size} bytes) -> ({len(resp)} bytes)")


def test_concurrent(n: int):
    errors = []

    def worker(tid):
        try:
            test_message(f"thread {tid} says hello")
        except Exception as e:
            errors.append((tid, e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, f"failures: {errors}"
    print(f"  PASS: {n} concurrent connections")


if __name__ == "__main__":
    print("sequential:")
    test_message("hello world")
    test_message("zig is great")
    test_message("ALREADY UPPER")

    print("large messages:")
    test_large(8192)
    test_large(64 * 1024)

    print("concurrent:")
    test_concurrent(16)

    print("\nall tests passed.")
