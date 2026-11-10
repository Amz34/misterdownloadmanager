"""
Enterprise QA Test Suite for Chunked Range Downloading and Concurrency Emulation.
Simulates an HTTP/1.1 Range-compliant server and validates partial chunk aggregation.
"""

import http.server
import threading
import urllib.request
import hashlib
import time
import sys
import os

TEST_DATA = b"MISTER_DOWNLOAD_MANAGER_ENTERPRISE_QA_VERIFICATION_PAYLOAD_" * 1024 * 64  # ~3.7 MB


class RangeRequestHandler(http.server.BaseHTTPRequestHandler):
    """HTTP Request Handler supporting RFC 7233 range requests."""

    def do_GET(self):
        """Handles GET requests with full byte-range slicing compliance."""
        range_header = self.headers.get("Range")
        total_len = len(TEST_DATA)

        if not range_header:
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(total_len))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(TEST_DATA)
            return

        try:
            byte_range = range_header.strip().split("=")[1]
            parts = byte_range.split("-")

            # Suffix range: bytes=-500
            if not parts[0] and parts[1]:
                length = int(parts[1])
                start = max(0, total_len - length)
                end = total_len - 1
            elif parts[0] and not parts[1]:
                start = int(parts[0])
                end = total_len - 1
            else:
                start = int(parts[0])
                end = int(parts[1])

            # Check for unsatisfiable range
            if start > end or start >= total_len:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{total_len}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            end = min(end, total_len - 1)
            chunk_len = (end - start) + 1

            self.send_response(206)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Range", f"bytes {start}-{end}/{total_len}")
            self.send_header("Content-Length", str(chunk_len))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(TEST_DATA[start:end + 1])
        except Exception:
            self.send_response(400)
            self.send_header("Content-Length", "0")
            self.end_headers()

    def log_message(self, format, *args):
        """Silences test server console logs."""
        pass


def start_server():
    """Starts the ThreadingHTTPServer on an ephemeral port (port 0)."""
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), RangeRequestHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server


def run_qa_benchmark(port, concurrency=8):
    """Executes multi-chunk concurrent range fetch and validates SHA256 integrity."""
    print(f"[QA] Starting Range-request concurrency verification with {concurrency} parallel connections on port {port}...")
    total_len = len(TEST_DATA)
    chunk_size = total_len // concurrency
    chunks = [None] * concurrency
    errors = []

    def fetch_worker(index, start, end):
        req = urllib.request.Request(f"http://127.0.0.1:{port}/payload.bin")
        req.add_header("Range", f"bytes={start}-{end}")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 206:
                    errors.append(f"Expected 206, got {resp.status}")
                chunks[index] = resp.read()
        except Exception as e:
            errors.append(str(e))

    threads = []
    start_time = time.time()
    for i in range(concurrency):
        start = i * chunk_size
        end = total_len - 1 if i == concurrency - 1 else (start + chunk_size - 1)
        t = threading.Thread(target=fetch_worker, args=(i, start, end))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    duration = time.time() - start_time
    if errors:
        print(f"[FAIL] Concurrency errors: {errors}")
        return False

    assembled = b"".join(chunks)
    expected_hash = hashlib.sha256(TEST_DATA).hexdigest()
    actual_hash = hashlib.sha256(assembled).hexdigest()

    print(f"[QA] Concurrency {concurrency} fetch duration: {duration:.3f}s")
    print(f"[QA] Expected SHA256: {expected_hash}")
    print(f"[QA] Actual SHA256:   {actual_hash}")

    if expected_hash == actual_hash:
        print("[PASS] Multi-chunk range aggregation verified successfully with 100% data integrity!")
        return True
    else:
        print("[FAIL] Checksum mismatch in assembled payload!")
        return False


if __name__ == "__main__":
    server = start_server()
    assigned_port = server.server_address[1]
    time.sleep(0.2)
    try:
        success_4 = run_qa_benchmark(assigned_port, concurrency=4)
        success_16 = run_qa_benchmark(assigned_port, concurrency=16)
        if not (success_4 and success_16):
            sys.exit(1)
    finally:
        server.shutdown()
        server.server_close()
