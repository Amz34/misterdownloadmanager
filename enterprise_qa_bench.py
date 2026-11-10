"""
Enterprise QA Test Suite for Chunked Range Downloading and Concurrency Emulation.
Simulates an HTTP/1.1 Range-compliant server and validates partial chunk aggregation.
"""

import http.server
import socketserver
import threading
import urllib.request
import hashlib
import time
import os

TEST_DATA = b"MISTER_DOWNLOAD_MANAGER_ENTERPRISE_QA_VERIFICATION_PAYLOAD_" * 1024 * 64 # ~3.7 MB
PORT = 8998

class RangeRequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
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

        # Parse range header: bytes=START-END
        try:
            byte_range = range_header.strip().split("=")[1]
            start_str, end_str = byte_range.split("-")
            start = int(start_str)
            end = int(end_str) if end_str else total_len - 1
            if end >= total_len:
                end = total_len - 1

            chunk_len = (end - start) + 1
            self.send_response(206)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Range", f"bytes {start}-{end}/{total_len}")
            self.send_header("Content-Length", str(chunk_len))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(TEST_DATA[start:end + 1])
        except Exception as e:
            self.send_response(400)
            self.end_headers()

    def log_message(self, format, *args):
        pass # Silence test server logs

def start_server():
    server = socketserver.TCPServer(("127.0.0.1", PORT), RangeRequestHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server

def run_qa_benchmark(concurrency=8):
    print(f"[QA] Starting Range-request concurrency verification with {concurrency} parallel connections...")
    total_len = len(TEST_DATA)
    chunk_size = total_len // concurrency
    chunks = [None] * concurrency
    errors = []

    def fetch_worker(index, start, end):
        req = urllib.request.Request(f"http://127.0.0.1:{PORT}/payload.bin")
        req.add_header("Range", f"bytes={start}-{end}")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
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
    time.sleep(0.5)
    success_4 = run_qa_benchmark(concurrency=4)
    success_16 = run_qa_benchmark(concurrency=16)
    server.shutdown()
    server.server_close()
    if not (success_4 and success_16):
        exit(1)
