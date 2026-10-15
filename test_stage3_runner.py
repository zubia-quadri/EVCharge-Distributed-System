# -*- coding: utf-8 -*-
import subprocess
import time
import sys
import os
import hashlib

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PYTHON = os.path.join(PROJECT_ROOT, "venv", "Scripts", "python.exe")
if not os.path.exists(PYTHON):
    PYTHON = sys.executable

def sha256_file(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def run_cmd(args):
    print(f"\n>>> Running: {' '.join(args)}")
    result = subprocess.run([PYTHON] + args, capture_output=True, text=True, cwd=PROJECT_ROOT)
    print(result.stdout, end="")
    if result.stderr:
        print("[stderr]", result.stderr, end="")
    return result

def main():
    print("=" * 65)
    print("Experiment 11 Stage 3: Live Distributed File System Test")
    print("=" * 65)

    # 1. Start Servers
    print("[1/6] Starting Metadata Server and 3 Storage Nodes...")
    p_meta = subprocess.Popen([PYTHON, "backend/dfs_metadata.py"], cwd=PROJECT_ROOT)
    time.sleep(1.0)
    p_s1 = subprocess.Popen([PYTHON, "backend/dfs_storage.py", "1"], cwd=PROJECT_ROOT)
    p_s2 = subprocess.Popen([PYTHON, "backend/dfs_storage.py", "2"], cwd=PROJECT_ROOT)
    p_s3 = subprocess.Popen([PYTHON, "backend/dfs_storage.py", "3"], cwd=PROJECT_ROOT)

    try:
        # Allow time for initial heartbeats
        print("Waiting 3 seconds for storage nodes to register via heartbeats...")
        time.sleep(3.0)

        # 2. Status check
        print("\n[2/6] Querying initial cluster status:")
        res_status_init = run_cmd(["backend/dfs_client.py", "status"])
        assert res_status_init.returncode == 0, "Initial status query failed"

        # 3. Upload test file (~147 KB spanning 3 chunks)
        print("\n[3/6] Uploading 'test_sample.txt' (147 KB):")
        src_hash = sha256_file("test_sample.txt")
        print(f"Source file SHA-256: {src_hash}")
        res_put = run_cmd(["backend/dfs_client.py", "put", "test_sample.txt"])
        assert res_put.returncode == 0, "Upload failed"

        # 4. Status check after upload
        print("\n[4/6] Querying cluster status after upload:")
        res_status_after = run_cmd(["backend/dfs_client.py", "status"])
        assert res_status_after.returncode == 0, "Post-upload status query failed"

        # 5. Download test file
        print("\n[5/6] Downloading 'test_sample.txt' to 'downloaded_sample.txt':")
        out_file = "downloaded_sample.txt"
        if os.path.exists(out_file):
            os.remove(out_file)
        res_get = run_cmd(["backend/dfs_client.py", "get", "test_sample.txt", out_file])
        assert res_get.returncode == 0, "Download failed"
        assert os.path.exists(out_file), "Downloaded file does not exist"

        dst_hash = sha256_file(out_file)
        print(f"Downloaded file SHA-256: {dst_hash}")

        # Byte-for-byte comparison
        with open("test_sample.txt", "rb") as f1, open(out_file, "rb") as f2:
            data1 = f1.read()
            data2 = f2.read()
        
        assert len(data1) == len(data2), f"Size mismatch: {len(data1)} vs {len(data2)}"
        assert data1 == data2, "Byte-for-byte mismatch!"
        assert src_hash == dst_hash, f"SHA-256 hash mismatch: {src_hash} vs {dst_hash}"
        print(f"\n[VERIFIED] 'test_sample.txt' matches 'downloaded_sample.txt' byte-for-byte ({len(data1)} bytes)!")

        # 6. Test Empty File Handling
        print("\n[6/6] Testing empty file upload and download:")
        empty_src = "empty_test.txt"
        empty_dst = "downloaded_empty.txt"
        with open(empty_src, "wb") as f:
            pass
        empty_src_hash = sha256_file(empty_src)
        
        run_cmd(["backend/dfs_client.py", "put", empty_src])
        run_cmd(["backend/dfs_client.py", "get", empty_src, empty_dst])
        empty_dst_hash = sha256_file(empty_dst)
        assert empty_src_hash == empty_dst_hash, "Empty file hash mismatch!"
        assert os.path.getsize(empty_dst) == 0, "Empty file size is non-zero!"
        print(f"[VERIFIED] Empty file handled safely! Size = 0 bytes, SHA-256 = {empty_dst_hash[:16]}...")

        print("\n" + "=" * 65)
        print("ALL TESTS PASSED SUCCESSFULLY!")
        print("=" * 65)

    finally:
        print("\nStopping background servers...")
        for p in [p_s1, p_s2, p_s3, p_meta]:
            try:
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(p.pid)], capture_output=True)
            except Exception:
                pass
        print("Servers stopped.")

if __name__ == "__main__":
    main()
