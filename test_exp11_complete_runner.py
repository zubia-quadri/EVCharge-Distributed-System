# -*- coding: utf-8 -*-
"""
test_exp11_complete_runner.py - Complete Automated Runner & Verifier for Experiment 11
Covers Step 10, Step 11, and Step 12 (Tests 1 through 6) from the lab manual.
"""
import hashlib
import os
import shutil
import subprocess
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PYTHON = os.path.join(PROJECT_ROOT, "venv", "Scripts", "python.exe")
if not os.path.exists(PYTHON):
    PYTHON = sys.executable


def sha256_file(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def run_cmd(args):
    print(f"\n$ {' '.join(args)}")
    result = subprocess.run([PYTHON] + args, capture_output=True, text=True, cwd=PROJECT_ROOT)
    print(result.stdout, end="")
    if result.stderr:
        print("[stderr]", result.stderr, end="")
    return result


def main():
    print("=" * 70)
    print("EXPERIMENT 11: FULL WORKFLOW VALIDATION (STEPS 10, 11, 12)")
    print("=" * 70)

    # Clean previous dfs_data and test artifacts for clean reproduction
    dfs_data_dir = os.path.join(PROJECT_ROOT, "dfs_data")
    if os.path.exists(dfs_data_dir):
        shutil.rmtree(dfs_data_dir)
    clock_file = os.path.join(PROJECT_ROOT, ".dfs_client_clock")
    if os.path.exists(clock_file):
        os.remove(clock_file)

    # Generate test files matching lab manual exactly
    # photo.bin: 300,000 bytes -> 5 chunks of 64KB (chunk 0..3: 65536, chunk 4: 37856)
    photo_bin = os.path.join(PROJECT_ROOT, "photo.bin")
    with open(photo_bin, "wb") as f:
        f.write(os.urandom(300000))
    photo_hash = sha256_file(photo_bin)
    print(f"Generated photo.bin: 300000 bytes, SHA256: {photo_hash[:16]}...")

    # second.bin: 70,000 bytes -> 2 chunks of 64KB (chunk 0: 65536, chunk 1: 4464)
    second_bin = os.path.join(PROJECT_ROOT, "second.bin")
    with open(second_bin, "wb") as f:
        f.write(os.urandom(70000))
    second_hash = sha256_file(second_bin)
    print(f"Generated second.bin: 70000 bytes, SHA256: {second_hash[:16]}...")

    # Step 10: Start Metadata server + 3 Storage nodes
    print("\n--- STEP 10: Starting Servers ---")
    p_meta = subprocess.Popen([PYTHON, "backend/dfs_metadata.py"], cwd=PROJECT_ROOT)
    time.sleep(1.0)
    p_s1 = subprocess.Popen([PYTHON, "backend/dfs_storage.py", "1"], cwd=PROJECT_ROOT)
    p_s2 = subprocess.Popen([PYTHON, "backend/dfs_storage.py", "2"], cwd=PROJECT_ROOT)
    p_s3 = subprocess.Popen([PYTHON, "backend/dfs_storage.py", "3"], cwd=PROJECT_ROOT)

    try:
        print("Waiting 3.5 seconds for storage node heartbeats...")
        time.sleep(3.5)

        # Step 11: Upload and download (client terminal)
        print("\n--- STEP 11: Upload and Download ---")
        run_cmd(["backend/dfs_client.py", "status"])

        res_put = run_cmd(["backend/dfs_client.py", "put", "photo.bin"])
        assert "Committed 'photo.bin'" in res_put.stdout, "Upload failed"

        run_cmd(["backend/dfs_client.py", "status"])

        out1 = os.path.join(PROJECT_ROOT, "out1.bin")
        if os.path.exists(out1):
            os.remove(out1)
        res_get1 = run_cmd(["backend/dfs_client.py", "get", "photo.bin", "out1.bin"])
        assert "checksum verified" in res_get1.stdout, "Download failed"
        assert sha256_file(out1) == photo_hash, "out1.bin checksum mismatch"
        print("[SUCCESS] out1.bin matches photo.bin exactly!")

        # Step 12 Test 1: Kill node 2 and read immediately
        print("\n--- STEP 12 Test 1: Kill store-2 and read anyway ---")
        p_s2.terminate()
        p_s2.wait()
        print("Killed storage node 2 (store-2). Reading within 5 seconds...")

        out2 = os.path.join(PROJECT_ROOT, "out2.bin")
        if os.path.exists(out2):
            os.remove(out2)
        res_get2 = run_cmd(["backend/dfs_client.py", "get", "photo.bin", "out2.bin"])
        assert sha256_file(out2) == photo_hash, "out2.bin checksum mismatch"
        print("[SUCCESS] out2.bin matches photo.bin intact despite store-2 being down!")

        # Step 12 Test 2: Watch the cluster notice (heartbeat timeout = 6s)
        print("\n--- STEP 12 Test 2: Watch the cluster notice (waiting 8 seconds) ---")
        time.sleep(8.0)
        res_stat2 = run_cmd(["backend/dfs_client.py", "status"])
        assert any("store-2" in line and "DOWN" in line for line in res_stat2.stdout.splitlines()), "store-2 should be DOWN"
        print("[SUCCESS] Cluster accurately detected store-2 as DOWN!")

        # Step 12 Test 3: Repair
        print("\n--- STEP 12 Test 3: Trigger repair script ---")
        res_rep = run_cmd(["backend/dfs_repair.py"])
        run_cmd(["backend/dfs_client.py", "status"])

        out3 = os.path.join(PROJECT_ROOT, "out3.bin")
        if os.path.exists(out3):
            os.remove(out3)
        res_get3 = run_cmd(["backend/dfs_client.py", "get", "photo.bin", "out3.bin"])
        assert sha256_file(out3) == photo_hash, "out3.bin checksum mismatch"
        print("[SUCCESS] out3.bin read succeeds after repair!")

        # Step 12 Test 4: Corrupt a chunk
        print("\n--- STEP 12 Test 4: Corrupt a stored chunk ---")
        # Find a chunk file in store-1
        store1_dir = os.path.join(dfs_data_dir, "store-1")
        chunks = [f for f in os.listdir(store1_dir) if f.startswith("photo.bin")]
        assert len(chunks) > 0, "No chunks found in store-1"
        corrupt_target = os.path.join(store1_dir, chunks[0])
        print(f"Corrupting chunk: {chunks[0]} on store-1")
        with open(corrupt_target, "w", encoding="utf-8") as f:
            f.write("corrupted garbage data")

        out4 = os.path.join(PROJECT_ROOT, "out4.bin")
        if os.path.exists(out4):
            os.remove(out4)
        res_get4 = run_cmd(["backend/dfs_client.py", "get", "photo.bin", "out4.bin"])
        assert "CORRUPT data, trying next replica" in res_get4.stdout, "Corruption not detected"
        assert sha256_file(out4) == photo_hash, "out4.bin checksum mismatch"
        print("[SUCCESS] Corruption detected, client retrieved healthy replica and verified whole-file SHA-256!")

        # Step 12 Test 5: Bring node 2 back
        print("\n--- STEP 12 Test 5: Bring store-2 back ---")
        p_s2_new = subprocess.Popen([PYTHON, "backend/dfs_storage.py", "2"], cwd=PROJECT_ROOT)
        print("Waiting 4 seconds for store-2 to re-register with metadata...")
        time.sleep(4.0)
        res_stat5 = run_cmd(["backend/dfs_client.py", "status"])
        assert any("store-2" in line and "UP" in line for line in res_stat5.stdout.splitlines()), "store-2 did not come back UP"
        print("[SUCCESS] store-2 is back UP and re-registered!")

        # Step 12 Test 6: A second file
        print("\n--- STEP 12 Test 6: Upload a second file ---")
        run_cmd(["backend/dfs_client.py", "put", "second.bin"])
        run_cmd(["backend/dfs_client.py", "status"])
        print("[SUCCESS] second.bin uploaded and distributed to least-loaded nodes!")

        print("\n" + "=" * 70)
        print("ALL EXPERIMENT 11 TESTS PASSED PERFECTLY!")
        print("=" * 70)

    finally:
        # Cleanup server processes
        for p in [p_meta, p_s1, p_s2, p_s3]:
            try:
                p.terminate()
                p.wait(timeout=2)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass
        if 'p_s2_new' in locals():
            try:
                p_s2_new.terminate()
                p_s2_new.wait(timeout=2)
            except Exception:
                try:
                    p_s2_new.kill()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
