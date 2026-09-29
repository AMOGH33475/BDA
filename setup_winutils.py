"""
setup_winutils.py
-----------------
One-time setup script for Windows to install Hadoop winutils.exe
Required by PySpark on Windows to write Parquet files.

Run once with: python setup_winutils.py
"""

import os
import sys
import urllib.request
import shutil
import subprocess
import ctypes

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
HADOOP_HOME = r"C:\hadoop"
HADOOP_BIN  = os.path.join(HADOOP_HOME, "bin")

# winutils.exe + hadoop.dll for Hadoop 3.3.5 (works with Spark 3.x and 4.x)
FILES = {
    "winutils.exe": (
        "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe"
    ),
    "hadoop.dll": (
        "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll"
    ),
}

def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except Exception:
        return False

def download(url, dest):
    print(f"  Downloading {os.path.basename(dest)} ...")
    headers = {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as r, open(dest, "wb") as f:
        shutil.copyfileobj(r, f)
    size_kb = os.path.getsize(dest) / 1024
    print(f"  Saved {os.path.basename(dest)} ({size_kb:.1f} KB)")

def set_env_permanent(name, value):
    """Set a User-level environment variable permanently via PowerShell."""
    cmd = f'[System.Environment]::SetEnvironmentVariable("{name}", "{value}", "User")'
    subprocess.run(["powershell", "-Command", cmd], check=True)
    # Also set for current process
    os.environ[name] = value
    print(f"  Set {name} = {value}  (User environment variable)")

def main():
    print("=" * 60)
    print("  PySpark Windows Winutils Setup")
    print("=" * 60)

    # 1. Create hadoop/bin directory
    os.makedirs(HADOOP_BIN, exist_ok=True)
    print(f"\n[1/3] Target directory: {HADOOP_BIN}")

    # 2. Download winutils.exe and hadoop.dll
    print("\n[2/3] Downloading Hadoop winutils binaries...")
    for filename, url in FILES.items():
        dest = os.path.join(HADOOP_BIN, filename)
        if os.path.exists(dest):
            size_kb = os.path.getsize(dest) / 1024
            print(f"  {filename} already exists ({size_kb:.1f} KB) — skipping download.")
        else:
            download(url, dest)

    # 3. Set HADOOP_HOME environment variable permanently
    print("\n[3/3] Setting environment variables...")
    set_env_permanent("HADOOP_HOME", HADOOP_HOME)

    # Also add hadoop/bin to PATH if not already there
    current_path = os.environ.get("PATH", "")
    if HADOOP_BIN.lower() not in current_path.lower():
        new_path = HADOOP_HOME + "\\bin;" + current_path
        set_env_permanent("PATH", new_path)
        print(f"  Added {HADOOP_BIN} to PATH.")

    # Verify
    print("\n" + "=" * 60)
    print("  SETUP COMPLETE!")
    print("=" * 60)
    for filename in FILES:
        dest = os.path.join(HADOOP_BIN, filename)
        exists = "✓ EXISTS" if os.path.exists(dest) else "✗ MISSING"
        print(f"  {exists}: {dest}")

    print(f"\n  HADOOP_HOME = {os.environ.get('HADOOP_HOME', 'NOT SET')}")
    print("""
NEXT STEPS:
  1. Close this terminal and open a NEW PowerShell window, OR
     run this in your current terminal to apply immediately:
     
       $env:HADOOP_HOME = "C:\\hadoop"
       $env:PATH = "C:\\hadoop\\bin;" + $env:PATH

  2. Then re-run the pipeline:
       python src\\02_cleaning.py
""")

if __name__ == "__main__":
    main()
