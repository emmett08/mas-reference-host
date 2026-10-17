#!/usr/bin/env python3
"""Audit every committed Python pin, including build/bootstrap packages."""
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
failed = False
for group, lock in [("runtime", "openai"), ("cursor", "cursor"), ("bridges", "bridges"), ("orchestrators", "orchestrators"), ("object-storage", "object-storage"), ("build", "build"), ("security", "security")]:
    path = ROOT / "host" / f"requirements-{lock}.txt"
    report = Path(f"python-{group}-audit.json")
    expected = set(re.findall(r"^([a-z0-9-]+)==([^\s]+)", path.read_text(), re.MULTILINE))
    # pip's temporary environment can omit bootstrap packages from its install
    # report. Parsing the complete hashed lock directly prevents an empty build
    # audit from being mistaken for evidence that all build tools were checked.
    process = subprocess.run([
        sys.executable, "-m", "pip_audit", "--disable-pip", "--require-hashes", "--strict",
        "-r", str(path), "--format", "json", "--output", str(report),
    ])
    if process.returncode != 0:
        failed = True
    if not report.exists():
        failed = True
        continue
    audited = json.loads(report.read_text()).get("dependencies", [])
    actual = {(re.sub(r"[-_.]+", "-", item["name"]).lower(), item.get("version")) for item in audited}
    if actual != expected or any(item.get("skip_reason") or item.get("vulns") for item in audited):
        print(f"{group}: incomplete or failed audit; expected {len(expected)}, received {len(actual)} packages", file=sys.stderr)
        failed = True
    else:
        print(f"{group}: audited all {len(expected)} locked packages")
raise SystemExit(1 if failed else 0)
