#!/usr/bin/env python3
"""Bind a trusted build's package and Rust binaries without running either."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import tomllib


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', type=Path, required=True)
    parser.add_argument('--rust-manifest', type=Path, required=True)
    parser.add_argument('--binaries', type=Path, required=True)
    parser.add_argument('--source-commit', default='')
    parser.add_argument('--release-tag', default='')
    args = parser.parse_args()
    if args.source_commit and re.fullmatch('[0-9a-f]{40}', args.source_commit) is None:
        parser.error('source commit must be a full SHA')
    if args.release_tag and (not args.source_commit or re.fullmatch(r'v0\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)', args.release_tag) is None):
        parser.error('release tag requires a source commit and v0.MINOR.PATCH')
    package = args.host / 'mas_host'
    # This explicitly selected build helper is trusted repository code. No
    # application package/factory or built executable is imported or invoked.
    spec = importlib.util.spec_from_file_location('runtime_provenance', package / 'runtime_provenance.py')
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    host = tomllib.loads((args.host / 'pyproject.toml').read_text())['project']['version']
    rust = tomllib.loads(args.rust_manifest.read_text())['package']['version']
    binaries = {name: {'version': rust, 'sha256': hashlib.sha256(helper._read(
        args.binaries / name, 256 * 1024 * 1024)).hexdigest()} for name in ('mas', 'mas-gateway')}
    record = {'format': 'mas-runtime-build/1', 'source_commit': args.source_commit or None,
        'release_tag': args.release_tag or None, 'host_version': host,
        'package_sha256': helper.package_digest(package), 'binaries': binaries}
    (package / helper.BUILD_RECORD).write_text(json.dumps(record, sort_keys=True, indent=2) + '\n')


if __name__ == '__main__':
    main()
