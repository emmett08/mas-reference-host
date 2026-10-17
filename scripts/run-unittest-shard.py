#!/usr/bin/env python3
"""Run an exact, deterministic partition of ordinary unittest discovery.

Discovery (including load_tests hooks) happens before selection. Selected test
objects run through TestSuite so normal module/class fixtures, skips, expected
failures and cleanup still apply within each isolated shard.
"""
from __future__ import annotations

import argparse
import json
import unittest


def flatten(suite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from flatten(test)
        else:
            yield test


def discover(start_directory, pattern):
    loader = unittest.TestLoader()
    suite = loader.discover(start_directory, pattern=pattern)
    if loader.errors:
        raise ValueError("test discovery failed:\n" + "\n".join(loader.errors))
    tests = sorted(flatten(suite), key=lambda test: test.id())
    identifiers = [test.id() for test in tests]
    if len(set(identifiers)) != len(identifiers):
        raise ValueError("test discovery returned duplicate test IDs")
    return tests


def select(tests, shard_index, shard_count):
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise ValueError("require shard-count > 0 and 0 <= shard-index < shard-count")
    if len(tests) < shard_count:
        raise ValueError("test discovery cannot populate every shard; empty shards are rejected")
    return tests[shard_index::shard_count]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-s", "--start-directory", default=".")
    parser.add_argument("-p", "--pattern", default="test*.py")
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--list", action="store_true", help="print the selected test manifest without executing tests")
    args = parser.parse_args(argv)
    try:
        tests = discover(args.start_directory, args.pattern)
        selected = select(tests, args.shard_index, args.shard_count)
    except (ImportError, ValueError) as error:
        parser.error(str(error))
    print(json.dumps({"shard_index": args.shard_index, "shard_count": args.shard_count,
        "discovered": len(tests), "selected": [test.id() for test in selected]}), flush=True)
    if args.list:
        return 0
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(selected))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
