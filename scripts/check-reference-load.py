#!/usr/bin/env python3
"""Run the fixed no-paid reference profile; --smoke is not qualification."""
import argparse
import json
from pathlib import Path
import sys

from mas_host.reference_load import run_reference


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', choices=('leaf', 'mixed'), default='leaf',
                        help='fixed leaf exposure or mixed/history contention profile')
    parser.add_argument('--smoke', action='store_true', help='short development probe, never qualification')
    parser.add_argument('--expected-commit', help='require this exact full source revision')
    parser.add_argument('--report', type=Path, required=True, help='JSON result destination outside the source checkout')
    args = parser.parse_args()
    repository = Path(__file__).resolve().parents[1]
    if args.report.resolve().is_relative_to(repository):
        parser.error('--report must be outside the measured source checkout')
    run = run_reference
    if args.profile == 'mixed':
        from mas_host.reference_mixed import run_mixed_reference
        run = run_mixed_reference
    report = run(repository, smoke=args.smoke, expected_commit=args.expected_commit)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    print(json.dumps({key: report[key] for key in ('qualification', 'passed', 'submitted', 'completed_by_principal', 'failures', 'checks')}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
