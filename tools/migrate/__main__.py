#!/usr/bin/env python
"""
Flowfile Migration Tool - CLI Entry Point

Converts old pickle-based .flowfile format to new YAML format.

Usage:
    python -m tools.migrate <path> [options]

Examples:
    # Migrate a single file
    python -m tools.migrate my_flow.flowfile

    # Migrate to specific output
    python -m tools.migrate my_flow.flowfile -o my_flow.yaml

    # Migrate entire directory
    python -m tools.migrate ./flows/

    # Migrate to JSON instead of YAML
    python -m tools.migrate my_flow.flowfile --format json

    # Migrate directory to different output location
    python -m tools.migrate ./old_flows/ -o ./new_flows/
"""

import argparse
import sys
from pathlib import Path

from tools.migrate.migrate import migrate_flowfile, migrate_directory


def main():
    parser = argparse.ArgumentParser(
        prog='flowfile-migrate',
        description='Migrate old .flowfile pickles to YAML format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s my_flow.flowfile              Migrate single file to YAML
  %(prog)s ./flows/                      Migrate all files in directory
  %(prog)s flow.flowfile -o flow.yaml    Specify output path
  %(prog)s ./flows/ --format json        Output as JSON instead of YAML
        """
    )

    parser.add_argument(
        'path',
        type=Path,
        help='Path to .flowfile or directory containing .flowfile files'
    )

    parser.add_argument(
        '-o', '--output',
        type=Path,
        default=None,
        help='Output path (file or directory). Default: same location with new extension'
    )

    parser.add_argument(
        '-f', '--format',
        choices=['yaml', 'json'],
        default='yaml',
        help='Output format (default: yaml)'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be migrated without actually migrating'
    )

    args = parser.parse_args()

    # Validate input path
    if not args.path.exists():
        print(f"Error: Path not found: {args.path}", file=sys.stderr)
        sys.exit(1)

    # Dry run mode
    if args.dry_run:
        if args.path.is_file():
            print(f"Would migrate: {args.path}")
            suffix = '.yaml' if args.format == 'yaml' else '.json'
            output = args.output or args.path.with_suffix(suffix)
            print(f"  → {output}")
        else:
            flowfiles = list(args.path.glob('**/*.flowfile'))
            print(f"Would migrate {len(flowfiles)} file(s):")
            for f in flowfiles:
                print(f"  - {f}")
        sys.exit(0)

    # Check for yaml dependency
    if args.format == 'yaml':
        try:
            import yaml
        except ImportError:
            print("Error: PyYAML is required for YAML output.", file=sys.stderr)
            print("Install with: pip install pyyaml", file=sys.stderr)
            sys.exit(1)

    # Run migration
    try:
        if args.path.is_file():
            migrate_flowfile(args.path, args.output, args.format)
        elif args.path.is_dir():
            migrate_directory(args.path, args.output, args.format)
        else:
            print(f"Error: {args.path} is neither a file nor a directory", file=sys.stderr)
            sys.exit(1)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    print("\nMigration complete!")


if __name__ == '__main__':
    main()
