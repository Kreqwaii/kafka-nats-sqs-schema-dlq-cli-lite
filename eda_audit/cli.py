"""CLI entrypoint for kafka-nats-sqs-schema-dlq-cli-lite."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

from eda_audit import __version__
from eda_audit.auditor import SEVERITY_ORDER, Finding, audit_kafka, audit_nats, audit_sqs
from eda_audit.reporter import format_json, format_markdown
from eda_audit.schema_diff import diff_schemas


def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafka-nats-sqs-schema-dlq-cli-lite",
        description="Offline event-driven architecture auditor for Kafka, SQS, NATS JetStream, and Avro/JSON Schemas.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--kafka", nargs="*", default=[], help="Kafka topic/consumer JSON config files")
    parser.add_argument("--sqs", nargs="*", default=[], help="AWS SQS queue JSON definition files")
    parser.add_argument("--nats", nargs="*", default=[], help="NATS JetStream stream JSON config files")
    parser.add_argument(
        "--schema-diff",
        nargs=2,
        metavar=("OLD_SCHEMA", "NEW_SCHEMA"),
        help="Compare two Avro or JSON Schema files for breaking changes",
    )
    parser.add_argument("--format", choices=["json", "markdown"], default="markdown", help="Output format")
    parser.add_argument(
        "--fail-on",
        choices=["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO", "NONE"],
        default="HIGH",
        help="Exit with code 1 if any finding meets or exceeds this severity (default: HIGH)",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not (args.kafka or args.sqs or args.nats or args.schema_diff):
        parser.print_help()
        return 2

    findings: List[Finding] = []

    try:
        for path in args.kafka:
            findings.extend(audit_kafka(_load_json(path), source=Path(path).name))
        for path in args.sqs:
            findings.extend(audit_sqs(_load_json(path), source=Path(path).name))
        for path in args.nats:
            findings.extend(audit_nats(_load_json(path), source=Path(path).name))
        if args.schema_diff:
            old_path, new_path = args.schema_diff
            findings.extend(
                diff_schemas(_load_json(old_path), _load_json(new_path), resource=Path(new_path).stem)
            )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    output = format_json(findings) if args.format == "json" else format_markdown(findings)
    print(output)

    if args.fail_on != "NONE":
        threshold = SEVERITY_ORDER[args.fail_on]
        if any(SEVERITY_ORDER.get(f.severity, 0) >= threshold for f in findings):
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
