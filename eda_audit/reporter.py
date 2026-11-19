"""Output formatters (JSON and Markdown) for audit findings."""

from __future__ import annotations

import json
from typing import Dict, List

from eda_audit.auditor import Finding


def format_json(findings: List[Finding]) -> str:
    summary: Dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "INFO": 0}
    for f in findings:
        summary[f.severity] = summary.get(f.severity, 0) + 1
    payload = {
        "total_findings": len(findings),
        "summary": summary,
        "findings": [f.to_dict() for f in findings],
    }
    return json.dumps(payload, indent=2)


def format_markdown(findings: List[Finding]) -> str:
    if not findings:
        return "# Event-Driven Architecture Audit\n\nNo findings detected. All checks passed.\n"

    lines = [
        "# Event-Driven Architecture Audit",
        "",
        f"Total findings: **{len(findings)}**",
        "",
        "| Rule | Severity | Platform | Resource | Message |",
        "| --- | --- | --- | --- | --- |",
    ]
    for f in findings:
        msg = f.message.replace("|", "\\|")
        res = f.resource.replace("|", "\\|")
        lines.append(f"| `{f.rule_id}` | **{f.severity}** | `{f.platform}` | `{res}` | {msg} |")
    lines.append("")
    return "\n".join(lines)
