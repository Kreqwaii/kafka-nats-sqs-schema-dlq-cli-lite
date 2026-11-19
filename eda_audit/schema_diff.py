"""Avro and JSON Schema backward-compatibility checker."""

from __future__ import annotations

from typing import Any, Dict, List, Set

from eda_audit.auditor import Finding


def _is_avro(schema: Dict[str, Any]) -> bool:
    return schema.get("type") == "record" and isinstance(schema.get("fields"), list)


def diff_schemas(
    old_schema: Dict[str, Any],
    new_schema: Dict[str, Any],
    resource: str = "schema",
) -> List[Finding]:
    if _is_avro(old_schema) and _is_avro(new_schema):
        return _diff_avro(old_schema, new_schema, resource)
    return _diff_json_schema(old_schema, new_schema, resource)


def _diff_avro(old: Dict[str, Any], new: Dict[str, Any], resource: str) -> List[Finding]:
    findings: List[Finding] = []
    old_fields = {f["name"]: f for f in old.get("fields", []) if isinstance(f, dict) and "name" in f}
    new_fields = {f["name"]: f for f in new.get("fields", []) if isinstance(f, dict) and "name" in f}

    for fname, old_f in old_fields.items():
        if fname not in new_fields:
            is_required = "default" not in old_f
            findings.append(Finding(
                rule_id="SCH-001",
                severity="CRITICAL" if is_required else "HIGH",
                platform="schema",
                resource=f"{resource}.{fname}",
                message=f"Avro field '{fname}' was removed in new schema.",
                remediation="Keep the field or deprecate it with a default value.",
            ))
            continue
        old_t = old_f.get("type")
        new_t = new_fields[fname].get("type")
        if old_t != new_t:
            findings.append(Finding(
                rule_id="SCH-002",
                severity="CRITICAL",
                platform="schema",
                resource=f"{resource}.{fname}",
                message=f"Avro field '{fname}' type changed from {old_t!r} to {new_t!r}.",
                remediation="Avoid breaking type changes; introduce a new versioned field instead.",
            ))

    for fname, new_f in new_fields.items():
        if fname not in old_fields and "default" not in new_f:
            findings.append(Finding(
                rule_id="SCH-003",
                severity="HIGH",
                platform="schema",
                resource=f"{resource}.{fname}",
                message=f"New required Avro field '{fname}' added without a default value.",
                remediation="Provide a default value for newly added Avro fields.",
            ))

    return findings


def _diff_json_schema(old: Dict[str, Any], new: Dict[str, Any], resource: str) -> List[Finding]:
    findings: List[Finding] = []
    old_props: Dict[str, Any] = old.get("properties", {}) if isinstance(old.get("properties"), dict) else {}
    new_props: Dict[str, Any] = new.get("properties", {}) if isinstance(new.get("properties"), dict) else {}
    old_req: Set[str] = set(old.get("required", [])) if isinstance(old.get("required"), list) else set()
    new_req: Set[str] = set(new.get("required", [])) if isinstance(new.get("required"), list) else set()

    for prop in old_req:
        if prop not in new_props:
            findings.append(Finding(
                rule_id="SCH-001",
                severity="CRITICAL",
                platform="schema",
                resource=f"{resource}.{prop}",
                message=f"Required JSON Schema property '{prop}' was removed.",
                remediation="Restore the required property or bump the major schema version.",
            ))

    for prop, old_def in old_props.items():
        if prop in new_props and isinstance(old_def, dict) and isinstance(new_props[prop], dict):
            old_type = old_def.get("type")
            new_type = new_props[prop].get("type")
            if old_type is not None and new_type is not None and old_type != new_type:
                findings.append(Finding(
                    rule_id="SCH-002",
                    severity="CRITICAL",
                    platform="schema",
                    resource=f"{resource}.{prop}",
                    message=f"JSON Schema property '{prop}' type changed from {old_type!r} to {new_type!r}.",
                    remediation="Preserve field type or introduce a new property name.",
                ))

    for prop in new_req - old_req:
        findings.append(Finding(
            rule_id="SCH-003",
            severity="HIGH",
            platform="schema",
            resource=f"{resource}.{prop}",
            message=f"Property '{prop}' became required in new JSON Schema.",
            remediation="Keep new properties optional for backward compatibility.",
        ))

    return findings
