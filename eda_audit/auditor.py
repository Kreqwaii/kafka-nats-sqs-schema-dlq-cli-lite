"""Core auditors for Kafka, AWS SQS, and NATS JetStream JSON configs."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List


SEVERITY_ORDER = {"INFO": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}


@dataclass
class Finding:
    rule_id: str
    severity: str
    platform: str
    resource: str
    message: str
    remediation: str

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


def _is_prod(cfg: Dict[str, Any]) -> bool:
    env = str(cfg.get("environment", cfg.get("env", ""))).lower()
    return env in {"prod", "production", "prd", "live"}


def audit_kafka(data: Dict[str, Any], source: str = "kafka.json") -> List[Finding]:
    findings: List[Finding] = []
    topics = data.get("topics")
    if isinstance(topics, list):
        for idx, topic in enumerate(topics):
            if isinstance(topic, dict):
                findings.extend(_audit_kafka_topic(topic, source, idx))
    elif "topic" in data or "replication_factor" in data or "configs" in data:
        findings.extend(_audit_kafka_topic(data, source, 0))

    consumers = data.get("consumers")
    if isinstance(consumers, list):
        for idx, consumer in enumerate(consumers):
            if isinstance(consumer, dict):
                findings.extend(_audit_kafka_consumer(consumer, source, idx))
    elif "auto.offset.reset" in data or "auto_offset_reset" in data or "group_id" in data:
        findings.extend(_audit_kafka_consumer(data, source, 0))

    return findings


def _audit_kafka_topic(topic: Dict[str, Any], source: str, idx: int) -> List[Finding]:
    findings: List[Finding] = []
    name = str(topic.get("name", topic.get("topic", f"{source}#topic[{idx}]")))
    configs = topic.get("configs", {}) if isinstance(topic.get("configs"), dict) else {}
    prod = _is_prod(topic)

    rf = topic.get("replication_factor", topic.get("replicationFactor"))
    if rf is not None:
        try:
            rf_val = int(rf)
            if prod and rf_val < 3:
                findings.append(Finding(
                    rule_id="KFK-001",
                    severity="CRITICAL",
                    platform="kafka",
                    resource=name,
                    message=f"Production topic has replication_factor={rf_val} (< 3), risking data loss on broker failure.",
                    remediation="Set replication_factor >= 3 for production topics.",
                ))
        except (TypeError, ValueError):
            pass

    min_isr = configs.get("min.insync.replicas", topic.get("min_insync_replicas"))
    if min_isr is not None:
        try:
            isr_val = int(min_isr)
            if isr_val < 2:
                findings.append(Finding(
                    rule_id="KFK-002",
                    severity="HIGH",
                    platform="kafka",
                    resource=name,
                    message=f"min.insync.replicas={isr_val} (< 2) weakens durability with acks=all.",
                    remediation="Set min.insync.replicas >= 2.",
                ))
        except (TypeError, ValueError):
            pass

    retention_ms = configs.get("retention.ms", topic.get("retention_ms"))
    cleanup_policy = str(configs.get("cleanup.policy", topic.get("cleanup_policy", "delete"))).lower()
    if retention_ms is not None:
        try:
            if int(retention_ms) == -1 and "compact" not in cleanup_policy:
                findings.append(Finding(
                    rule_id="KFK-003",
                    severity="HIGH",
                    platform="kafka",
                    resource=name,
                    message=f"Infinite retention (retention.ms=-1) without log compaction (cleanup.policy={cleanup_policy}).",
                    remediation="Set a bounded retention.ms or enable cleanup.policy=compact.",
                ))
        except (TypeError, ValueError):
            pass

    dlq = topic.get("dlq_topic", topic.get("dead_letter_topic", configs.get("dlq.topic")))
    if not dlq:
        findings.append(Finding(
            rule_id="KFK-004",
            severity="MEDIUM",
            platform="kafka",
            resource=name,
            message="No Dead-Letter Queue (dlq_topic / dead_letter_topic) configured.",
            remediation="Configure a dedicated DLQ topic for poison-pill messages.",
        ))

    return findings


def _audit_kafka_consumer(consumer: Dict[str, Any], source: str, idx: int) -> List[Finding]:
    findings: List[Finding] = []
    name = str(consumer.get("group_id", consumer.get("name", f"{source}#consumer[{idx}]")))
    offset_reset = str(
        consumer.get("auto.offset.reset", consumer.get("auto_offset_reset", ""))
    ).lower()
    if offset_reset == "latest":
        findings.append(Finding(
            rule_id="KFK-005",
            severity="HIGH",
            platform="kafka",
            resource=name,
            message="Consumer uses auto.offset.reset=latest, risking silent data loss for new consumer groups.",
            remediation="Use auto.offset.reset=earliest unless skipping historical events is intentional.",
        ))
    return findings


def audit_sqs(data: Dict[str, Any], source: str = "sqs.json") -> List[Finding]:
    findings: List[Finding] = []
    queues = data.get("queues")
    if isinstance(queues, list):
        for idx, q in enumerate(queues):
            if isinstance(q, dict):
                findings.extend(_audit_sqs_queue(q, source, idx))
    else:
        findings.extend(_audit_sqs_queue(data, source, 0))
    return findings


def _audit_sqs_queue(queue: Dict[str, Any], source: str, idx: int) -> List[Finding]:
    findings: List[Finding] = []
    name = str(queue.get("QueueName", queue.get("name", f"{source}#queue[{idx}]")))
    attrs = queue.get("Attributes", queue) if isinstance(queue.get("Attributes"), dict) else queue

    redrive = attrs.get("RedrivePolicy", attrs.get("redrivePolicy"))
    if not redrive:
        findings.append(Finding(
            rule_id="SQS-001",
            severity="HIGH",
            platform="sqs",
            resource=name,
            message="Missing RedrivePolicy (Dead-Letter Queue) on SQS queue.",
            remediation="Configure RedrivePolicy with deadLetterTargetArn and maxReceiveCount.",
        ))

    vis_timeout = attrs.get("VisibilityTimeout", attrs.get("visibilityTimeout"))
    handler_timeout = queue.get("handler_timeout_seconds", queue.get("lambda_timeout_seconds"))
    if vis_timeout is not None and handler_timeout is not None:
        try:
            vis_val = int(vis_timeout)
            hdl_val = int(handler_timeout)
            if vis_val < hdl_val:
                findings.append(Finding(
                    rule_id="SQS-002",
                    severity="CRITICAL",
                    platform="sqs",
                    resource=name,
                    message=f"VisibilityTimeout ({vis_val}s) is shorter than handler timeout ({hdl_val}s), causing duplicate processing.",
                    remediation="Set VisibilityTimeout to at least 6x handler timeout (or > handler timeout).",
                ))
        except (TypeError, ValueError):
            pass

    return findings


def audit_nats(data: Dict[str, Any], source: str = "nats.json") -> List[Finding]:
    findings: List[Finding] = []
    streams = data.get("streams")
    if isinstance(streams, list):
        for idx, s in enumerate(streams):
            if isinstance(s, dict):
                findings.extend(_audit_nats_stream(s, source, idx))
    else:
        findings.extend(_audit_nats_stream(data, source, 0))
    return findings


def _audit_nats_stream(stream: Dict[str, Any], source: str, idx: int) -> List[Finding]:
    findings: List[Finding] = []
    cfg = stream.get("config", stream) if isinstance(stream.get("config"), dict) else stream
    name = str(cfg.get("name", stream.get("name", f"{source}#stream[{idx}]")))

    max_bytes = cfg.get("max_bytes", -1)
    max_age = cfg.get("max_age", 0)
    try:
        unbounded_bytes = int(max_bytes) <= 0
    except (TypeError, ValueError):
        unbounded_bytes = True
    try:
        unbounded_age = int(max_age) <= 0
    except (TypeError, ValueError):
        unbounded_age = True

    if unbounded_bytes and unbounded_age:
        findings.append(Finding(
            rule_id="NTS-001",
            severity="HIGH",
            platform="nats",
            resource=name,
            message="NATS JetStream stream has no max_bytes or max_age cap (unbounded storage growth).",
            remediation="Set max_bytes > 0 and/or max_age > 0 to bound stream storage.",
        ))

    dlq = cfg.get("dlq_subject", stream.get("dlq_subject"))
    if not dlq:
        findings.append(Finding(
            rule_id="NTS-002",
            severity="MEDIUM",
            platform="nats",
            resource=name,
            message="NATS JetStream stream/consumer has no dlq_subject configured for failed advisories.",
            remediation="Configure a dead-letter subject or advisory handler for max-deliveries exceeded.",
        ))

    return findings
