import unittest

from eda_audit.auditor import audit_kafka, audit_nats, audit_sqs


class TestKafkaAuditor(unittest.TestCase):
    def test_prod_low_replication_factor(self):
        data = {"name": "orders", "environment": "prod", "replication_factor": 2, "dlq_topic": "orders.dlq"}
        rules = {f.rule_id for f in audit_kafka(data)}
        self.assertIn("KFK-001", rules)

    def test_non_prod_low_replication_factor_ok(self):
        data = {"name": "orders", "environment": "dev", "replication_factor": 1, "dlq_topic": "orders.dlq"}
        rules = {f.rule_id for f in audit_kafka(data)}
        self.assertNotIn("KFK-001", rules)

    def test_prod_replication_factor_3_ok(self):
        data = {"name": "orders", "environment": "production", "replication_factor": 3, "dlq_topic": "orders.dlq"}
        rules = {f.rule_id for f in audit_kafka(data)}
        self.assertNotIn("KFK-001", rules)

    def test_min_insync_replicas_below_2(self):
        data = {"name": "orders", "configs": {"min.insync.replicas": 1}, "dlq_topic": "orders.dlq"}
        rules = {f.rule_id for f in audit_kafka(data)}
        self.assertIn("KFK-002", rules)

    def test_min_insync_replicas_2_ok(self):
        data = {"name": "orders", "configs": {"min.insync.replicas": 2}, "dlq_topic": "orders.dlq"}
        rules = {f.rule_id for f in audit_kafka(data)}
        self.assertNotIn("KFK-002", rules)

    def test_infinite_retention_without_compact(self):
        data = {"name": "orders", "configs": {"retention.ms": -1, "cleanup.policy": "delete"}, "dlq_topic": "orders.dlq"}
        rules = {f.rule_id for f in audit_kafka(data)}
        self.assertIn("KFK-003", rules)

    def test_infinite_retention_with_compact_ok(self):
        data = {"name": "orders", "configs": {"retention.ms": -1, "cleanup.policy": "compact"}, "dlq_topic": "orders.dlq"}
        rules = {f.rule_id for f in audit_kafka(data)}
        self.assertNotIn("KFK-003", rules)

    def test_missing_dlq_topic(self):
        data = {"name": "orders", "replication_factor": 3}
        rules = {f.rule_id for f in audit_kafka(data)}
        self.assertIn("KFK-004", rules)

    def test_consumer_auto_offset_reset_latest(self):
        data = {"group_id": "worker-1", "auto.offset.reset": "latest"}
        rules = {f.rule_id for f in audit_kafka(data)}
        self.assertIn("KFK-005", rules)

    def test_consumer_auto_offset_reset_earliest_ok(self):
        data = {"group_id": "worker-1", "auto.offset.reset": "earliest"}
        rules = {f.rule_id for f in audit_kafka(data)}
        self.assertNotIn("KFK-005", rules)


class TestSqsAuditor(unittest.TestCase):
    def test_missing_redrive_policy(self):
        data = {"QueueName": "jobs", "Attributes": {"VisibilityTimeout": 60}}
        rules = {f.rule_id for f in audit_sqs(data)}
        self.assertIn("SQS-001", rules)

    def test_with_redrive_policy_ok(self):
        data = {"QueueName": "jobs", "Attributes": {"RedrivePolicy": {"deadLetterTargetArn": "arn:dlq", "maxReceiveCount": 5}}}
        rules = {f.rule_id for f in audit_sqs(data)}
        self.assertNotIn("SQS-001", rules)

    def test_visibility_timeout_shorter_than_handler(self):
        data = {
            "QueueName": "jobs",
            "handler_timeout_seconds": 60,
            "Attributes": {"VisibilityTimeout": 30, "RedrivePolicy": "arn:dlq"},
        }
        rules = {f.rule_id for f in audit_sqs(data)}
        self.assertIn("SQS-002", rules)

    def test_visibility_timeout_longer_than_handler_ok(self):
        data = {
            "QueueName": "jobs",
            "handler_timeout_seconds": 30,
            "Attributes": {"VisibilityTimeout": 180, "RedrivePolicy": "arn:dlq"},
        }
        rules = {f.rule_id for f in audit_sqs(data)}
        self.assertNotIn("SQS-002", rules)


class TestNatsAuditor(unittest.TestCase):
    def test_unbounded_stream(self):
        data = {"config": {"name": "EVENTS", "max_bytes": -1, "max_age": 0}, "dlq_subject": "events.dlq"}
        rules = {f.rule_id for f in audit_nats(data)}
        self.assertIn("NTS-001", rules)

    def test_bounded_by_max_bytes_ok(self):
        data = {"config": {"name": "EVENTS", "max_bytes": 1048576, "max_age": 0}, "dlq_subject": "events.dlq"}
        rules = {f.rule_id for f in audit_nats(data)}
        self.assertNotIn("NTS-001", rules)

    def test_bounded_by_max_age_ok(self):
        data = {"config": {"name": "EVENTS", "max_bytes": -1, "max_age": 3600}, "dlq_subject": "events.dlq"}
        rules = {f.rule_id for f in audit_nats(data)}
        self.assertNotIn("NTS-001", rules)

    def test_missing_dlq_subject(self):
        data = {"config": {"name": "EVENTS", "max_bytes": 1024}}
        rules = {f.rule_id for f in audit_nats(data)}
        self.assertIn("NTS-002", rules)


if __name__ == "__main__":
    unittest.main()
