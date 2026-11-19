import unittest

from eda_audit.schema_diff import diff_schemas


class TestAvroSchemaDiff(unittest.TestCase):
    def test_removed_required_field(self):
        old = {"type": "record", "name": "Ev", "fields": [{"name": "id", "type": "string"}]}
        new = {"type": "record", "name": "Ev", "fields": []}
        findings = diff_schemas(old, new)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].rule_id, "SCH-001")
        self.assertEqual(findings[0].severity, "CRITICAL")

    def test_removed_optional_field(self):
        old = {"type": "record", "name": "Ev", "fields": [{"name": "note", "type": "string", "default": ""}]}
        new = {"type": "record", "name": "Ev", "fields": []}
        findings = diff_schemas(old, new)
        self.assertEqual(findings[0].rule_id, "SCH-001")
        self.assertEqual(findings[0].severity, "HIGH")

    def test_type_change(self):
        old = {"type": "record", "name": "Ev", "fields": [{"name": "amt", "type": "long"}]}
        new = {"type": "record", "name": "Ev", "fields": [{"name": "amt", "type": "string"}]}
        findings = diff_schemas(old, new)
        self.assertEqual(findings[0].rule_id, "SCH-002")

    def test_new_required_field_without_default(self):
        old = {"type": "record", "name": "Ev", "fields": []}
        new = {"type": "record", "name": "Ev", "fields": [{"name": "tenant", "type": "string"}]}
        findings = diff_schemas(old, new)
        self.assertEqual(findings[0].rule_id, "SCH-003")

    def test_new_field_with_default_ok(self):
        old = {"type": "record", "name": "Ev", "fields": [{"name": "id", "type": "string"}]}
        new = {
            "type": "record",
            "name": "Ev",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "v", "type": "int", "default": 1},
            ],
        }
        self.assertEqual(diff_schemas(old, new), [])


class TestJsonSchemaDiff(unittest.TestCase):
    def test_removed_required_property(self):
        old = {"type": "object", "required": ["id"], "properties": {"id": {"type": "string"}}}
        new = {"type": "object", "required": [], "properties": {}}
        findings = diff_schemas(old, new)
        self.assertEqual(findings[0].rule_id, "SCH-001")

    def test_property_type_change(self):
        old = {"type": "object", "properties": {"count": {"type": "integer"}}}
        new = {"type": "object", "properties": {"count": {"type": "string"}}}
        findings = diff_schemas(old, new)
        self.assertEqual(findings[0].rule_id, "SCH-002")

    def test_new_required_property(self):
        old = {"type": "object", "required": [], "properties": {"a": {"type": "string"}}}
        new = {"type": "object", "required": ["a"], "properties": {"a": {"type": "string"}}}
        findings = diff_schemas(old, new)
        self.assertEqual(findings[0].rule_id, "SCH-003")

    def test_compatible_json_schema_ok(self):
        old = {"type": "object", "required": ["id"], "properties": {"id": {"type": "string"}}}
        new = {
            "type": "object",
            "required": ["id"],
            "properties": {"id": {"type": "string"}, "tag": {"type": "string"}},
        }
        self.assertEqual(diff_schemas(old, new), [])


if __name__ == "__main__":
    unittest.main()
