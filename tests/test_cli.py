import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from eda_audit.cli import main

SAMPLES = Path(__file__).resolve().parent.parent / "samples"


class TestCli(unittest.TestCase):
    def test_cli_no_args_returns_2(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = main([])
        self.assertEqual(rc, 2)

    def test_cli_json_output_and_fail_on(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = main(["--kafka", str(SAMPLES / "kafka_risky.json"), "--format", "json", "--fail-on", "CRITICAL"])
        self.assertEqual(rc, 1)
        data = json.loads(buf.getvalue())
        self.assertGreaterEqual(data["total_findings"], 5)

    def test_cli_fail_on_none_returns_0(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = main(["--sqs", str(SAMPLES / "sqs_risky.json"), "--fail-on", "NONE"])
        self.assertEqual(rc, 0)

    def test_cli_schema_diff_markdown(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = main([
                "--schema-diff",
                str(SAMPLES / "schema_v1.json"),
                str(SAMPLES / "schema_v2.json"),
                "--format",
                "markdown",
                "--fail-on",
                "HIGH",
            ])
        self.assertEqual(rc, 1)
        self.assertIn("SCH-002", buf.getvalue())

    def test_cli_missing_file_returns_2(self):
        rc = main(["--nats", str(SAMPLES / "does_not_exist.json")])
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
