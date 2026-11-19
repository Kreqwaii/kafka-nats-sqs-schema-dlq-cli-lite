# kafka-nats-sqs-schema-dlq-cli-lite

[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero%20(stdlib%20only)-brightgreen.svg)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Offline event-driven architecture (EDA) auditor** for Kafka topic/consumer configs, AWS SQS queue definitions, NATS JetStream stream configs, and Avro/JSON Schema backward-compatibility checks.  
**Офлайн-аудитор событийно-ориентированной архитектуры (EDA)** для конфигов Kafka, очередей AWS SQS, стримов NATS JetStream и проверки совместимости схем Avro / JSON Schema.

---

## Features / Возможности

- **100% Pure Python 3.9+ stdlib** — zero external dependencies, instant CI/CD execution.
- **Kafka Audit**: `replication_factor < 3` in prod, `min.insync.replicas < 2`, infinite retention (`retention.ms = -1`) without compaction, missing DLQ topic, `auto.offset.reset = latest` data-loss risk.
- **AWS SQS Audit**: missing `RedrivePolicy` (DLQ), `VisibilityTimeout` shorter than handler timeout.
- **NATS JetStream Audit**: missing `max_bytes` / `max_age` storage cap, missing `dlq_subject`.
- **Schema Compatibility Diff**: Avro & JSON Schema breaking changes (removed required fields, type changes, new required fields without defaults).
- **CI/CD Ready**: JSON & Markdown output, configurable `--fail-on` severity exit codes.

---

## Quick Start / Быстрый старт

```bash
# Audit all platforms + schema diff in Markdown
python audit.py \
  --kafka samples/kafka_risky.json \
  --sqs samples/sqs_risky.json \
  --nats samples/nats_risky.json \
  --schema-diff samples/schema_v1.json samples/schema_v2.json \
  --format markdown \
  --fail-on HIGH

# JSON output for CI pipelines
python audit.py --kafka samples/kafka_risky.json --format json --fail-on CRITICAL
```

---

## Rule Table / Таблица правил

| Rule ID | Platform | Severity | Description (EN / RU) |
| --- | --- | --- | --- |
| `KFK-001` | Kafka | `CRITICAL` | `replication_factor < 3` in production / Низкий фактор репликации в prod |
| `KFK-002` | Kafka | `HIGH` | `min.insync.replicas < 2` / Недостаточный `min.insync.replicas` |
| `KFK-003` | Kafka | `HIGH` | `retention.ms = -1` without `compact` / Бесконечное хранение без компакции |
| `KFK-004` | Kafka | `MEDIUM` | Missing Dead-Letter Queue topic / Отсутствует DLQ-топик |
| `KFK-005` | Kafka | `HIGH` | `auto.offset.reset = latest` data-loss risk / Риск потери данных при старте консьюмера |
| `SQS-001` | SQS | `HIGH` | Missing `RedrivePolicy` (DLQ) / Отсутствует DLQ (`RedrivePolicy`) |
| `SQS-002` | SQS | `CRITICAL` | `VisibilityTimeout < handler_timeout` / Таймаут видимости меньше таймаута обработчика |
| `NTS-001` | NATS | `HIGH` | Missing `max_bytes` and `max_age` cap / Нет лимита на размер или время хранения стрима |
| `NTS-002` | NATS | `MEDIUM` | Missing `dlq_subject` / Не задан `dlq_subject` |
| `SCH-001` | Schema | `CRITICAL`/`HIGH` | Removed field / Удалено поле из схемы |
| `SCH-002` | Schema | `CRITICAL` | Breaking field type change / Изменён тип поля |
| `SCH-003` | Schema | `HIGH` | New required field without default / Новое обязательное поле без значения по умолчанию |

---

## Need the full Pro Studio?

Looking for the **Pro Studio** with **Web UI, consumer-lag simulator, SARIF 2.1.0 export, and multi-cluster diff**?  
Get it here: **[https://funpay.com/users/20096179/](https://funpay.com/users/20096179/)**
