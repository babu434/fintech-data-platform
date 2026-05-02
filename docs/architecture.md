# Fintech Data Platform — Architecture Decisions

## Project Overview
Real-time fraud detection + regulatory reporting + 
self-serve analytics on AWS for a fintech company.

## Design Decisions

### Why Delta Lake over Apache Iceberg?
- Better PySpark integration with AWS Glue
- MERGE/UPSERT support for Customer 360
- ACID transactions for safe concurrent writes
- Row-level deletes for GDPR compliance

### Why Flink over Spark Streaming?
- Lower latency: milliseconds vs seconds
- True streaming not micro-batch
- Stateful processing for velocity features
- Critical for 300ms fraud detection SLA

### Why Redis over DynamoDB for feature store?
- Sub-millisecond reads (DynamoDB is 1-10ms)
- Simple key-value perfect for user features
- TTL built-in for feature expiry
- Critical for staying under 300ms SLA

### Why Kinesis over MSK for ingestion?
- Fully managed — no broker management
- Native AWS integration with Lambda + Glue
- MSK used for internal event backbone only

### Why Step Functions for GDPR?
- Visual workflow — easy to audit for compliance
- Built-in retry + error handling
- Each deletion step tracked independently
- Full execution history for audit trail

## Layer Design
- Bronze: raw immutable data, 7yr retention
- Silver: cleaned, PII masked, schema validated
- Gold: aggregated, business-ready, analyst-facing

## Latency Budget (Fraud Detection)
- API Gateway:   20ms
- Flink enrich:  80ms
- Redis fetch:   10ms
- SageMaker:    150ms
- Decision:      50ms
- Total:        310ms (target < 500ms)
