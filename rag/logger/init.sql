CREATE STREAM IF NOT EXISTS rag_telemetry_stream (
    `query` VARCHAR,
    retrieved_chunks_count INT,
    reranked_chunks_count INT,
    retrieval_time_sec DOUBLE,
    rerank_time_sec DOUBLE,
    llm_generation_time_sec DOUBLE,
    total_latency_sec DOUBLE,
    answer_length_chars INT
) WITH (
    KAFKA_TOPIC='rag-telemetry',
    VALUE_FORMAT='JSON',
    PARTITIONS=1,
    REPLICAS=1
);

CREATE TABLE IF NOT EXISTS rag_pipeline_metrics_5min WITH (KAFKA_TOPIC='rag-metrics-5min') AS
    SELECT
        'RAG_PIPELINE' AS pipeline_id,
        COUNT(*) AS total_queries,
        ROUND(AVG(total_latency_sec), 4) AS avg_total_latency_sec,
        ROUND(MAX(total_latency_sec), 4) AS max_total_latency_sec,
        ROUND(AVG(retrieval_time_sec), 4) AS avg_retrieval_time_sec,
        ROUND(AVG(rerank_time_sec), 4) AS avg_rerank_time_sec,
        ROUND(AVG(llm_generation_time_sec), 4) AS avg_llm_time_sec,
        ROUND(AVG(answer_length_chars), 0) AS avg_answer_length_chars
    FROM rag_telemetry_stream
    WINDOW TUMBLING (SIZE 5 MINUTES)
    GROUP BY 'RAG_PIPELINE'
    EMIT CHANGES;
