## TOMAS CONTI plugin

Starting from the original RAG project, I updated pipeline.py to incorporate an asynchronous telemetry component (telemetry.py).
The logger operates non-blockingly via a background thread loop and an async Kafka producer. Execution metrics are emitted as standard JSON events to the rag-telemetry Kafka topic for downstream consumption. To demonstrate advanced stream processing, I also included a ksqlDB setup that performs aggregations on the event stream to capture some system performance features(mock).

[**`rag/logger/docker-compose.yaml`**](rag/logger/docker-compose.yaml) — Docker Compose configuration orchestrating Apache Kafka in KRaft mode.

[**`rag/logger/init.sql`**](rag/logger/init.sql) — Initial database setup script for metrics aggregation and logging.

[**`rag/telemetry.py`**](rag/telemetry.py) — Thread-safe asynchronous Kafka telemetry producer using `aiokafka`.

[**`rag/pipeline.py`**](rag/pipeline.py) — Instrumented `SimpleRAGPipeline` with fine-grained latency profiling.

[**`examples/simple_rag_bm25_ollama.py`**](examples/simple_rag_bm25_ollama.py) — Runnable end-to-end example query script.

-----------

Model warm up 

```bash
#warm-up
ollama run llama3:instruct "hi"
```
Create The back end support by docker compose yaml:

```bash

cd rag/logger/

docker compose up -d
[+] Running 4/4
 ✔ Network logger_default   Created                                                                                                                                                0.0s 
 ✔ Container kafka-rag      Healthy                                                                                                                                                6.7s 
 ✔ Container ksqldb-server  Healthy                                                                                                                                               12.3s 
 ✔ Container ksqldb-init    Started                                                   
```

Check tabels:
```bash

docker exec -it ksqldb-server ksql http://localhost:8088 -e "SHOW STREAMS; SHOW TABLES;"

SHOW STREAMS;
 Stream Name          | Kafka Topic                                     | Key Format | Value Format | Windowed 
---------------------------------------------------------------------------------------------------------------
 KSQL_PROCESSING_LOG  | ksql_rag_telemetry_processorksql_processing_log | KAFKA      | JSON         | false    
 RAG_TELEMETRY_STREAM | rag-telemetry                                   | KAFKA      | JSON         | false    
---------------------------------------------------------------------------------------------------------------

SHOW TABLES;
 Table Name                | Kafka Topic      | Key Format | Value Format | Windowed 
-------------------------------------------------------------------------------------
 RAG_PIPELINE_METRICS_5MIN | rag-metrics-5min | KAFKA      | JSON         | true     
-------------------------------------------------------------------------------------
```

Follow the original repository steps for system startup.
Run the ollama server and RAG support:

```bash
python examples/simple_rag_bm25_ollama.py 
```

Check the Kafka topic log:

```bash

docker exec -it kafka-rag /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic rag-telemetry \
  --from-beginning
The consumer rebalance protocol (KIP-848) is production-ready! Set group.protocol=consumer to try it out. See https://kafka.apache.org/documentation/#consumer_rebalance_protocol
{"query": "What can Ollama do?", "retrieved_chunks_count": 10, "reranked_chunks_count": 3, "retrieval_time_sec": 0.0015, "rerank_time_sec": 0.7818, "llm_generation_time_sec": 35.5409, "total_latency_sec": 36.3242, "answer_length_chars": 599}
{"query": "hi", "retrieved_chunks_count": 10, "reranked_chunks_count": 1, "retrieval_time_sec": 0.0017, "rerank_time_sec": 0.8116, "llm_generation_time_sec": 11.0898, "total_latency_sec": 11.903, "answer_length_chars": 174}
{"query": "hi", "retrieved_chunks_count": 10, "reranked_chunks_count": 1, "retrieval_time_sec": 0.0102, "rerank_time_sec": 0.8517, "llm_generation_time_sec": 1.8984, "total_latency_sec": 2.7605, "answer_length_chars": 31}
```
Check the table aggregation into the db attach to Kafka:
```bash
docker exec -it ksqldb-server ksql http://localhost:8088 -e "SELECT * FROM RAG_PIPELINE_METRICS_LIVE WHERE PIPELINE_ID = 'RAG_PIPELINE';"
+---------+---------+---------+---------+---------+---------+---------+---------+
|PIPELINE_|TOTAL_QUE|AVG_TOTAL|MAX_TOTAL|AVG_RETRI|AVG_RERAN|AVG_LLM_T|AVG_ANSWE|
|ID       |RIES     |_LATENCY_|_LATENCY_|EVAL_TIME|K_TIME_SE|IME_SEC  |R_LENGTH_|
|         |         |SEC      |SEC      |_SEC     |C        |         |CHARS    |
+---------+---------+---------+---------+---------+---------+---------+---------+
|RAG_PIPEL|4        |39.5765  |40.8344  |0.0013   |0.8014   |38.7738  |599.0    |
|INE      |         |         |         |         |         |         |         |
Query terminated
```
---------------------------------------------
# Simple RAG
This is a simple RAG (Retrieval-Augmented Generation) that mostly self-implemented. This simple-rag package contain 4 modules:
- **Retrieval**: A retriever that retrieve the most relevant documents from a given corpus.
- **Rerank**: A reranker that rerank the retrieved documents.
- **LLM**: A language model that generate the answer.
- **Data Helper**: A helper that help to load the PDF data.


## Overview
The simple RAG pipeline is shown in the following figure:
![](/assets/flow.png)

## Installation

**Pre-requisites**:
- Python 3.6 or later
- Ollama (for LLM self-hosted)
- Poppler (for PDF processing)

To install poppler, select one of the following commands that is appropriate for your OS:
```bash
# Debian/Ubuntu
sudo apt install build-essential libpoppler-cpp-dev pkg-config python3-dev

# Fedora/RHEL
sudo yum install gcc-c++ pkgconfig poppler-cpp-devel python3-devel

# macOS
brew install pkg-config poppler python

# Windows (using conda)
conda install -c conda-forge poppler
```

Then, install the package using the following commands:
```bash
git clone https://github.com/behitek/simple-rag/
cd simple-rag
pip install -e .
```

## How to use
Here is an example of how to use the simple-rag package:
```python
import os

from rag.data_helper import PDFReader
from rag.llm import OllamaLLM
from rag.pipeline import Answer, SimpleRAGPipeline
from rag.rerank import CrossEncoderRerank
from rag.retrieval import BM25Retrieval
from rag.text_utils import text2chunk

# Set your PDF path here
sample_pdf = os.path.join(os.path.dirname(__file__), "sample.pdf")
contents = PDFReader(pdf_paths=[sample_pdf]).read()
text = " ".join(contents)
chunks = text2chunk(text, chunk_size=200, overlap=50)
print(f"Number of chunks: {len(chunks)}")

retrieval = BM25Retrieval(documents=chunks)
llm = OllamaLLM(model_name="llama3:instruct")
rerank = CrossEncoderRerank(model_name="cross-encoder/ms-marco-MiniLM-L-6-v2")
pipeline = SimpleRAGPipeline(retrieval=retrieval, llm=llm, rerank=rerank)


def run(query: str) -> Answer:
    return pipeline.run(query)


if __name__ == "__main__":
    query = "What can Ollama do?"
    print("Sample query:", query)
    response: Answer = pipeline.run(query)
    print(response.answer)
    print("Now, please ask your own questions!")
    while True:
        query = input("Your question: ")
        response: Answer = run(query)
        print(response.answer)
        print()
```

Result:
```bash
$ python examples/simple_rag_bm25_ollama.py 

Number of chunks: 10
Sample query: What can Ollama do?
Based on the provided context, what can Ollama do?

According to the text, Ollama can:

1. Self-host a lot of "top" open-source LLMs, including LLAMA2 (by Facebook), Mistral, Phi (from Microsoft), Gemma (by Google), and more.
2. Deploy a model with custom parameters.
3. Deploy a custom model from .GGUF format.
4. Support 4-bit quantization to save memory.
5. Handle several GPU types: NVIDIA, AMD, and Apple GPU.
6. Provide an OpenAI-compatible API.

Additionally, Ollama can also:

1. Run on multiple platforms: Windows (preview), MacOS, and Linux.
2. Deploy LLM without a GPU, although this is not explicitly tested in the context.
```

Please refer to the [examples](/examples) for more examples.
