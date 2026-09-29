import time
from abc import ABC

from .llm import BaseLLM
from .prompt import ANSWER_PROMPT
from .rerank import BaseRerank
from .retrieval import BaseRetrieval
from .telemetry import KafkaTelemetryLogger


class Answer:
    def __init__(self, answer: str, contexts: list[str]):
        self.answer = answer
        self.contexts = contexts


class Pipeline(ABC):
    def __init__(self, *args, **kwargs):
        pass

    def run(self, query: str) -> Answer:
        raise NotImplementedError


class SimpleRAGPipeline(Pipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not kwargs.get("retrieval"):
            raise ValueError("Please provide a `retrieval` model.")
        if not kwargs.get("llm"):
            raise ValueError("Please provide a `llm` model.")

        self.retrieval = kwargs.get("retrieval")
        assert issubclass(self.retrieval.__class__, BaseRetrieval)

        self.rerank = kwargs.get("rerank")
        if self.rerank:
            assert issubclass(self.rerank.__class__, BaseRerank)

        self.llm = kwargs.get("llm")
        assert issubclass(self.llm.__class__, BaseLLM)

        self.retrieval_top_k = kwargs.get("retrieval_top_k", 100)
        self.rerank_top_k = kwargs.get("rerank_top_k", 3)

        # KafkaTelemetryLogger init
        enable_telemetry = kwargs.get("enable_telemetry", True)
        self.telemetry = KafkaTelemetryLogger() if enable_telemetry else None

    def run(self, query: str) -> Answer:
        total_start = time.time()

        t0 = time.time()
        relevant_docs, relevant_meta = self.retrieval.retrieve( query, top_k=self.retrieval_top_k )
        retrieval_time = time.time() - t0

        t1 = time.time()
        if self.rerank:
            reranked_docs, scores = self.rerank.rerank( query, relevant_docs, top_k=self.rerank_top_k )
        else:
            reranked_docs = relevant_docs
        rerank_time = time.time() - t1

        prompt = ANSWER_PROMPT.format(query=query, context="\n".join(reranked_docs))
        t2 = time.time()
        answer_text = self.llm.generate(prompt)
        llm_time = time.time() - t2

        total_time = time.time() - total_start

        # JSON Metrics Formatting for the kafka event
        metrics = {
            "query": query,
            "retrieved_chunks_count": len(relevant_docs),
            "reranked_chunks_count": len(reranked_docs),
            "retrieval_time_sec": round(retrieval_time, 4),
            "rerank_time_sec": round(rerank_time, 4),
            "llm_generation_time_sec": round(llm_time, 4),
            "total_latency_sec": round(total_time, 4),
            "answer_length_chars": len(answer_text)
            if isinstance(answer_text, str)
            else len(str(answer_text)),
        }

        for k, v in metrics.items():
            print(f"  {k}: {v}")
        

        # Non-blocking async event logging via Kafka
        if self.telemetry:
            self.telemetry.log_event(metrics)

        return Answer(answer=answer_text, contexts=reranked_docs)
