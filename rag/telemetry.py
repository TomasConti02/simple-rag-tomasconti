import asyncio
import json
import logging
import threading
from typing import Any, Dict
from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)


class KafkaTelemetryLogger:

    def __init__( self, bootstrap_servers: str = "localhost:9092", topic: str = "rag-telemetry", ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.enabled = False

        # event loop thread in back ground operate with a corutine principle 
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        #asynch kafka producer
        asyncio.run_coroutine_threadsafe(self._start_producer(), self._loop)

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _start_producer(self):
        try:
            self.producer = AIOKafkaProducer( bootstrap_servers=self.bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                                             request_timeout_ms=3000, )
            await self.producer.start()
            self.enabled = True
            print(f"[Kafka Telemetry] Connected to broker on {self.bootstrap_servers}")
        except Exception as e:
            logger.warning(f"[Kafka Telemetry] Connection failed: {e}")
            self.enabled = False

    def log_event(self, metrics: Dict[str, Any]): # log is no blocking. lets operate the backgrounf thread
        if self.enabled and self.producer:
            asyncio.run_coroutine_threadsafe( self._async_send(metrics), self._loop )

    async def _async_send(self, metrics: Dict[str, Any]):
        try:
            metadata = await self.producer.send_and_wait(self.topic, value=metrics)
            print( f"[Kafka Telemetry] Event emitted -> topic '{metadata.topic}' " f"(partition {metadata.partition})" )
        except Exception as e:
            logger.error(f"[Kafka Telemetry] Delivery error: {e}")

    def stop(self): 
        if self.enabled and self.producer:
            future = asyncio.run_coroutine_threadsafe( self.producer.stop(), self._loop )
            try:
                future.result(timeout=3)
            except Exception:
                pass
            self._loop.call_soon_threadsafe(self._loop.stop)
