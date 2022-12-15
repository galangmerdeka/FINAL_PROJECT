import logging
import requests
import time

import threading
import time
import json
from datetime import datetime
from kafka import KafkaProducer

class ProducerThread(threading.Thread):
    """
    Producer in Thread

    Producer function that run in single thread, allowing multithread runtime.
    Inherited from Thread.

    params:
    - name: str. Thread process name (label).
    - args: tuple. Arguments used by producer function.
    - topic: str. Topic for producer to publish data.
    """
    def __init__(self, name: str, args: tuple, bootstrap_servers: str, topic: str):
        super().__init__(
            target=self._produce,
            name=name,
            args=args
        )

        self.active = False
        self._setup_publisher(bootstrap_servers=bootstrap_servers, topic=topic)
        self.logger = logging.getLogger(__name__)
    
    def start(self):
        self.active = True
        super().start()
    
    def stop(self):
        self.active = False

    def get_stream_data_from_api(url):
        data = requests.Session()

        with data.get(url, headers=None, stream=True) as response:
            for line in response.iter_lines():
                if line:
                    print(line)
    
    # Private Method
    def _produce(self, id: int):
        url = "https://www.freeforexapi.com/api/live?pairs=EURUSD,EURGBP,USDEUR"
        while(self.active):
            response_data = self.get_stream_data_from_api(url)

            self.logger.info(f"Creating data : {response_data}")

            self._publish_data(response_data)
            time.sleep(5)
    
    def _setup_publisher(self, bootstrap_servers, topic):
        kafka_log = logging.getLogger("Kafka Connection")
        kafka_log.setLevel(logging.ERROR)

        self.publisher = KafkaProducer(
            value_serializer = self._serializer,
            bootstrap_servers = bootstrap_servers
        )
        self.topic = topic

    def _publish_data(self, data: dict):
        future = self.publisher.send(self.topic, data)
        future.get(timeout = 5)
    
    def _serializer(self, data: dict):
        return json.dumps(data).encode("utf-8")
