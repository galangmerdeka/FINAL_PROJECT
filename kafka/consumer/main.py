from sqlalchemy import create_engine
from kafka import KafkaConsumer
import json
import logging
import argparse
import time
from modules import consumer

# To consume latest message and auto-commit offset
consume = KafkaConsumer(
    topics="TopicCurrency",
    bootstrap_servers = ['kafka:9092'],
    auto_offset_reset = "earliest",
    value_deserializer = lambda m: json.loads(m.decode('utf-8')) 
)

def main(bootstrap_servers: str, topic: str, tablename: str, logger: logging.Logger):
    logger.info("Running Consumer")
    connection = consumer.Consumer(
        bootstrap_servers = bootstrap_servers,
        topic = topic,
        db_config = {
            "host": "localhost",
            "port": "5432",
            "user": "postgres",
            "password": "root"
        },
        tablename = tablename 
    )

    connection.start()

    try:
        while(True):
            time.sleep(60)
            logger.info("Consume is Running...")
    except KeyboardInterrupt:
        logger.info("Stopping Consumer")
        connection.stop()

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers")
    parser.add_argument("--topic")
    parser.add_argument("--tablename")

    arguments = parser.parse_args()
    return {
        "bootstrap_servers": arguments.bootstrap_servers,
        "topic": arguments.topic,
        "tablename": arguments.tablename
    }

if __name__ == "__main__":
    logging.basicConfig(
        level = logging.INFO,
        format = "[ %(asctime)s ] { %(name)s } - %(levelname)s - %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )

    logger = logging.getLogger(__name__)

    arguments = get_args()
    main(**arguments, logger=logger)



