import logging

import json
import time
from sqlalchemy import create_engine, engine, text
from datetime import datetime
from kafka import KafkaConsumer

from . import database

class Consumer(KafkaConsumer):
    def __init__(self, bootstrap_servers: str, topic: str, db_config: dict, tablename: str):
        super().__init__(topic, bootstrap_servers = [bootstrap_servers], value_deserializer = self._deserializer)



    def start(self):
        self.activate = True
        self.start()
    
    def stop(self):
        self.activate = False
    
    def _deserializer(self, data: bytes) -> dict:
        return json.loads(data.decode("utf-8"))
    
    def _consume_data(self):
        while(self.activate):
            data = self.poll(timeout_ms=200)
            for data, message in data.items():
                formatData = self._data_format(message=value)
                for data in formatData:
                    logging.info(f"FORMATED : {data}")
                    database.ins

    def _data_format(self, message : str) -> list:
        dataJson = json.loads(message)
        dataJson = dataJson["rates"]
        currencies = {
            'EURUSD': 'US Dollar', 
            'EURGBP': 'Pound Sterling', 
            'USDEUR': 'Euro'
        }
        formatData = []
        for code, name in currencies.items():
            formatData = {
                "currency_code" : code,
                "currency_name" : name,
                "rate" : dataJson[code]["rates"],
                "timestamp" : dataJson[code]["timestamp"]
            }
            formatData.append(formatData)
        return formatData
    
    def get_engine(host: str, port: int, user: str, password: str):
        try:
            db = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres"
            logging.info(f"CONNECTION DB : url = {db}")
            engine = create_engine(db)
            return engine
        except Exception as e:
            logging.info("LOG ERROR CONNECTION DB")
            logging.exception(msg=e)
    
    def insert_into_db(enging: engine.Engine, data: dict, tablename: str):
        with engine.begin() as cn :
            cn.execute(
                text(
                    f"""
                        INSERT INTO {tablename} (currency_code, currency_name, rate, timestamp) 
                        VALUES (:currency_code, :currency_name, :rate, :timestamp)
                    """
                ),
                data
            )