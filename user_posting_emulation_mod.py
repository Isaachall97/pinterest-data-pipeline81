import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml

random.seed(100)

class AWSDBConnector:
    def __init__(self, db_creds='/Users/isaachall/Desktop/pinterest_project/db_creds.yaml'):
        with open(db_creds, 'r') as file:
            self.load_creds = yaml.safe_load(file)
        self.HOST = self.load_creds["HOST"]
        self.USER = self.load_creds["USER"]
        self.PASSWORD = self.load_creds["PASSWORD"]
        self.DATABASE = self.load_creds["DATABASE"]
        self.PORT = self.load_creds["PORT"]
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

def post_data_to_api(payload, invoke_url):
    response = requests.request("POST", invoke_url, data=payload, headers={"Content-Type": "application/vnd.kafka.json.v2+json"})
    if response.status_code != 200:
        print(f"Error posting to {invoke_url}, Status Code: {response.status_code}, Response: {response.text}")
    else:
        print(f"Data posted to {invoke_url}, Status Code: {response.status_code}")

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_string = text("SELECT * FROM pinterest_data LIMIT :row, 1")
            pin_result = connection.execute(pin_string, {'row': random_row}).fetchone()
            geo_string = text("SELECT * FROM geolocation_data LIMIT :row, 1")
            geo_result = connection.execute(geo_string, {'row': random_row}).fetchone()
            user_string = text("SELECT * FROM user_data LIMIT :row, 1")
            user_result = connection.execute(user_string, {'row': random_row}).fetchone()

            if pin_result and geo_result and user_result:
                pin_res = json.dumps({
                    "records" : [{
                        "value" : pin_result
                    }
                    ]
                }
                , default=str)
                geo_res = json.dumps({
                    "records" : [{
                        "value" : geo_result
                    }]
                    }
                , default=str)
                user_res = json.dumps({
                    "records" : [{
                        "value" : user_result
                    }]
                }, default=str)
                    
                post_data_to_api(pin_res, "https://lir2ur9ide.execute-api.us-east-1.amazonaws.com/0affee876ba9-stage/topics/0affee876ba9.pin")
                post_data_to_api(geo_res, "https://lir2ur9ide.execute-api.us-east-1.amazonaws.com/0affee876ba9-stage/topics/0affee876ba9.geo")
                post_data_to_api(user_res, "https://lir2ur9ide.execute-api.us-east-1.amazonaws.com/0affee876ba9-stage/topics/0affee876ba9.user")
                
           
if __name__ == "__main__":
    run_infinite_post_data_loop()