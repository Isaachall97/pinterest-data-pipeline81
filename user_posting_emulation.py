import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
from datetime import datetime
import sqlalchemy
from sqlalchemy import text


random.seed(100)


class AWSDBConnector:

    #Credentials to connect to the RDS database
    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306

    #Initialise engine 
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()
#JSON serializer for objects not serializable by default json code
def serialize_datetime(obj):
    
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

#Method wraps data in list of dicts with key "value" and value "payload"- where payload is later set to the content inside an RDS table
def post_data_to_api(payload, invoke_url):
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    format_payload = {
        "records" : [{"value" : payload}]
    }
    response = requests.request("POST", invoke_url, headers= headers, data=json.dumps(format_payload, default=serialize_datetime))
    
    print(f"Data posted to {invoke_url}, Status Code: {response.status_code}, Text: {response.text}, Content: {response.content}")

#Loops through all tables randomly to iterate through all datapoints
def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
 
        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                payload_pin = json.dumps(pin_result, default=serialize_datetime)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                payload_geo = json.dumps(geo_result, default= serialize_datetime)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                payload_user = json.dumps(user_result, default= serialize_datetime)
            
            #Calling post_data_to_api sends the data from the table to the correct Kafka topic via the invoke URL. 
            post_data_to_api(payload_pin, "https://lir2ur9ide.execute-api.us-east-1.amazonaws.com/0affee876ba9-stage/topics/0affee876ba9.pin") 
            post_data_to_api(payload_geo, "https://lir2ur9ide.execute-api.us-east-1.amazonaws.com/0affee876ba9-stage/topics/0affee876ba9.geo")
            post_data_to_api(payload_user, "https://lir2ur9ide.execute-api.us-east-1.amazonaws.com/0affee876ba9-stage/topics/0affee876ba9.user")     
            


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


