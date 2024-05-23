# Pinterest Data Pipeline

## Introduction

In the digital age, the ability to handle vast amounts of data efficiently and effectively can define the success of a platform. Pinterest, a major player in the social media landscape, processes billions of data points daily to enhance user engagement and deliver value. Inspired by this, this project aims to develop a similar data processing system, utilizing the robust capabilities of the AWS Cloud. This data pipeline emulates key aspects of Pinterest's approach to data management and analytics, focusing on scalability, reliability, and speed.

## Installation Requirements and Prerequisites

Before deploying and running this data pipeline, the following prerequisites and installation requirements were met:

### AWS Configuration

1. **AWS Credentials**: AWS credentials configured as an IAM user with appropriate permissions to access the necessary AWS services.
2. **EC2 Key Pair**:
   - Created a `.pem` key pair file through the AWS Management Console. This file stores the specific key pair associated with an EC2 instance.
   - Correct permissions were set for the key pair file on local machine using `chmod 400 your-key-pair.pem`.

### Connecting to EC2

- **SSH Access**:
  - Use the SSH client integrated in the EC2 console or the preferred SSH tool to connect to the EC2 instance.

### Software Installation on EC2

1. **Kafka Installation**:
   - Installed Kafka version 2.12-2.8.1 on the client EC2 machine. The specific instructions for Kafka installation and setup are in the [official Kafka documentation](https://kafka.apache.org/28/documentation.html).
2. **IAM MSK Authentication**:
   - Install the IAM MSK authentication package on the EC2 instance to enable secure communication with the Kafka cluster.
3. **Kafka Client Configuration**:
   - Configure the Kafka client to use AWS IAM authentication for secure data transactions with the Kafka cluster. This setup ensures that the data pipeline integrates smoothly with AWS managed services. This is done in the `kafka_folder/bin` directory, by modifying the `client.properties` file. 

Ensure that all software installations and configurations are done correctly to avoid issues during the deployment and operation of the data pipeline. 

### Creation of Kafka topics

 **Retrieve information about the MSK cluster**:
- Both the Bootstrap server string and the Plaintext Apache Zookeeper connection string of the MSK cluster are retrieved from the MSK console.
- Three topics are created:
  1. `user-id.pin` - for the Pinterest posts data
  2. `user-id.geo` - for the Pinterest post geolocation data 
  3. `user-id.user` - for the Pinterest post user data
- The Bootstrap server string is used in the kafka `create topic` command

### Batch Processing- Connecting the MSK cluster to an S3 bucket

1. **Create custom plugin with MSK connect**
- Assuming the target S3 bucket has already been created, the name must be noted for later use.
- On the EC2 client, the Confluent.io Amazon S3 Connector is downloaded and copied to the S3 bucket identified in the previous step.
- Custom plugin is created using the MSK connect console.

2. **Create a connector with MSK connect**
- When building the connector, the IAM role used for authentication to the MSK cluster must be chosen in the Access permissions tab.
- Now that the plugin-connector pair has been created, data passing through the IAM authenticated cluster, will be automatically stored in the designated S3 bucket.

### Batch Processiong- Configure an API in API gateway

1. **Build a Kafka REST proxy integration for the API**
- Resource must be created that allows a PROXY integreation for the API
- For the previously created resource, an HTTP `ANY` method is created. The `Endpoint URL` must be the `PublicDNS` from the EC2 machine in previous steps.
- The API is deployed. `Invoke URL` is noted.

2. **Set up KAFKA REST proxy on the EC2 client**
- Confluent package must be installed for the Kafka REST proxy on the EC2 client machine.
- `kafka-rest.properties` file must be modified to allow the REST proxy to perform IAM authentication to the MSK cluster.
- The REST proxy can now be deployed on the EC2 client machine.

3. **Send data to the API**
- The file [`user-posting-emulation.py`](user_posting_emulation.py) was originally provided as a starting point- it contained the login credentials for an RDS database. This contains 3 tables with data resembling data received by the pinterest API when a `POST` request is made by a user uploading data to pinterest. A screenshot of the code provided is below:
  
![original user_posting_emulation](https://private-user-images.githubusercontent.com/55752358/325586012-d0f9b9e0-a109-41a2-8829-d2efb70e069a.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MTQwNDQ4MDEsIm5iZiI6MTcxNDA0NDUwMSwicGF0aCI6Ii81NTc1MjM1OC8zMjU1ODYwMTItZDBmOWI5ZTAtYTEwOS00MWEyLTg4MjktZDJlZmI3MGUwNjlhLnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNDA0MjUlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjQwNDI1VDExMjgyMVomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTU1NmQ1M2IzNGU0ZDg0MTA2ZWVkNGE3OWQ4ZGYxY2NiYzVlYzEwNGI1MWRiNzA0NDdiYzliNWM0OTMxYTBmMmYmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0JmFjdG9yX2lkPTAma2V5X2lkPTAmcmVwb19pZD0wIn0.53MT-eDSAq-96wUFjv3WHFgUFXVcICCivaNNlRpyMUw)

- This file was then modified in order to send data to the Kafka topics previously initialised (`user-id.pin`, `user-id.geo`, `user-id.user`) using the API invoke URL.
- Data from the three tables needs to be stored in their corresponding Kafka topic
- A screenshot of the modified code is below- the full script can be found here- [`user_posting_emulation_mod.py`](user_posting_emulation_mod.py):

![user_posting_emulation_mod](https://private-user-images.githubusercontent.com/55752358/325590600-a0903c64-d6e8-47b4-9a89-d507be367ed5.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MTUwOTQ4MTksIm5iZiI6MTcxNTA5NDUxOSwicGF0aCI6Ii81NTc1MjM1OC8zMjU1OTA2MDAtYTA5MDNjNjQtZDZlOC00N2I0LTlhODktZDUwN2JlMzY3ZWQ1LnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNDA1MDclMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjQwNTA3VDE1MDgzOVomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTQ4Mzc2MDY0OGViODE4NzJiYTNmYTQ4M2VlNmIzMDZlOTViODMxNGFiYmM3NDY2NTgzMGE3YjI2ZDEyZWFmNjkmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0JmFjdG9yX2lkPTAma2V5X2lkPTAmcmVwb19pZD0wIn0.YlFBwPivgJIR3URhBJclD4peCichv5WIqlN3U01sHaw)

- When this script is run, data is sent to the MSK cluster. This can be checked by running a Kafka consumer for each topic. Messages will be consumed while the consumer is running.
- The data is ultimately being stored in the S3 bucket as per the topic names. A screen capture can be seen below of what this should look like:
  ![S3_bucket_screencapture](https://private-user-images.githubusercontent.com/55752358/325594024-3942afa4-8714-4548-81a2-56cc5352f9fe.gif?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MTQwNDYyNTYsIm5iZiI6MTcxNDA0NTk1NiwicGF0aCI6Ii81NTc1MjM1OC8zMjU1OTQwMjQtMzk0MmFmYTQtODcxNC00NTQ4LTgxYTItNTZjYzUzNTJmOWZlLmdpZj9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNDA0MjUlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjQwNDI1VDExNTIzNlomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPWNmODRmZDEzM2NjNzMxY2RhNDhjNGE0ZjFhZGZlMjIwZGI4ZWM0NWZmNjkwY2EwOTE1NzNlMGQ3Y2IwYjRlMjUmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0JmFjdG9yX2lkPTAma2V5X2lkPTAmcmVwb19pZD0wIn0.R4t2x5PQZkZ-tciHLocmWkq-0qa8VP6TH3BKEGwYVwQ)

## Batch processing- Databricks

Once the topics have been stored in the AWS S3 bucket, the data must be read into Databricks where it can be transformed and queried.

### Mount S3 bucket to Databricks

- In order to clean and query the batch data, the data from the S3 bucket must be read into Databricks. To do this, the S3 bucket must be mounted to the Databricks account.
- Details of this code can be found here: [`mount_s3_to_databricks.py`](mount_s3_to_databricks.py)

### Batch processing- Spark on Databricks

- After data has been mounted, it is then loaded into spark dataframes, where it can be cleaned
- Various cleaning operations are performed dependent on the specific needs of the data.
- After cleaning has been performed, queries are done on the transformed data in order to verify the quality.
- All cleaning and query operations can also be found in [`mount_s3_to_databricks.py`](mount_s3_to_databricks.py)



  
