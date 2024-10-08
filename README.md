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
- The file [`user-posting-emulation.py`](user_posting_emulation.py) was originally provided as a starting point- it contained the login credentials for an RDS database. This contains 3 tables with data resembling data received by the pinterest API when a `POST` request is made by a user uploading data to pinterest.

- This file was then modified in order to send data to the Kafka topics previously initialised (`user-id.pin`, `user-id.geo`, `user-id.user`) using the API invoke URL.
- Data from the three tables needs to be stored in their corresponding Kafka topic
- The full script can be found here- [`user_posting_emulation_mod.py`](user_posting_emulation_mod.py):

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


### Batch processing- AWS MWAA

- The script created using Spark on Databricks [`mount_s3_to_databricks.py`](mount_s3_to_databricks.py) can currently only be run manually. However, it can be automated using AWS MWAA.
- AWS MWAA (Managed Workflows for Apache Airflow) is a managed service that was designed to helpintegrate Apache Airflow straight in the cloud, with minimal setup and the quickest time to execution. Apache Airflow is a tool used to schedule and monitor different sequences of processes and tasks, referred to as workflows.
- For this project, an environment had already been created which was linked with an accessible S3 bucket to store data in. If this is not the case, an API token will need to be created in Databricks to connect your AWS account.
- The DAG [`0affee876ba9_dag.py`](0affee876ba9_dag.py) was created. This specifies parameters such as the filepath to the notebook which is to be run, the frequency of the run, the number of retries should the DAG fail and the authentication credentials to access the notebook.
- Once this is created, it must be uploaded to the S3 bucket linked to the MWAA environment.
- The DAG 0affee876ba9_dag.py was scheduled to run daily, so captures clusters of data on a daily basis.
- Once uploaded to MWAA, you should be able to follow the link to the Airflow UI and see the code along with other key metrics on there. The DAG can also be manually triggered to check that it runs correctly.
  ![Airflow_UI_Screenshot](https://private-user-images.githubusercontent.com/55752358/338727886-844183b7-9257-48d8-a85b-ed5ff6470f69.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MTgxMzYxMDUsIm5iZiI6MTcxODEzNTgwNSwicGF0aCI6Ii81NTc1MjM1OC8zMzg3Mjc4ODYtODQ0MTgzYjctOTI1Ny00OGQ4LWE4NWItZWQ1ZmY2NDcwZjY5LnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNDA2MTElMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjQwNjExVDE5NTY0NVomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTZiNGM5NmZlZjc5OGY4MmE5NWVjYTliNTc1ZjkzYmIwOTk4ZDMzYzkyOTMzMDU5YzkyMWFjYzgwNTZiZjU0NzEmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0JmFjdG9yX2lkPTAma2V5X2lkPTAmcmVwb19pZD0wIn0.l44Wf_dIq6cy4HLDDL7t5S9l5ucdSgjlxchQmgzv724)

## Stream processing- Databricks and Kinesis 

### AWS Kinesis

- The previous batch processing method was sufficient for data which does not need to be ingested in real time. However, sometimes it is useful to stream data. We can use AWS Kinesis for this purpose.
- Firstly, on the Kinesis console, three data streams must be made- one for each data type- the posts data, the geolocation data and the user data.
- Next, an API must be configured with Kinesis proxy integration. We can use the previously created REST API and modify it so that it can invoke Kinesis actions.
- Make sure that your account has the necessary permissions to do this.
- The API should be configured so that it can 1) List streams in Kinesis, 2) Create, Describe and Delete streams in Kinesis, and 3) Add records to stream in Kinesis
- Once this has been done, data can be sent to the Kinesis streams. The script 'user_posting_emulation.py' can be modified so that requests are sent to the Kinesis-integrated API which allow records to be added one at a time to the streams previously created by topic. The code needed for this can be observed in [`user_posting_emulation_streaming.py`](user_posting_emulation_streaming.py)
- Ensure that the database credentials are encoded in a seperate file.
- If the script is successful in connecting and sending data to the Kinesis stream, the data should be able to be viewed using the 'Data Viewer' tab inside the Kinesis console. It should look something like this:
  ![Kinesis_Stream_Screenshot](https://private-user-images.githubusercontent.com/55752358/338732716-26809978-2644-4346-be83-97cbfe4c1fef.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MTgxMzcxNjQsIm5iZiI6MTcxODEzNjg2NCwicGF0aCI6Ii81NTc1MjM1OC8zMzg3MzI3MTYtMjY4MDk5NzgtMjY0NC00MzQ2LWJlODMtOTdjYmZlNGMxZmVmLnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNDA2MTElMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjQwNjExVDIwMTQyNFomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTZlZTc0NzdlNjEwOTg5MTU0NTUzZDQyMTM5OGI2M2RiNDc5ODkzNGVmMTIxN2FiMzZmMDE0ZThmMDJmNTFjZGUmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0JmFjdG9yX2lkPTAma2V5X2lkPTAmcmVwb19pZD0wIn0.8KD8TW0EIjqHO3P6h60kllqXADGZIbqIhVU2Y9JecWM)


### Reading data from Kinesis to Databricks

- Once the data has been set to stream using Kinesis, we can link it to Databricks using the access key and secret access key in the Delta Table created for the batch data processing.
- The code used to link the Kinesis Stream to Databricks can be found in [`pinterest_data_kinesis_stream.py`](pinterest_data_kinesis_stream.py).
- The schema is specified so that data can be read into a Spark dataframe with data in the correct columns.
- The JSON data stored in Kinesis is then parsed into the defined schema, with each key as a column.
- The data can then be transformed and cleaned like a normal Spark dataframe.
- Finally, this can be written and saved to a Delta Table. 


  



  
