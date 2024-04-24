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
   - Configure the Kafka client to use AWS IAM authentication for secure data transactions with the Kafka cluster. This setup ensures that the data pipeline integrates smoothly with AWS managed services.

Ensure that all software installations and configurations are done correctly to avoid issues during the deployment and operation of the data pipeline.

