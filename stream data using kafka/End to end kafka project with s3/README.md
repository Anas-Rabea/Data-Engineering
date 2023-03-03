# Summary :
### Executing an End-To-End Data Engineering Project on Real-Time Data using Kafka and different technologies such as Python, Amazon Web Services (AWS).

# Project Steps:
#### 1 - Create AWS account. 
#### 2 - Launch EC2 machine (AWS Linux) with S3FullAccess role and create a Bucket.
#### 3 - Connect to the ec2 machine using ssh after editing the inbound role to All traffic .
#### 4 - download the kafka and install java openjdk.
#### 5 - activate the zookeeper and kafka server; we may need to modify the kafka used memory.
#### 6 - create a topic and producer to ingest data.
#### 7 - create the consumer to subscribe the topic and try to test to make sure the producer works fine.
#### 8 - Create consumer.py, producer.py file to simulate the stream data ingestion.
#### 9 - Upload data to S3 directly or save locally then upload using boto3 and s3fs.




