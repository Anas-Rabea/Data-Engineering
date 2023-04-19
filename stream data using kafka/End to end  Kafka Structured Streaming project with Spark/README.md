# Kafka + PySpark Project

This is a project that demonstrates how to consume data from Kafka using PySpark. The project includes an example application that reads data from a Kafka topic and performs some basic data processing using PySpark.

## Table of Contents

- [Getting Started](#getting_started)
- [Running the Application](#running_the_application)
- [Project Structure](#project_structure)
- [Dependencies](#running_the_application)
- [Screenshots](#screenshots)

## Getting Started

Before running the application, you will need to install Kafka and PySpark. You can find instructions on how to install these dependencies below:

### Kafka

- Download Kafka from [the Apache Kafka website](https://kafka.apache.org/downloads).

- Extract the downloaded file to a directory of your choice.

- Start the Zookeeper and Kafka server by running the following command from the extracted directory:

```
# Zookeeper server

bin/zookeeper-server-start.sh config/zookeeper.properties
```

```
# kafka server

bin/kafka-server-start.sh config/server.properties

```
### PySpark

- Install PySpark using pip by running the following command:
```
pip install pyspark

```


## Running the Application

To run the example application, follow the steps below:

1- Start the Kafka server (if it's not already running) by following the instructions above.

2- Create a Kafka topic by running the following command:
```
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic test-topic

```
3- Start producing messages to the Kafka topic by running the following command:
```
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic

```
 or we can use kafka-python module to do same task.
 
 4- Start consuming messages to the Kafka topic by running the following command:
```
bin/kafka-console-consumer.sh --broker-list localhost:9092 --topic test-topic --from-beginning

```



## Project Structure

The project consists of the following files:

1- [spark_application.py](https://github.com/Anas-Rabea/Data-Engineering/blob/main/stream%20data%20using%20kafka/End%20to%20end%20%20Kafka%20Structured%20Streaming%20project%20with%20Spark/spark_application.ipynb): This is the main PySpark application that reads data from Kafka, performs data processing and write stream on console.

2- [README.md](https://github.com/Anas-Rabea/Data-Engineering/blob/main/stream%20data%20using%20kafka/End%20to%20end%20%20Kafka%20Structured%20Streaming%20project%20with%20Spark/README.md): This file decribe the project summary, structure and dependencies.

3- [producer.py](https://github.com/Anas-Rabea/Data-Engineering/blob/main/stream%20data%20using%20kafka/End%20to%20end%20%20Kafka%20Structured%20Streaming%20project%20with%20Spark/producer.ipynb): A simulation for producing stream data using python.

## Dependencies

The project requires the following dependencies:

- Kafka
- PySpark
- Java

## Screenshots
- kafka data from simulator producer

![Screenshot of kafka data from simulator producer](https://github.com/Anas-Rabea/Data-Engineering/blob/main/stream%20data%20using%20kafka/End%20to%20end%20%20Kafka%20Structured%20Streaming%20project%20with%20Spark/dataproducer.png)
- kafka data after encoding

![Screenshot of kafka data after encoding](https://github.com/Anas-Rabea/Data-Engineering/blob/main/stream%20data%20using%20kafka/End%20to%20end%20%20Kafka%20Structured%20Streaming%20project%20with%20Spark/kafkasourcedata.png)
- kafka data being deserialized and processed

![Screenshot of kafka data being processed](https://github.com/Anas-Rabea/Data-Engineering/blob/main/stream%20data%20using%20kafka/End%20to%20end%20%20Kafka%20Structured%20Streaming%20project%20with%20Spark/processeddata.png)
- kafka data outputd after filteration and aggregation

![Screenshot of kafka data outputs](https://github.com/Anas-Rabea/Data-Engineering/blob/main/stream%20data%20using%20kafka/End%20to%20end%20%20Kafka%20Structured%20Streaming%20project%20with%20Spark/results.png)
