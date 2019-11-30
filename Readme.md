
## TODO
* [x] Spark app for batch
* [] Kibana displays street level
* [] Handle HDFS if samll size
* [] Optimize code
* [x] Use scala spark


## Requirement
* Kafka (Host on AWS EC2)
* AWS EMR (Run Spark-2.4.3 application and use HDFS)
* ElasticSearch (EC2)
    - or you could use [Bonsai](https://bonsai.io) instead

## System Architecture
<img src="./img/flowchart.png" height=auto>

## Prepare Data
* Get open source data from the website below

```bash
wget https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
```

```bash
python prepare.py
```
## Producer
* We will send message to our kafka server

```bash
python producer.py
```

## Spark Stream

* Steps

* Deploy to EMR
  1. Download Kafka + Spark Streaming JAR of Maven `spark-streaming-kafka-0-8-assembly_2.11-2.4.3.jar`

  2. packing your dependencies
  use pipenv to pack and generate ZIP file
  [ref](https://realpython.com/pipenv-guide/)

  3. EMR Add steps to run streaming App in the cluser

    ```bash
    spark-submit --jars s3://emr-harry/spark-streaming-kafka-0-8-assembly_2.11-2.4.3.jar --master yarn --deploy-mode cluster --num-executors 3 --executor-cores 3 --executor-memory 3g --py-files s3://emr-harry/project.zip s3://emr-harry/taxiSparkStreaming.py
    ```

<img src="./img/Dashboard.png" height=auto>
