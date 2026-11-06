# Building Real-Time Data Pipelines with Kafka and Spark Streaming

Repository ini merupakan assignment dari bootcamp Data Engineering Dibimbing. Repository ini berisi implementasi *Real-Time Data Pipeline* menggunakan **Apache Kafka** untuk *mengirimkan message* dan **Apache Spark** untuk pemrosesan data, validasi,dan  routing data ke valid data dan Dead Letter Queue (DLQ).

## Tech Stack
* **Python** (PySpark, Confluent Kafka)
* **Apache Kafka & Zookeeper** (via Docker Compose)
* **Apache Spark** 
* **Docker & Docker Compose**

## Project Structure
```text
Day 51/
│
├── producer/
│   └── producer.py            # Script Python untuk membuat Kafka producer dan mengirimkan message
├── streaming/
│   └── spark_streaming_job.py # Script PySpark untuk streaming, validasi, dan data routing
├── checkpoints/               # checkpoint Spark streaming
├── docker-compose.yml         # Konfigurasi infrastruktur Docker
└── README.md     
```        

## Cara Menjalankan Project
1. Menjalankan Docker Compose

```text
docker-compose up -d
```

2. Menjalankan Event Producer

```text
python producer/producer.py
```
 
3. Menjalankan Spark 

```text
python streaming/spark_streaming_job.py
```

## Screenshot

1. Kafka Producer

![alt text](<Screenshot Kafka Producer.png>)

2. Spark Streaming Job

![alt text](<Screenshot streaming console output-2.png>)

3. Topik Valid

![alt text](<Screenshot topik valid.png>)

4. Topik DLQ

![alt text](<Screenshot topik DLQ.png>)
