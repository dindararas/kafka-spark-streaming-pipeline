from pyspark.sql import  SparkSession, functions as F, types as T
from pyspark.sql.functions import col, expr, from_json, when, to_timestamp, window, sum, count
import logging
import os

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += r';C:\hadoop\bin'

# konfigurasi kafka
INPUT_TOPIC = 'transactions'
VALID_OUTPUT_TOPIC = 'transactions_valid'
DLQ_TOPIC = 'transactions_dlq'

# konfigurasi logging
logging.basicConfig(
    level=logging.DEBUG, # akan menampilkan semua level logging : DEBUG -> INFO -> WARNING -> ERROR -> CRITICAL
    format= '%(asctime)s | %(levelname)s | %(message)s') # format output: timestamp | DEBUG/INFO/WARNING/ERROR/CRITICAL | message
logger = logging.getLogger(__name__)

def get_spark_session():
    """
    Fungsi untuk menyiapkan session Spark dengan package kafka-sql
    """
    return SparkSession.builder \
        .appName('KafkaSparkIntegration') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

def get_schema() :
    """
    Fungsi untuk mendefinisikan schema
    """
    return T.StructType([
        T.StructField('user_id', T.StringType(), True),
        T.StructField('amount', T.StringType(), True),
        T.StructField('timestamp', T.StringType(), True),
        T.StructField('source', T.StringType(), True),
    ])

def main() :
    spark = get_spark_session()
    spark.sparkContext.setLogLevel('WARN')

    logging.info('Mulai membaca aliran data dari Kafka...')

    # membaca data dari Kafka
    raw_stream = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', INPUT_TOPIC) \
        .option('startingOffsets', 'latest') \
        .load()

    # cast data dari Kafka ke string lalu parse sebagai JSON
    schema = get_schema()
    parsed_stream = raw_stream.selectExpr('CAST(value AS STRING) as json_val') \
        .select(from_json(col('json_val'), schema).alias('data')) \
        .select('data.*') \
        .withColumn('event_time', to_timestamp(col('timestamp'))).withWatermark('event_time', '3 minutes')
    

    # mengecek data yang valid
    # membuat kolom error_reason
    validated_data = parsed_stream.withColumn('error_reason',
                                          # 1. Mandatory field check (user_id, amount, timestamp)
                                          when(
                                              col('user_id').isNull() | col('amount').isNull() | F.col('timestamp').isNull(), 'Mandatory field missing')
                                          # 2. Type validation
                                          # timestamp
                                          .when(col('event_time').isNull(), 'Invalid Type')
                                          # amount
                                          .when(col('amount').cast('double').isNull(), 'Invalid Type')
                                          # 3. Range validation untuk amount (1 – 10.000.000)
                                          .when((col('amount').cast('double') < 1) | (col('amount').cast('double') > 10000000), 'Invalid Amount')
                                          # 4. Source validation (“mobile”, “web”, “pos”)
                                          .when(~col('source').isin('mobile', 'web', 'pos'), 'Invalid Source')
                                          # Late event
                                          .when(col('event_time') < expr("current_timestamp() - interval 3 minutes"), 'Late event')
                                          .otherwise('Valid')
                                        )

    validated_data = validated_data.withColumn('is_valid', when(col('error_reason')=='Valid', True).otherwise(False) )

    # buang duplikat
    deduped_data = validated_data.dropDuplicates(['user_id', 'timestamp', 'event_time'])

    # ambil data valid
    valid_data =  deduped_data.filter(col('is_valid')==True)

    # Data Routing (Dead Letter Queue): Mengambil data kotor untuk dikirim ke tempat lain
    invalid_data = validated_data.filter(col('is_valid')==False)

    # menghitung total transaksi per window menggunakan "Tumbling Window" berdurasi 1 menit
    # Event terlambat lebih dari 3 menit dianggap invalid → kirim ke DLQ
    agg_data = deduped_data.groupBy(window(col('event_time'), '1 minute')).agg(count('*').alias('total_transactions'),
         sum('amount').alias('total_amount'))


    running_data = valid_data \
        .agg(count('*').alias('running_total')) \
        .withColumn('timestamp', F.current_timestamp()) \
        .select('timestamp', 'running_total')

    # Data valid -> Kafka topic transactions_valid 
    valid_kafka_df = valid_data.selectExpr("to_json(struct(user_id, CAST(amount AS DOUBLE) AS amount, timestamp, source, is_valid, error_reason)) AS value")
    valid_query = valid_kafka_df \
        .writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers',  'localhost:9092') \
        .option('topic', VALID_OUTPUT_TOPIC) \
        .option('checkpointLocation', 'checkpoints/valid_v2') \
        .outputMode('append') \
        .start()
    
    # Print hasil validasi valid_data ke console 
    console_query = agg_data \
        .writeStream \
        .format('console') \
        .outputMode('update') \
        .option('truncate', 'false') \
        .option('checkpointLocation', 'checkpoints/console_v2') \
        .start()

    # Hasil running total
    # Output ke console harus memiliki kolom: timestamp dan running total
    running_query = running_data \
        .writeStream \
        .format('console') \
        .outputMode('complete') \
        .option('truncate', 'false') \
        .option('checkpointLocation', 'checkpoints/running_v2') \
        .start()

    # Mengirim data yang invalid ke Dead Letter Queue Topic di Kafka
    dlq_kafka_df = invalid_data.selectExpr("to_json(struct(*)) AS value")
    dlq_query = dlq_kafka_df \
        .writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers',  'localhost:9092') \
        .option('topic', DLQ_TOPIC) \
        .option('checkpointLocation', 'checkpoints/dlq_v2') \
        .outputMode('append') \
        .start()

    logging.info("Pipeline berjalan! Menunggu terminasi...")
                                
    # Tunggu semua query berjalan tanpa henti
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()

   
