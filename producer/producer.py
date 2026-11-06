import json
from confluent_kafka import Producer
import time
import uuid
import random
import logging
from datetime import datetime, timedelta

# konfigurasi Producer
producer = Producer({"bootstrap.servers": "localhost:9092"})

# konfigurasi logging
logging.basicConfig(
    level=logging.DEBUG, # akan menampilkan semua level logging : DEBUG -> INFO -> WARNING -> ERROR -> CRITICAL
    format= '%(asctime)s | %(levelname)s | %(message)s') # format output: timestamp | DEBUG/INFO/WARNING/ERROR/CRITICAL | message
logger = logging.getLogger(__name__)


# nilai untuk duplikat 
user_id = str(uuid.uuid4())
timestamp = datetime.now().isoformat()
source = random.choice(['mobile', 'web', 'pos'])


# generate event invalid dan late
invalid_data = [
     # invalid event : amount negatif
    {
    'user_id' : str(uuid.uuid4()),
    'amount' : -50000,
    'timestamp' : datetime.now().isoformat(),
    'source' : random.choice(['mobile', 'web', 'pos'])
    }

    # invalid event : source tidak dikenal
    , {
    'user_id' : str(uuid.uuid4()),
    'amount' : 100000,
    'timestamp' : datetime.now().isoformat(),
    'source' : 'store'
    }

    # invalid event : data duplikat
    , {
    'user_id' : user_id,
    'amount' : 300000,
    'timestamp' : timestamp,
    'source' : source
    }
    
    , {
    'user_id' : user_id,
    'amount' : 300000,
    'timestamp' : timestamp,
    'source' : source
    }

    # invalid timestamp
    , {
    'user_id' :str(uuid.uuid4()),
    'amount' : 250000,
    'timestamp' : '14-12-2025 09:00:20',
    'source' : random.choice(['mobile', 'web', 'pos'])
    }


    # late event
    , {
    'user_id' :str(uuid.uuid4()),
    'amount' : 170000,
    'timestamp' : (datetime.now() - timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'source' : random.choice(['mobile', 'web', 'pos'])
    }

    , {
    'user_id' :str(uuid.uuid4()),
    'amount' : 70000,
    'timestamp' : (datetime.now() - timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'source' : random.choice(['mobile', 'web', 'pos'])
    }

    , {
    'user_id' :str(uuid.uuid4()),
    'amount' : 1750000,
    'timestamp' : (datetime.now() - timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'source' : random.choice(['mobile', 'web', 'pos'])
    }
]

def generate_data() :
    """
    Fungsi untuk membuat data event normal
    """
    normal_transactions = {
        'user_id' : str(uuid.uuid4()),
        'amount' : random.randint(1000, 1000000), 
        'timestamp' : datetime.now().isoformat(), 
        'source' : random.choice(['mobile', 'web', 'pos'])
    }

    return  normal_transactions

def delivery_report(err, msg):
    """
    Fungsi callback untuk mengirimkan report
    """
    if err is not None:
        logger.error(f'Delivery failed: {err}')
    else:
        logger.info(f'Delivered to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}')


def main() :
    """
    Fungsi utama untuk menjalankan Kafka Producer
    """
    try:
        logger.info('Producer Kafka dimulai..')


        # mengirimkan invalid data dan late event
        for data in invalid_data :
            producer.produce(
                topic='transactions',
                value=json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )

            producer.poll(0)
            time.sleep(1.5) # mengirimkan event tiap 1-2 detik
        
        # mengirimkan data normal secara terus menerus kecuali dihentikan melalui keyboard
        while True :
            json_playload = generate_data()
            producer.produce(
                topic='transactions',
                value=json.dumps(json_playload).encode('utf-8'),
                callback=delivery_report
            )

            producer.poll(0)
            time.sleep(1.5)


    except KeyboardInterrupt:
        logger.info('Producer dihentikan user')
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
