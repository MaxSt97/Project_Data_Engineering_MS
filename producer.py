from kafka import KafkaProducer
import csv

#config
KAFKA_BROKER = 'broker:9092'
CSV_FILE_PATH = 'nyc_yellow_taxi_trip_records.csv'
KAFKA_TOPIC = 'batch_pipeline_iu'

#producer config
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

#read data and send to topic
try:
    with open(CSV_FILE_PATH, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            message = ','.join(row).encode('utf-8')
            producer.send(KAFKA_TOPIC, value=message)
            print(f'Nachricht gesendet: {message}')
except FileNotFoundError:
    print(f'Die Datei {CSV_FILE_PATH} wurde nicht gefunden.')

#close producer
producer.close()

