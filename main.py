from kafka import KafkaProducer, KafkaConsumer
from json import loads
import confluent_kafka
import csv
from pymongo import MongoClient


# Kafka-Producer-Konfiguration
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Nachrichten senden
topic = 'test_topic11'

# MongoDB-Verbindung konfigurieren
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['sample_db']
mongo_collection = mongo_db['sample_collection']



with open(r'C:\Users\MaximilianStoepler\OneDrive - Deutsche Bahn\Studium\4 Semester\Data Engineering Projekt\Data\data_cleaned_1000.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        message = ','.join(row).encode('utf-8')
        producer.send('topic', value=message)
        print(f'Nachricht gesendet: {message}')

producer.close()

consumer = KafkaConsumer(
    'topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8'),
    consumer_timeout_ms=1000
)

first_row= next(consumer)
field_names = first_row.value.split(',')


# Nachrichten empfangen und ausgeben
for message in consumer:
    message_value = message.value
    # Verarbeiten Sie die CSV-Nachricht, z.B. durch Aufteilen in ein List
    message_parts = message_value.split(',')

    # Hier können Sie die Teile der Nachricht weiterverarbeiten, wie gewünscht
    print(f'Empfangene Nachricht: {message_parts}')

    mongo_collection.insert_one({'message': message_parts})

print(field_names)
# Löschen Sie den gesamten Inhalt der Collection
#result = mongo_collection.delete_many({})