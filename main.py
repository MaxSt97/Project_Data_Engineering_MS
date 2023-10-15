from kafka import KafkaProducer, KafkaConsumer
from json import loads
import confluent_kafka
import csv
x

# Kafka-Producer-Konfiguration
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Nachrichten senden
topic = 'test_topic11'


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

# Nachrichten empfangen und ausgeben
for message in consumer:
    message_value = message.value
    # Verarbeiten Sie die CSV-Nachricht, z.B. durch Aufteilen in ein List
    message_parts = message_value.split(',')

    # Hier können Sie die Teile der Nachricht weiterverarbeiten, wie gewünscht
    print(f'Empfangene Nachricht: {message_parts}')