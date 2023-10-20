
from kafka import KafkaProducer
import csv
# Kafka-Producer-Konfiguration
producer = KafkaProducer(bootstrap_servers='broker:9092')

#Datei einlesen und an die Kafka-Topic senden
with open('data_cleaned_1000.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        message = ','.join(row).encode('utf-8')
        producer.send('topicxx', value=message)
        print(f'Nachricht gesendet: {message}')

# Producer-Verbindung beenden
producer.close()

