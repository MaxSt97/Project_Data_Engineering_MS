from kafka import KafkaConsumer


# Kafka-Consumer-Konfiguration
consumer = KafkaConsumer(bootstrap_servers='broker:9092')

# Das gewünschte Kafka-Topic abonnieren
consumer.subscribe(['topicxx'])
print(consumer)
for message in consumer:
    print(f'Nachricht empfangen: {message.value}')

# Verbindung schließen
consumer.close()
