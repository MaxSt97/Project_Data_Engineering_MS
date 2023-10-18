from kafka import KafkaConsumer


# Kafka-Consumer-Konfiguration

consumer = KafkaConsumer(
    'topicxx',
    bootstrap_servers=['broker:9092'],
    auto_offset_reset='earliest',
    consumer_timeout_ms=2000)
# Das gewünschte Kafka-Topic abonnieren


for message in consumer:
    print(f'Nachricht empfangen: {message.value}')

# Verbindung schließen
consumer.close()
