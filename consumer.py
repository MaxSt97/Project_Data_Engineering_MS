
from kafka import KafkaConsumer
import psycopg2

# Kafka-Consumer-Konfiguration

consumer = KafkaConsumer(
    'topicxx',
    bootstrap_servers=['broker:9092'],
    auto_offset_reset='earliest',
    consumer_timeout_ms=2000)
# Das gewünschte Kafka-Topic abonnieren

# Kafka-Consumer-Konfiguration

consumer = KafkaConsumer(
    'topicxx',
    bootstrap_servers=['broker:9092'],
    auto_offset_reset='earliest',
    consumer_timeout_ms=2000)
# Das gewünschte Kafka-Topic abonnieren




header_row = next(consumer)
header_row = header_row.value.decode('utf-8').split(',')

try:
    conn = psycopg2.connect(
        database="raw_data",
        user="my_username",
        password="my_password",
        host="localhost",  # Der Hostname oder die IP-Adresse Ihrer PostgreSQL-Instanz
        port="5433"  # Der Port, auf dem PostgreSQL läuft (entsprechend Ihrem Docker-Container)
    )
    cursor = conn.cursor()

    # Erstellen Sie eine neue Tabelle mit den Überschriften aus header_row
    create_table_query = f"CREATE TABLE IF NOT EXISTS raw_data ({', '.join(header_row)} VARCHAR)"
    cursor.execute(create_table_query)
    conn.commit()


except Exception as error:
    print(f"Fehler bei der Verbindung zur Datenbank: {error}")
finally:
    # Verbindung und Cursor schließen
    if cursor:
        cursor.close()
    if conn:
        conn.close()

for message in consumer:
    data = message.value.decode('utf-8')
    values = data.split(',')



# Verbindung schließen
consumer.close()
