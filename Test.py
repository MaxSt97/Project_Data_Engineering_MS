from kafka import KafkaProducer, KafkaConsumer
from json import loads
import csv
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
import sys

import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        database="raw_data",  # Ändern Sie den Datenbanknamen entsprechend
        user="my_username",
        password="my_password",
        port="5433"
    )
    print("Verbindung zur PostgreSQL-Datenbank erfolgreich hergestellt.")
except Exception as e:
    print("XXX")
    print(e)
    conn = None





if conn is not None:
    # Kafka-Producer-Konfiguration
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Nachrichten senden
    topic = 'test_topic11'

    with open(r'C:\Users\MaximilianStoepler\OneDrive - Deutsche Bahn\Studium\4 Semester\Data Engineering Projekt\Data\data_cleaned_1000.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            message = ','.join(row).encode('utf-8')
            producer.send(topic, value=message)
            print(f'Nachricht gesendet: {message}')

    producer.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: x.decode('utf-8'),
        consumer_timeout_ms=1000
    )

    first_row = next(consumer)
    field_names = first_row.value.split(',')

    cursor = conn.cursor()
    # Erstellen Sie die Tabelle, falls sie noch nicht existiert
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sample_table (
        {', '.join([f"{name} text" for name in field_names])}
    );
    """

    cursor.execute(create_table_query)
    conn.commit()

    # Nachrichten empfangen und in die PostgreSQL-Datenbank einfügen
    for message in consumer:
        message_value = message.value
        message_parts = message_value.split(',')

        # Fügen Sie die Teile der Nachricht in die PostgreSQL-Tabelle ein
        insert_query = "INSERT INTO sample_table VALUES (%s, %s, %s)"  # Passen Sie die Spaltenanzahl an
        cursor.execute(insert_query, message_parts)
        conn.commit()

    # Schließen Sie die Verbindung
    conn.close()
