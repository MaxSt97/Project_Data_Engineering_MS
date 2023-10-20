from kafka import KafkaConsumer
import psycopg2

# Kafka-Consumer-Konfiguration
consumer = KafkaConsumer(
    'topicxx',
    bootstrap_servers=['broker:9092'],
    auto_offset_reset='earliest',
    consumer_timeout_ms=2000)


table_name = "raw_data"


# Header-Reihe aus der ersten Kafka-Nachricht lesen
header_row = next(consumer)
header_row = header_row.value.decode('utf-8').split(',')

try:
    conn = psycopg2.connect(
        host="raw_data_db",
        database="raw_data",
        user="my_username",
        password="my_password",
        port="5432"
    )
    cursor = conn.cursor()
    print("Verbindung zur PostgreSQL-Datenbank erfolgreich hergestellt.")



    columns_and_types = ", ".join([f"{col} text" for col in header_row])

    # Erstellen Sie die Tabelle mit den Spalten und Datentypen
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_and_types})"
    cursor.execute(create_table_sql)
    conn.commit()




    for message in consumer:
        # Nachrichtenwerte in eine Liste umwandeln
        data_values = message.value.decode('utf-8').split(',')

        # Erstellen Sie die INSERT INTO-Anweisung dynamisch
        insert_query = f"INSERT INTO raw_data ({', '.join(header_row)}) VALUES ({', '.join(['%s'] * len(header_row))})"

        # Führen Sie den INSERT-Befehl aus
        cursor.execute(insert_query, data_values)
        conn.commit()

    print("Tabelle erfolgreich erstellt und Daten eingefügt.")

except Exception as error:
    print(f"Fehler bei der Verbindung zur Datenbank oder beim Erstellen der Tabelle: {error}")
finally:
    # Verbindung und Cursor schließen
    if cursor:
        cursor.close()
    if conn:
        conn.close()

# Verbindung schließen
consumer.close()
