from kafka import KafkaConsumer
import psycopg2

#config raw data
RAW_DATA_DB_HOST = "raw_data_db"
RAW_DATA_DB_PORT = "5432"
RAW_DATA_DB_NAME = "raw_data"
RAW_DATA_DB_USER = "my_username"
RAW_DATA_DB_PASSWORD = "my_password"

#config consumer
consumer = KafkaConsumer(
    'batch_pipeline_iu',
    bootstrap_servers=['broker:9092'],
    auto_offset_reset='earliest',
    consumer_timeout_ms=2000
)

table_name = "raw_data"

#read header row
header_row = next(consumer)
header_row = header_row.value.decode('utf-8').split(',')

try:
    #connect to raw data db
    raw_data_conn = psycopg2.connect(
        host=RAW_DATA_DB_HOST,
        database=RAW_DATA_DB_NAME,
        user=RAW_DATA_DB_USER,
        password=RAW_DATA_DB_PASSWORD,
        port=RAW_DATA_DB_PORT
    )
    raw_data_cursor = raw_data_conn.cursor()
    print(f"Verbindung zur PostgreSQL-Datenbank {RAW_DATA_DB_NAME} erfolgreich hergestellt.")

    columns_and_types = ", ".join([f"{col} text" for col in header_row])

    #create table
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_and_types})"
    raw_data_cursor.execute(create_table_sql)
    raw_data_conn.commit()

    for message in consumer:

        data_values = message.value.decode('utf-8').split(',')

        insert_query = f"INSERT INTO {table_name} ({', '.join(header_row)}) VALUES ({', '.join(['%s'] * len(header_row))})"

        raw_data_cursor.execute(insert_query, data_values)
        raw_data_conn.commit()

    print(f"Tabelle {table_name} erfolgreich erstellt und Daten eingefügt.")

except Exception as error:
    print(f"Fehler bei der Verbindung zur Datenbank {RAW_DATA_DB_NAME} oder beim Erstellen der Tabelle: {error}")
finally:
    #close connection
    if raw_data_cursor:
        raw_data_cursor.close()
    if raw_data_conn:
        raw_data_conn.close()

#close consumer
consumer.close()

