from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import psycopg2
from scipy import stats


# Konfiguration für die "raw_data" Datenbank
RAW_DATA_DB_HOST = "raw_data_db"
RAW_DATA_DB_PORT = "5432"
RAW_DATA_DB_NAME = "raw_data"
RAW_DATA_DB_USER = "my_username"
RAW_DATA_DB_PASSWORD = "my_password"

# Konfiguration für die "cleaned_data" Datenbank
CLEANED_DATA_DB_HOST = "cleaned_data_db"
CLEANED_DATA_DB_PORT = "5432"
CLEANED_DATA_DB_NAME = "cleaned_data"
CLEANED_DATA_DB_USER = "my_username"
CLEANED_DATA_DB_PASSWORD = "my_password"

# Konfiguration für die Spark-Anwendung
SPARK_APP_NAME = "PostgreSQL Connection with PySpark"
JDBC_DRIVER_JAR = "/app/postgres-jdbc-driver.jar"

# Verbindung zur "raw_data" Datenbank herstellen
raw_data_url = f"jdbc:postgresql://{RAW_DATA_DB_HOST}:{RAW_DATA_DB_PORT}/{RAW_DATA_DB_NAME}"
raw_data_properties = {
    "user": RAW_DATA_DB_USER,
    "password": RAW_DATA_DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}
# Verbindung zur "cleaned_data" Datenbank herstellen
cleaned_data_url = f"jdbc:postgresql://{CLEANED_DATA_DB_HOST}:{CLEANED_DATA_DB_PORT}/{CLEANED_DATA_DB_NAME}"
cleaned_data_properties = {
    "user": CLEANED_DATA_DB_USER,
    "password": CLEANED_DATA_DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Tabellennamen
CLEANED_DATA_TABLE_NAME = "cleaned_data"

# SparkSession erstellen
spark = SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .config("spark.jars", JDBC_DRIVER_JAR) \
    .getOrCreate()

# Daten aus der "raw_data"-Tabelle lesen
df = spark.read.jdbc(url=raw_data_url, table=RAW_DATA_DB_NAME, properties=raw_data_properties)

column_mapping = {
    "unnamed0": "index",
    "vendorid": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "ratecodeid": "rate_code_id",
    "pulocationid": "pickup_location_id",
    "dolocationid": "dropoff_location_id"
}

# Die Umbenennungen auf den DataFrame anwenden
for old_col, new_col in column_mapping.items():
    df = df.withColumnRenamed(old_col, new_col)



columns_and_types = ", ".join([f"{col} text" for col in df.columns])

try:
    # Verbindung zur "cleaned_data" Datenbank herstellen
    cleaned_data_conn = psycopg2.connect(
        host=CLEANED_DATA_DB_HOST,
        database=CLEANED_DATA_DB_NAME,
        user=CLEANED_DATA_DB_USER,
        password=CLEANED_DATA_DB_PASSWORD,
        port=CLEANED_DATA_DB_PORT
    )
    cleaned_data_cursor = cleaned_data_conn.cursor()
    print("Verbindung zur PostgreSQL-Datenbank erfolgreich hergestellt.")

    create_table_sql = f"CREATE TABLE IF NOT EXISTS {CLEANED_DATA_TABLE_NAME} ({columns_and_types})"
    cleaned_data_cursor.execute(create_table_sql)
    cleaned_data_conn.commit()

    try:

        conversion_rules = {
            "index": "int",
            "vendor_id": "int",
            "pickup_datetime": "timestamp",
            "dropoff_datetime": "timestamp",
            "passenger_count": "int",
            "trip_distance": "double",
            "rate_code_id": "int",
            "store_and_fwd_flag": "string",
            "pickup_location_id": "int",
            "dropoff_location_id": "int",
            "payment_type": "int",
            "fare_amount": "double",
            "extra": "double",
            "mta_tax": "double",
            "tip_amount": "double",
            "tolls_amount": "double",
            "improvement_surcharge": "double",
            "total_amount": "double",
            "congestion_surcharge": "double",
            "airport_fee": "int"
        }

        df = df.select(*[
            col(column).cast(conversion_rules.get(column, "string")).alias(column)
            for column in df.columns
        ])

        df = df.na.drop(how='any')
        #drop duplicate rows from dataframe where picup_datetime and dropoff_datetime and trip_distance are same
        df = df.dropDuplicates(subset=['pickup_datetime', 'dropoff_datetime', 'trip_distance'])
        #drop rows where trip_distance is 0 because we cant actual distance with pickup and dropoff location
        df = df.filter(df['trip_distance'] != 0)




        #add column which calculates the trip duration in minutes and two decimal places
        df = df.withColumn("trip_duration", ((col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60).cast("decimal(10,2)"))


        df.write.jdbc(url=cleaned_data_url, table=CLEANED_DATA_TABLE_NAME, mode="overwrite",
                    properties=cleaned_data_properties)
        print(f"Schreiben der Daten in die Tabelle {CLEANED_DATA_TABLE_NAME} erfolgreich.")
    except Exception as e:
        print(f"Schreiben der Daten in die Tabelle {CLEANED_DATA_TABLE_NAME} nicht erfolgreich. Fehler: {e}")

except Exception as e:
    print(f"Verbindung zur PostgreSQL-Datenbank nicht erfolgreich hergestellt. Fehler: {e}")




