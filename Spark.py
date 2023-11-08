from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import psycopg2
from sklearn.ensemble import IsolationForest


#config raw data
RAW_DATA_DB_HOST = "raw_data_db"
RAW_DATA_DB_PORT = "5432"
RAW_DATA_DB_NAME = "raw_data"
RAW_DATA_DB_USER = "my_username"
RAW_DATA_DB_PASSWORD = "my_password"

#config cleaned data
CLEANED_DATA_DB_HOST = "cleaned_data_db"
CLEANED_DATA_DB_PORT = "5432"
CLEANED_DATA_DB_NAME = "cleaned_data"
CLEANED_DATA_DB_USER = "my_username"
CLEANED_DATA_DB_PASSWORD = "my_password"

#config spark
SPARK_APP_NAME = "PostgreSQL Connection with PySpark"
JDBC_DRIVER_JAR = "/app/postgres-jdbc-driver.jar"

#config connection to raw data db
raw_data_url = f"jdbc:postgresql://{RAW_DATA_DB_HOST}:{RAW_DATA_DB_PORT}/{RAW_DATA_DB_NAME}"
raw_data_properties = {
    "user": RAW_DATA_DB_USER,
    "password": RAW_DATA_DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}
#config connection to cleaned data db
cleaned_data_url = f"jdbc:postgresql://{CLEANED_DATA_DB_HOST}:{CLEANED_DATA_DB_PORT}/{CLEANED_DATA_DB_NAME}"
cleaned_data_properties = {
    "user": CLEANED_DATA_DB_USER,
    "password": CLEANED_DATA_DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

#tablename cleaned data
CLEANED_DATA_TABLE_NAME = "cleaned_data"

#init spark session
spark = SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .config("spark.jars", JDBC_DRIVER_JAR) \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

#read data from raw db
df = spark.read.jdbc(url=raw_data_url, table=RAW_DATA_DB_NAME, properties=raw_data_properties)

#count raw_data rows
raw_data_rows = df.count()

#rename mapping
column_mapping = {
    "unnamed0": "index",
    "vendorid": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "ratecodeid": "rate_code_id",
    "pulocationid": "pickup_location_id",
    "dolocationid": "dropoff_location_id"
}

#rename df columns
for old_col, new_col in column_mapping.items():
    df = df.withColumnRenamed(old_col, new_col)

columns_and_types = ", ".join([f"{col} text" for col in df.columns])

#error handling
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
    print(f"Verbindung zur PostgreSQL-Datenbank {CLEANED_DATA_DB_NAME} erfolgreich hergestellt.")

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

        #drop all rows with null values
        df = df.na.drop(how='any')

        #drop columns store_and_fwd_flag, congestion_surcharge, airport_fee, improvement_surcharge, extra, mta_tax, tolls_amount, tip_amount
        df = df.drop('store_and_fwd_flag', 'congestion_surcharge', 'airport_fee', 'improvement_surcharge', 'extra', 'mta_tax', 'tolls_amount', 'tip_amount')

        #drop rows where fare_amount is not reasonable, payment_type is not cash or credit card, trip_distance is not reasonable
        df = df.filter((df['fare_amount'] > 0) & (df['fare_amount'] <= 300))
        df = df.filter((df['payment_type'] == 1) | (df['payment_type'] == 2))
        df = df.filter((df['trip_distance'] > 0) & (df['trip_distance'] <= 300))

        #drop duplicate rows from dataframe where picup_datetime, dropoff_datetime, trip_distance, pickup_location_id, dropoff_location_id are the same
        df = df.dropDuplicates(subset=['pickup_datetime', 'dropoff_datetime', 'trip_distance', 'pickup_location_id', 'dropoff_location_id'])

        #add column which calculates the trip duration in minutes and two decimal places
        df = df.withColumn("trip_duration", ((col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60).cast("double"))

        #drop rows where trip_duration in combination with trip_distance and fare_amount is not reasonable
        df = df[~((df['trip_duration'] < 10) & (df['trip_distance'] < 10) & (df['fare_amount'] > 100))]

        # isolations forest model
        X = df.select("fare_amount", "trip_distance", "trip_duration").toPandas()

        model = IsolationForest(contamination=0.05)  # contamination legt den Anteil der Ausreißer in den Daten fest

        #train model
        model.fit(X)

        #predict outliers
        y_pred = model.predict(X)

        #add prediction column to dataframe
        df = df.withColumn("outlier_prediction", lit(y_pred[0]).cast("int"))

        #filter dataframe for outliers
        df = df.filter(col("outlier_prediction") == 1)

        #drop outlier prediction column
        df = df.drop("outlier_prediction")

        clean_data_rows = df.count()

        #amount of outlier rows that have been removed
        print(f'Es wurden {raw_data_rows - clean_data_rows} Ausreißer entfernt.')
        #write cleaned data to postgresql
        df.write.jdbc(url=cleaned_data_url, table=CLEANED_DATA_TABLE_NAME, mode="overwrite",
                    properties=cleaned_data_properties)
        print(f"Schreiben der Daten in die Tabelle {CLEANED_DATA_TABLE_NAME} erfolgreich.")
    except Exception as e:
        print(f"Schreiben der Daten in die Tabelle {CLEANED_DATA_TABLE_NAME} nicht erfolgreich. Fehler: {e}")

except Exception as e:
    print(f"Verbindung zur PostgreSQL-Datenbank {CLEANED_DATA_DB_NAME} nicht erfolgreich hergestellt. Fehler: {e}")




