
from pyspark.sql import SparkSession
import psycopg2

spark = SparkSession.builder \
    .appName("PostgreSQL Connection with PySpark") \
    .config("spark.jars", "/app/postgres-jdbc-driver.jar") \
    .getOrCreate()

url = "jdbc:postgresql://raw_data_db:5432/raw_data"

properties = {
    "user": "my_username",       # Ihr Benutzername
    "password": "my_password",  # Ihr Passwort
    "driver": "org.postgresql.Driver"
}

table_name = "cleaned_data"



#read raw_data table to df
df = spark.read.jdbc(url=url, table="raw_data", properties=properties)

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


header_row = df.columns
print(header_row)

columns_and_types = ", ".join([f"{col} text" for col in header_row])

try:
    conn = psycopg2.connect(
        host="cleaned_data_db",
        database="cleaned_data",
        user="my_username",
        password="my_password",
        port="5432"
    )
    cursor = conn.cursor()
    print("Verbindung zur PostgreSQL-Datenbank erfolgreich hergestellt.")

    create_table_sql = f"CREATE TABLE IF NOT EXISTS cleaned_data ({columns_and_types})"
    cursor.execute(create_table_sql)
    conn.commit()

except:
    print("Verbindung zur PostgreSQL-Datenbank nicht erfolgreich hergestellt.")
#df.show()
