from pyspark.sql import SparkSession

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

df = spark.read.jdbc(url=url, table="raw_data", properties=properties)

print(df.show())
