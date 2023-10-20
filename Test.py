import psycopg2


try:

    conn = psycopg2.connect(
        host="localhost",
        database="raw_data",
        user="my_username",
        password="my_password",
        port="5432"

)
except Exception as error:
    print(f"Fehler bei der Verbindung zur Datenbank oder beim Erstellen der Tabelle: {error}")