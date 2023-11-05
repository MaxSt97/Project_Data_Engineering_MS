import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import matplotlib.ticker as mtick
import numpy as np


# Konfiguration für die "cleaned_data" Datenbank
CLEANED_DATA_DB_HOST = "cleaned_data_db"
CLEANED_DATA_DB_PORT = "5432"
CLEANED_DATA_DB_NAME = "cleaned_data"
CLEANED_DATA_DB_USER = "my_username"
CLEANED_DATA_DB_PASSWORD = "my_password"

# Erstelle eine SQLAlchemy-Verbindung zur Datenbank
db_uri = f"postgresql://{CLEANED_DATA_DB_USER}:{CLEANED_DATA_DB_PASSWORD}@{CLEANED_DATA_DB_HOST}:{CLEANED_DATA_DB_PORT}/{CLEANED_DATA_DB_NAME}"
engine = create_engine(db_uri)

# Daten aus der "cleaned_data"-Tabelle lesen
sql_query = "SELECT * FROM cleaned_data"
cleaned_data_df = pd.read_sql_query(sql_query, engine)


##Plot 1: Umsatz nach VendorID

vendor_umsatz = cleaned_data_df.groupby('vendor_id')['total_amount'].sum()

# Balkendiagramm
vendor_ids = vendor_umsatz.index
umsatz_values = vendor_umsatz.values

# Balkendiagramm
plt.bar(vendor_ids, umsatz_values)
plt.xlabel('VendorID')
plt.ylabel('Umsatz')
plt.title('Umsatz nach VendorID')

# Tick-Formate auf der x-Achse als Ganzzahlen festlegen
plt.xticks(vendor_ids)
plt.gca().xaxis.set_major_formatter(mtick.FormatStrFormatter('%d'))

plt.savefig('Umsatz_nach_VendorID.png')

plt.clf()

##Plot 2: Verteilung von Fahrstrecken nach Tageszeit

"""cleaned_data_df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
cleaned_data_df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime')"""

# Gruppierung nach Stunde
fahrten_pro_stunde = cleaned_data_df.groupby(cleaned_data_df['pickup_datetime'].dt.hour)['index'].count()

# Zeitreihenplot
plt.bar(fahrten_pro_stunde.index, fahrten_pro_stunde.values)
plt.xlabel('Stunde des Tages')
plt.ylabel('Anzahl der Fahrten')
plt.title('Fahrten pro Stunde')
plt.xticks(range(24))
plt.grid()

plt.savefig('Fahrten_pro_Stunde.png')

plt.clf()

##Plot 3: 2D Scatter Plot
#trip distance vs. total amount

# Daten für den Scatter Plot
x = cleaned_data_df['trip_distance']
y = cleaned_data_df['fare_amount']

# Scatter Plot erstellen
plt.scatter(x, y, c='b', marker='o')

# Achsenbeschriftungen
plt.xlabel('Trip Distance')
plt.ylabel('Fare Amount')

# Titel
plt.title('2D Scatter Plot: Trip Distance vs. Total Amount')

plt.savefig('2D_Scatter_Plot.png')

plt.clf()
######



x = cleaned_data_df['trip_distance']
y = cleaned_data_df['fare_amount']
trip_duration = cleaned_data_df['trip_duration']

# Die Farbe der Punkte basierend auf der Trip-Dauer codieren
colors = trip_duration

plt.scatter(x, y, c=colors, cmap='viridis', marker='o')
cbar = plt.colorbar()
cbar.set_label('Trip Duration')

plt.xlabel('Trip Distance')
plt.ylabel('Fare Amount')

plt.title('2D Scatter Plot: Trip Distance vs. Total Amount (Color-coded by Trip Duration)')

plt.savefig('2D_Scatter_Plot2.png')









