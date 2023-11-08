import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import matplotlib.ticker as mtick

# config clean data
CLEANED_DATA_DB_HOST = "cleaned_data_db"
CLEANED_DATA_DB_PORT = "5432"
CLEANED_DATA_DB_NAME = "cleaned_data"
CLEANED_DATA_DB_USER = "my_username"
CLEANED_DATA_DB_PASSWORD = "my_password"

# create engine for cleaned data db
db_uri = f"postgresql://{CLEANED_DATA_DB_USER}:{CLEANED_DATA_DB_PASSWORD}@{CLEANED_DATA_DB_HOST}:{CLEANED_DATA_DB_PORT}/{CLEANED_DATA_DB_NAME}"
engine = create_engine(db_uri)

# read data from cleaned db
sql_query = "SELECT * FROM cleaned_data"
cleaned_data_df = pd.read_sql_query(sql_query, engine)

# Plot 1: Umsatz nach VendorID
vendor_umsatz = cleaned_data_df.groupby('vendor_id')['total_amount'].sum()
vendor_ids = vendor_umsatz.index
umsatz_values = vendor_umsatz.values

plt.figure(figsize=(8, 6))
plt.bar(vendor_ids, umsatz_values)
plt.xlabel('VendorID')
plt.ylabel('Revenue')
plt.title('Revenue per VendorID')
plt.xticks(vendor_ids)
plt.gca().xaxis.set_major_formatter(mtick.FormatStrFormatter('%d'))
plt.savefig('Umsatz_nach_VendorID.png')
plt.clf()

# Plot 2: Distribution rides per hour
fahrten_pro_stunde = cleaned_data_df.groupby(cleaned_data_df['pickup_datetime'].dt.hour)['index'].count()

plt.figure(figsize=(8, 6))
plt.bar(fahrten_pro_stunde.index, fahrten_pro_stunde.values)
plt.xlabel('Hour')
plt.ylabel('Rides')
plt.title('Rides per Hour')
plt.xticks(range(24))
plt.grid()
plt.savefig('Fahrten_pro_Stunde.png')
plt.clf()

# Plot 3: 2D Scatter Plot (Trip Distance vs. Fare Amount)
x = cleaned_data_df['trip_distance']
y = cleaned_data_df['fare_amount']

plt.figure(figsize=(8, 6))
plt.scatter(x, y, c='b', marker='o')
plt.xlim(0, 200)
plt.ylim(0, 300)
plt.xlabel('Trip Distance')
plt.ylabel('Fare Amount')
plt.title('Trip Distance vs. Fare Amount')
plt.savefig('Trip_Distance_vs_Fare_Amount.png')
plt.clf()

# Plot 4: 2D Scatter Plot (Trip Distance vs. Fare Amount with Color-coded Trip Duration)
trip_duration = cleaned_data_df['trip_duration']
colors = trip_duration

plt.figure(figsize=(8, 6))
plt.scatter(x, y, c=colors, cmap='viridis', norm=plt.Normalize(vmin=0, vmax=120), marker='o')
cbar = plt.colorbar()
cbar.set_label('Trip Duration')
plt.xlim(0, 200)
plt.ylim(0, 300)
plt.xlabel('Trip Distance')
plt.ylabel('Fare Amount')
plt.title('Trip Distance vs. Fare Amount (Color-coded by Trip Duration)')
plt.savefig('Trip_Distance_vs_Fare_Amount_Color-coded_by_Trip_Duration.png')









