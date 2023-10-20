import csv
import pandas as pd

df = pd.read_csv(r'C:\Users\MaximilianStoepler\PycharmProjects\Project Data Engineering II IU\data_cleaned_1000.csv', sep=',', header=None)

#remove last column from df
df = df.iloc[:, :-1]
#store back to csv
df.to_csv(r'C:\Users\MaximilianStoepler\PycharmProjects\Project Data Engineering II IU\data_cleaned_1000.csv', index=False, header=False)
print(df)