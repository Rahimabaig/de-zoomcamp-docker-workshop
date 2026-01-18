import sys 
import pandas as pd

print("arguments",sys.argv)
month = int(sys.argv[1])
print(f"Running pipeline for month {month}")

df = pd.DataFrame({"day": ["monday", "tuesday"], "num_passenger": [3, 4]})
df["month"] = month
print(df.head())

df.to_parquet(f"output{month}.parquet")

