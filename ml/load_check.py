import pandas as pd
from deltalake import DeltaTable

df = DeltaTable("data/gold/features").to_pandas()
print(df.shape)
print(df.dtypes)
print(df["is_fraud"].value_counts())
print(df["event_time"].min(), df["event_time"].max())