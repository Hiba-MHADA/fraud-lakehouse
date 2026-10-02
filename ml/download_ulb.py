import os

from sklearn.datasets import fetch_openml

d = fetch_openml(data_id=1597, as_frame=True, parser="auto")
df = d.frame
df["Class"] = df["Class"].astype(int)

os.makedirs("data/raw", exist_ok=True)
df.to_csv("data/raw/creditcard.csv", index=False)

print(df.shape)
print(df["Class"].value_counts())
print(list(df.columns))