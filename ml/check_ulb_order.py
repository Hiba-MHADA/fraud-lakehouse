import pandas as pd

df = pd.read_csv("data/raw/creditcard.csv")
print(df[["V1", "V2", "Amount", "Class"]].head(5))
print()
print(df["Amount"].describe())
print()
# Les fraudes sont-elles réparties sur tout le fichier, ou regroupées ?
fraud_pos = df.index[df["Class"] == 1]
print("Position de la 1re fraude :", fraud_pos.min())
print("Position de la dernière fraude :", fraud_pos.max())
print("Fraudes dans la 1re moitié :", (fraud_pos < len(df) // 2).sum())
print("Fraudes dans la 2e moitié :", (fraud_pos >= len(df) // 2).sum())