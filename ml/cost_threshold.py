import numpy as np
import xgboost as xgb
from deltalake import DeltaTable
from sklearn.metrics import precision_score, recall_score

ALERT_COST = 5.0  # coût d'une alerte (vérification), en euros

df = DeltaTable("data/silver/transactions_real").to_pandas()
df = df.sort_values("row_id").reset_index(drop=True)
features = [f"V{i}" for i in range(1, 29)] + ["Amount"]

n = len(df)
a, b = int(n * 0.6), int(n * 0.8)
train, val, test = df.iloc[:a], df.iloc[a:b], df.iloc[b:]
print(f"Train {len(train)} ({int(train.is_fraud.sum())} fraudes) | "
      f"Validation {len(val)} ({int(val.is_fraud.sum())}) | "
      f"Test {len(test)} ({int(test.is_fraud.sum())})")

model = xgb.XGBClassifier(
    n_estimators=300, max_depth=5, learning_rate=0.05, subsample=0.8,
    colsample_bytree=0.8, tree_method="hist", n_jobs=2, random_state=42,
    scale_pos_weight=float((train.is_fraud == 0).sum() / (train.is_fraud == 1).sum()),
)
model.fit(train[features], train.is_fraud)


def total_cost(d, proba, t):
    flag = proba >= t
    missed = d.loc[(~flag) & (d.is_fraud == 1), "Amount"].sum()
    return ALERT_COST * flag.sum() + missed


p_val = model.predict_proba(val[features])[:, 1]
p_test = model.predict_proba(test[features])[:, 1]

# Le seuil est choisi sur la VALIDATION uniquement
grid = np.round(np.arange(0.02, 1.0, 0.02), 2)
best_t = min(grid, key=lambda t: total_cost(val, p_val, t))
print(f"\nSeuil optimal (choisi sur la validation) : {best_t}")

baseline = test.loc[test.is_fraud == 1, "Amount"].sum()
print(f"\nCoût sur le TEST (les montants des fraudes manquées + {ALERT_COST} par alerte) :")
print(f"  Sans modèle (aucune alerte)  : {baseline:10.2f}")
for label, t in [("Seuil 0,5", 0.5), (f"Seuil optimal {best_t}", best_t)]:
    flag = (p_test >= t).astype(int)
    c = total_cost(test, p_test, t)
    print(f"  {label:<28}: {c:10.2f}  | précision {precision_score(test.is_fraud, flag):.3f}"
          f"  rappel {recall_score(test.is_fraud, flag):.3f}  alertes {int(flag.sum())}")