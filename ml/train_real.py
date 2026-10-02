import os

import mlflow
import numpy as np
import xgboost as xgb
from deltalake import DeltaTable
from sklearn.metrics import (
    average_precision_score, confusion_matrix, f1_score,
    precision_score, recall_score,
)

# 1. Chargement, ordre chronologique (row_id remplace Time)
df = DeltaTable("data/silver/transactions_real").to_pandas()
df = df.sort_values("row_id").reset_index(drop=True)
features = [f"V{i}" for i in range(1, 29)] + ["Amount"]

# 2. Découpage chronologique 80/20
cut = int(len(df) * 0.8)
train, test = df.iloc[:cut], df.iloc[cut:]
X_train, y_train = train[features], train["is_fraud"]
X_test, y_test = test[features], test["is_fraud"]
print(f"Train : {len(train)} lignes, {int(y_train.sum())} fraudes")
print(f"Test  : {len(test)} lignes, {int(y_test.sum())} fraudes")

# 3. Modèle
params = {
    "n_estimators": 300,
    "max_depth": 5,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "scale_pos_weight": float((y_train == 0).sum() / (y_train == 1).sum()),
    "tree_method": "hist",
    "n_jobs": 2,
    "random_state": 42,
}

mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("fraud-detection-real")

with mlflow.start_run():
    model = xgb.XGBClassifier(**params)
    model.fit(X_train, y_train)

    proba = model.predict_proba(X_test)[:, 1]
    pred = (proba >= 0.5).astype(int)
    metrics = {
        "precision": precision_score(y_test, pred),
        "recall": recall_score(y_test, pred),
        "f1": f1_score(y_test, pred),
        "pr_auc": average_precision_score(y_test, proba),
    }
    mlflow.log_params(params)
    mlflow.log_metrics(metrics)

    os.makedirs("models", exist_ok=True)
    model.save_model("models/xgb_real.json")
    mlflow.log_artifact("models/xgb_real.json")

    print("\nMatrice de confusion [[TN, FP], [FN, TP]] :")
    print(confusion_matrix(y_test, pred))
    print("\nMétriques (seuil 0,5) :")
    for k, v in metrics.items():
        print(f"  {k}: {v:.4f}")

    print("\nEffet du seuil de décision :")
    print("  seuil  precision  rappel  alertes")
    for t in [0.1, 0.3, 0.5, 0.7, 0.9]:
        p = (proba >= t).astype(int)
        print(f"  {t:.1f}    {precision_score(y_test, p, zero_division=0):.3f}      "
              f"{recall_score(y_test, p):.3f}   {int(p.sum())}")

    print("\nTop 5 des variables :")
    for name, imp in sorted(zip(features, model.feature_importances_), key=lambda x: -x[1])[:5]:
        print(f"  {name}: {imp:.4f}")