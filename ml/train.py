import mlflow
import pandas as pd
import xgboost as xgb
from deltalake import DeltaTable
from sklearn.metrics import (
    average_precision_score, confusion_matrix, f1_score,
    precision_score, recall_score,
)

# 1. Chargement, tri chronologique
df = DeltaTable("data/gold/features").to_pandas()
df = df.sort_values("event_time").reset_index(drop=True)
df = pd.get_dummies(df, columns=["merchant_category"], dtype=int)

features = ["amount", "amount_ratio", "tx_1h", "is_foreign"] + [
    c for c in df.columns if c.startswith("merchant_category_")
]

# 2. Découpage chronologique : 80 % du passé pour entraîner, 20 % du futur pour tester
cut = int(len(df) * 0.8)
train, test = df.iloc[:cut], df.iloc[cut:]
X_train, y_train = train[features], train["is_fraud"]
X_test, y_test = test[features], test["is_fraud"]
print(f"Train : {len(train)} lignes, {y_train.sum()} fraudes")
print(f"Test  : {len(test)} lignes, {y_test.sum()} fraudes")

# 3. Modèle, avec pondération de la classe rare
params = {
    "n_estimators": 200,
    "max_depth": 4,
    "learning_rate": 0.1,
    "scale_pos_weight": float((y_train == 0).sum() / (y_train == 1).sum()),
    "tree_method": "hist",
    "n_jobs": 2,
    "random_state": 42,
}

mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("fraud-detection")

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

    import os
    os.makedirs("models", exist_ok=True)
    model.save_model("models/xgb_fraud.json")
    mlflow.log_artifact("models/xgb_fraud.json")

    print("\nMatrice de confusion [[TN, FP], [FN, TP]] :")
    print(confusion_matrix(y_test, pred))
    print("\nMétriques :")
    for k, v in metrics.items():
        print(f"  {k}: {v:.4f}")
    print("\nImportance des features :")
    for name, imp in sorted(zip(features, model.feature_importances_), key=lambda x: -x[1]):
        print(f"  {name}: {imp:.4f}")