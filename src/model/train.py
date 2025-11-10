import os
import json
import boto3
import joblib
import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "lakefs"])

import mlflow
import lakefs
import pandas as pd
from lakefs.client import Client
from lakefs.repository import Repository
from mlflow.models.signature import infer_signature
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, 
    f1_score, confusion_matrix, classification_report,
    matthews_corrcoef
)

subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow"])
subprocess.check_call([sys.executable, "-m", "pip", "install", "lakefs"])

# lakeFS configuration
lakefs_endpoint = os.environ.get("LAKEFS_ENDPOINT") + "/api/v1"
access_key = os.environ.get("LAKEFS_ACCESS_KEY")
secret_key = os.environ.get("LAKEFS_SECRET_KEY")
clt = Client(
    username=access_key,
    password=secret_key,
    host=lakefs_endpoint,
)

# S3 setup for lakeFS
s3 = boto3.client(
    "s3",
    endpoint_url=lakefs_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# Directories for model and output
model_dir = os.environ.get("SM_MODEL_DIR", "/opt/ml/model")
output_dir = os.environ.get("SM_OUTPUT_DATA_DIR", "/opt/ml/output/data")

def load_data(repo: Repository, branch: str = "main"): 
    ref = repo.ref(branch)
    # list & read CSVs (prefix filtering)
    prefixes = ["data/processed/year=2022", "data/processed/year=2023", "data/processed/year=2024"]
    dfs = []
    for p in prefixes:
        for obj in ref.objects(prefix=p):  # iterator of objects
            if obj.path.endswith(".csv"):
                with ref.object(obj.path).reader(mode="r") as f:
                    df = pd.read_csv(f)
                    dfs.append(df)
    processed_data = pd.concat(dfs, ignore_index=True)
    processed_data = processed_data.drop(columns=["date"])
    # Separate data
    target_name = 'weather_code'
    X = processed_data.drop(columns=[target_name]).values
    y = processed_data[target_name].values
    return X, y

def fit_model(X_train, y_train, params, grid_search=False):
    start_model = RandomForestClassifier(**params)
    if grid_search:
        param_grid = {
            'n_estimators': [50, 100, 200],
            'max_depth': [None, 10, 20],
            'min_samples_split': [2, 5, 10],
            'min_samples_leaf': [1, 2, 4],
            'max_features': ['sqrt', 'log2', None],
        }
        grid_search = GridSearchCV(
            start_model,
            param_grid,
            n_jobs=-1
        )
        grid_search.fit(X_train, y_train)
        best_model = grid_search.best_estimator_
        best_params = grid_search.best_params_
        print("best_params_ ----> Random Forest:", best_params)
        print("best_rf_model: ", best_model)
    else:
        best_model = start_model
        best_model.fit(X_train, y_train)
    return best_model

# Useful values for classification
def calculate_performance_metrics(y_test, y_pred):
    return {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, average='weighted'),
        "recall": recall_score(y_test, y_pred, average='weighted'),
        "f1_score": f1_score(y_test, y_pred, average='weighted'),
        "confusion_matrix": confusion_matrix(y_test, y_pred).tolist(),
        "mcc": matthews_corrcoef(y_test, y_pred),
        "classification_report": classification_report(y_test, y_pred, output_dict=True)
    }
    
#TODO: Set proper tracking arn and expirement tag
# mlflow.set_tracking_uri("arn:aws:sagemaker:us-east-2:478492276227:mlflow-tracking-server/TrackingServerV1")
# mlflow.set_experiment("some-experiment")

if __name__ == "__main__":

    params = {
        "verbose": 1,
        "n_jobs": -1,
        "n_estimators": 200,
        "min_samples_leaf": 2,
        "min_samples_split": 10,
        "max_depth": 10,
        "max_features": None,
        "random_state": 42,
        "class_weight": "balanced"
    }
    
    repo_name = "weather-data"
    branch_name = "main"
    repo = lakefs.Repository(repository_id=repo_name, client=clt)
    branch = repo.branch(branch_name)
    commit_id = branch.get_commit().id

    # load data
    X, y = load_data(repo, branch_name)
    # Get train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    # Fit model
    model = fit_model(X_train, y_train, params, grid_search=False)
    # Test predictions
    y_pred = model.predict(X_test)
    # Evaluate model
    metrics_rf = calculate_performance_metrics(y_test, y_pred)
    
    # Save model and accuracy
    joblib.dump(model, os.path.join(model_dir, "model.pkl"))
    output_path = os.path.join(output_dir, "accuracy.json")
    with open(output_path, "w") as f:
        json.dump({"accuracy": metrics_rf["accuracy"]}, f)

    # with mlflow.start_run():
    #     run_id = mlflow.active_run().info.run_id
    #     with open(output_path, "w") as f:
    #         json.dump({"mlflow_run_id": run_id}, f)

    #     mlflow.log_params(params)
    #     mlflow.log_param("lakefs_repo",   repo)
    #     mlflow.log_param("lakefs_branch", branch)
    #     mlflow.log_param("lakefs_commit_id", commit_id)
    #     mlflow.log_metrics(metrics_rf)
    #     signature = infer_signature(X_train, model.predict(X_train))
    #     model_info = mlflow.sklearn.log_model(
    #         sk_model=model,
    #         name="weather_model",
    #         signature=signature,
    #         input_example=X_train,
    #         registered_model_name="weather_model"
    #     )

