import subprocess, sys
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (accuracy_score, precision_score, recall_score, 
                             f1_score, confusion_matrix, classification_report, matthews_corrcoef)
from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn.preprocessing import StandardScaler
import numpy as np
import joblib
import boto3
subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow"])
import mlflow
subprocess.check_call([sys.executable, "-m", "pip", "install", "lakefs"])
import lakefs
from lakefs.client import Client
import os
from io import StringIO



lakefs_endpoint = os.environ.get("LAKEFS_ENDPOINT") + "/api/v1"
access_key = os.environ.get("LAKEFS_ACCESS_KEY")
secret_key = os.environ.get("LAKEFS_SECRET_KEY")



clt = Client(
    username=access_key,
    password=secret_key,
    host=lakefs_endpoint,
)


# Initialize an S3 client pointing to lakeFS
s3 = boto3.client(
    "s3",
    endpoint_url=lakefs_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)


model_dir = os.environ.get("SM_MODEL_DIR", "/opt/ml/model")
output_dir = os.environ.get("SM_OUTPUT_DATA_DIR", "/opt/ml/output/data")

def train_model(X_train, y_train, X_test, y_test, feature_names, grid_search=False):
    # Initialize and fit the model
    start_model = RandomForestClassifier(
        verbose=1,
        n_jobs=-1,
        n_estimators=200,
        min_samples_leaf=2, 
        min_samples_split=10,
        max_depth=10,
        max_features=None,
        random_state=42,
        class_weight='balanced'
    )

    if grid_search:
        param_grid = {
            'n_estimators': [50, 100, 200],
            'max_depth': [None, 10, 20],
            'min_samples_split': [2, 5, 10],
            'min_samples_leaf': [1, 2, 4],
            'max_features': ['sqrt', 'log2', None],
        }

        # Searching for the best tree parameters
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

    # Test predictions
    y_pred = best_model.predict(X_test)

    # Evaluate model
    metrics_rf = calculate_performance_metrics(y_test, y_pred)
    return best_model, metrics_rf

# Useful values for classification
def calculate_performance_metrics(y_test, y_pred):
    metrics = {}
    metrics['accuracy'] = accuracy_score(y_test, y_pred)
    metrics['precision'] = precision_score(y_test, y_pred, average='weighted')
    metrics['recall'] = recall_score(y_test, y_pred, average='weighted')
    metrics['f1_score'] = f1_score(y_test, y_pred, average='weighted')
    metrics['confusion_matrix'] = confusion_matrix(y_test, y_pred)
    metrics['mcc'] = matthews_corrcoef(y_test, y_pred)
    metrics['classification_report'] = classification_report(y_test, y_pred)
    
    return metrics


def process_data(): 

    repo = lakefs.Repository("weather-data", client=clt)
    ref  = repo.ref("main") 

    # list & read CSVs (prefix filtering)
    prefixes = ["data/processed/year=2022", "data/processed/year=2023", "data/processed/year=2024"]

    dfs = []
    for p in prefixes:
        for obj in ref.objects(prefix=p):  # iterator of objects
            if obj.path.endswith(".csv"):
                with ref.object(obj.path).reader(mode="r") as f:
                    df = pd.read_csv(f)
                    dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)


    processed_data = pd.concat(dfs, ignore_index=True)
    processed_data = processed_data.drop(columns=["date"])


    # Separate data
    target_name = 'weather_code'
    X = processed_data.drop(columns=[target_name]).values
    y = processed_data[target_name].values

    # feature_names = processed_data.columns[:-1].tolist()
    feature_names = processed_data.drop(columns=[target_name]).columns.tolist()
    return X,y,feature_names
    

# Trains the model
def create_model():
    X,y,feature_names = process_data()

    # Get test and train
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print('Read the data')
    # Run the model
    model, metrics_rf = train_model(X_train, y_train, X_test, y_test, feature_names)

    return model, metrics_rf, X_train


#TODO: Set proper tracking arn and expirement tag
# mlflow.set_tracking_uri("arn:aws:sagemaker:us-east-2:478492276227:mlflow-tracking-server/TrackingServerV1")
# mlflow.set_experiment("some-experiment")


def run_model_training():
    # Train the model
    model, metrics_rf, X_train = create_model()
    return model, metrics_rf, X_train



repo_name = "weather-data"
branch_name = "main"
repo = lakefs.Repository(repository_id=repo_name, client=clt)
branch = repo.branch(branch_name)
commit_id = branch.get_commit().id


model, metrics_rf, X_train = run_model_training()
joblib.dump(model, os.path.join(model_dir, "model.pkl"))


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

output_path = os.path.join(output_dir, "mlflow.json")


# with mlflow.start_run():
#     run_id = mlflow.active_run().info.run_id
#     with open(output_path, "w") as f:
#         json.dump({"mlflow_run_id": run_id}, f)


#     mlflow.log_params(params)
#     mlflow.log_param("lakefs_repo",   repo)
#     mlflow.log_param("lakefs_branch", branch)
#     mlflow.log_param("lakefs_commit_id", commit_id)
#     mlflow.log_metrics(metrics_fr)
#     signature = infer_signature(X_train, model.predict(X_train))


#     model_info = mlflow.sklearn.log_model(
#         sk_model=rf,
#         name="weather_model",
#         signature=signature,
#         input_example=X_train,
#         registered_model_name="weather_model",
#     )

