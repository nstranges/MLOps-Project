from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (accuracy_score, precision_score, recall_score, 
                             f1_score, confusion_matrix, classification_report, matthews_corrcoef)
from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn.preprocessing import StandardScaler
import numpy as np
import joblib
import boto3
import subprocess
import sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow"])
import mlflow
import os


train_dir = os.environ.get("SM_CHANNEL_TRAIN", "/opt/ml/input/data/train")
model_dir = os.environ.get("SM_MODEL_DIR", "/opt/ml/model")

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
    data_name = ''
    train_path = os.path.join(train_dir, "final.csv")
    processed_data = df = pd.read_csv(train_path)
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

s3 = boto3.client("s3")
def run_model_training():
    # Train the model
    model, metrics_rf, X_train = create_model()
    return model, metrics_rf, X_train

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

# with mlflow.start_run():
#     mlflow.log_params(params)
#     mlflow.log_metrics(metrics_fr)
#     signature = infer_signature(X_train, model.predict(X_train))

#     model_info = mlflow.sklearn.log_model(
#         sk_model=rf,
#         name="weather_model",
#         signature=signature,
#         input_example=X_train,
#         registered_model_name="weather_model",
#     )

