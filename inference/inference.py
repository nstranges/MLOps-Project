import os, joblib

def model_fn(model_dir):
    return joblib.load(os.path.join(model_dir, "model.pkl"))