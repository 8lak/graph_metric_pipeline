# 04_run_batch_prediction.py
import os
from google.cloud import aiplatform
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
PROJECT_ID = os.getenv("PROJECT_ID") # Make SURE this is the right one!
REGION = os.getenv("REGION")
CURRENT_LANGUAGE_REPO = os.getenv("CURRENT_LANGUAGE_REPO")
# Find this in the Vertex AI Model Registry page in the console
TUNED_MODEL_ID = f"FINETUNED_RESOURCENAME/{CURRENT_LANGUAGE_REPO}" # This is the NUMERICAL ID of your model
# The GCS path to your big input file from Phase 1



GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BLOB_BATCHING_TO_CLASSIFY_DESTINATION = os.getenv("BLOB_BATCHING_TO_CLASSIFY_DESTINATION")
FULL_COMMIT_DATA_RETRIEVAL_URI = f"gs://{GCS_BUCKET_NAME}/{BLOB_BATCHING_TO_CLASSIFY_DESTINATION}"
FULL_COMMIT_DATA_RESULTS = os.getenv("BLOB_BATCHING_RESULTS")
# The GCS path where you want the results to be saved
GCS_OUTPUT_URI_PREFIX = f"gs://{GCS_BUCKET_NAME}/{FULL_COMMIT_DATA_RESULTS}"

def launch_batch_prediction_job():
    aiplatform.init(project=PROJECT_ID, location=REGION)

    try:
        with open(TUNED_MODEL_ID, 'r') as f:
            model_resource_name = f.read().strip()
            print(f"Read model resource name from file: {model_resource_name}")
    except FileNotFoundError:
        print(f"Error: Model resource name file not found at {TUNED_MODEL_ID}")
        return
    model = aiplatform.Model(model_name=model_resource_name)

    print("Launching batch prediction job...")
    
    # Launch the job. This is the key function.
    batch_prediction_job = model.batch_predict(
        job_display_name="libxml2_full_classification",
        gcs_source=FULL_COMMIT_DATA_RETRIEVAL_URI,
        gcs_destination_prefix=GCS_OUTPUT_URI_PREFIX,
        sync=False # Make it asynchronous
    )

    print(f"Job launched. View its progress in the Vertex AI console.")
    print(f"Job Name: {batch_prediction_job.resource_name}")

if __name__ == "__main__":
    launch_batch_prediction_job()