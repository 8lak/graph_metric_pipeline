import os
import vertexai
from vertexai.tuning import sft
from vertexai.generative_models import GenerativeModel
from dotenv import load_dotenv
import json
import time

load_dotenv()

# --- Configuration ---
CURRENT_LANGUAGE_REPO = os.getenv("CURRENT_LANGUAGE_REPO")
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BLOB_TRAINING_DESTINATION = os.getenv("BLOB_TRAINING_DESTINATION")
TRAINING_DATA_URI = f"gs://{GCS_BUCKET_NAME}/{BLOB_TRAINING_DESTINATION}" # From previous script
endpoint_file = f"FINETUNED_RESOURCENAME/{CURRENT_LANGUAGE_REPO}" # From previous script
BASE_MODEL = "gemini-2.5-flash" # Check documentation for latest tunable models
TUNED_MODEL_DISPLAY_NAME = "libxml2_classifier_with_diffs_v2"
ALL_CATEGORIES = [
    "Parser Logic", "Memory", "General Logic Error", "API Logic",
    "Security Vulnerability (CVE)", "Integer", "Error Handling",
    "Concurrency", "Type System", "State Management", "Performance",
    "Incorrect Output/Calculation", "Standard Library Misuse",
    "Build/CI/Tests", "Refactoring", "Documentation",
    "Feature/Enhancement", "Non-Maintenance"
]

def fine_tune_and_use_model():
    """
    Orchestrates the fine-tuning process and subsequent use of the tuned model.
    """
    # 1. Initialize Vertex AI
    # This must be done before any Vertex AI operations
    vertexai.init(project=PROJECT_ID, location=REGION)

    print(f"Starting fine-tuning job for model: {TUNED_MODEL_DISPLAY_NAME}...")

    # 2. Start the asynchronous fine-tuning job
    # This function call returns immediately with a job object.
  
    tuning_job = sft.train(
        source_model=BASE_MODEL,
        tuned_model_display_name=TUNED_MODEL_DISPLAY_NAME,
        train_dataset=TRAINING_DATA_URI

    )
    print(f"Fine-tuning job started. Job name: {tuning_job.tuned_model_name}")

    # 3. Wait for the tuning job to complete
    print("Waiting for tuning job to complete...")
    while not tuning_job.has_ended:
        time.sleep(60)
        tuning_job.refresh()

    tuned_resource_name = tuning_job.tuned_model_name
    print(f"Fine-tuning job succeeded. Tuned model endpoint: {tuned_resource_name}")

    # Always save the latest successful endpoint name, overwriting the old one.
    # This ensures your prediction script always uses the newest model.
    print(f"Saving endpoint name to: {endpoint_file}")
    # Ensure the directory exists
    os.makedirs(os.path.dirname(endpoint_file), exist_ok=True)
    with open(endpoint_file, 'w') as f:
        f.write(tuned_resource_name)

    print(f"Loading tuned model for '{CURRENT_LANGUAGE_REPO}' from endpoint: {tuned_resource_name}")

    


if __name__ == "__main__":
    fine_tune_and_use_model()