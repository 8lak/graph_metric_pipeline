import os
import json
import pandas as pd
from tqdm import tqdm
from google.cloud import storage
from dotenv import load_dotenv
import re

# --- Configuration ---
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# This should match the output prefix from your batch script.
# Make sure it ends with a slash '/'.
GCS_RESULTS_PATH = os.getenv("BLOB_BATCHING_RESULTS") 
LOCAL_DOWNLOAD_PATH = "CLASSIFED_FULL_JSONL/c_libxml2_batching_results_prediction-libxml2_classifier_with_diffs_v2-2025-11-04T04_15_46.422719Z_predictions.jsonl"
CURRENT_LANGUAGE_REPO= os.getenv("CURRENT_LANGUAGE_REPO")

# Input/Output for the final merge
FULL_METADATA_CSV = "full_commit_with_author_data/full_commit_libxml2.csv"
FINAL_CLASSIFIED_CSV = f"FINAL_CLASSIFIED_FULL_DATA/{CURRENT_LANGUAGE_REPO}/final_classified_commits_batch.csv"

def download_batch_results():
    """Downloads the prediction results from the correct GCS folder."""
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    
    print(f"Searching for results in: gs://{GCS_BUCKET_NAME}/{GCS_RESULTS_PATH}")
    
    # Batch jobs create a subfolder. We need to find the prediction file inside it.
    blobs = list(bucket.list_blobs(prefix=GCS_RESULTS_PATH))
    results_blob = next((blob for blob in blobs if "prediction.results" in blob.name and blob.name.endswith(".jsonl")), None)

    if not results_blob:
        raise FileNotFoundError(f"Could not find a 'prediction.results...jsonl' file in gs://{GCS_BUCKET_NAME}/{GCS_RESULTS_PATH}. Please check the path and job status.")

    print(f"Found results file: {results_blob.name}")
    print(f"Downloading to: {LOCAL_DOWNLOAD_PATH}")
    os.makedirs(os.path.dirname(LOCAL_DOWNLOAD_PATH), exist_ok=True)
    results_blob.download_to_filename(LOCAL_DOWNLOAD_PATH)
    print("  -> Download successful.")


def process_and_merge():
    """
    Reads the downloaded results file, parses the specific Gemini batch output format,
    and merges the classifications with the original metadata.
    """
    all_predictions = []
    
    print(f"\nProcessing downloaded results file: {LOCAL_DOWNLOAD_PATH}")
    with open(LOCAL_DOWNLOAD_PATH, 'r') as f:
        # Use tqdm for a progress bar, as parsing can take a moment
        for line in tqdm(f, desc="Parsing Predictions"):
            try:
                # First parse: load the entire line as a JSON object
                data = json.loads(line)
                
                # Extract the commit hash from the top-level 'key'
                commit_hash = data['key']
                
                # Second parse: navigate deep to the model's output and parse it
                # This is the most critical and fragile part
                prediction_string = data['response']['candidates'][0]['content']['parts'][0]['text']
                prediction_json = json.loads(prediction_string)
                
                all_predictions.append({
                    'commit_id': commit_hash,
                    'is_bug_fix': prediction_json['is_bug_fix'],
                    'predicted_category': prediction_json['category'],
                    'predicted_reasoning': prediction_json['reasoning']
                })

            except (json.JSONDecodeError, KeyError, IndexError) as e:
                # This is a robust catch-all for any parsing failures
                commit_hash_fallback = data.get('key', 'UNKNOWN')
                print(f"Warning: Could not parse line for commit {commit_hash_fallback}, skipping. Error: {e}")
                all_predictions.append({
                    'commit_id': commit_hash_fallback,
                    'predicted_category': 'PARSING_FAILED',
                    'predicted_reasoning': str(e)
                })

    print(f"\nSuccessfully processed {len(all_predictions)} predictions.")
    print("Merging predictions with full commit metadata...")
    predictions_df = pd.DataFrame(all_predictions)
    
    metadata_df = pd.read_csv(FULL_METADATA_CSV)
    
    final_df = pd.merge(metadata_df, predictions_df, on='commit_hash', how='left')
    
    final_df.to_csv(FINAL_CLASSIFIED_CSV, index=False)
    
    print("\n--- ✅ SUCCESS! ---")
    print(f"Saved the final, complete, and classified dataset to: {FINAL_CLASSIFIED_CSV}")


if __name__ == "__main__":
    if not all([PROJECT_ID, GCS_BUCKET_NAME, os.getenv('BLOB_BATCHING_RESULTS')]):
         raise ValueError("FATAL: One or more required environment variables are not set.")
    
    #download_batch_results()
    #process_and_merge()
    successful_parses = 0
    failed_parses = 0
    error_summary = {}

OUTPUT_FILE = 'FINAL_CLASSIFIED_FULL_DATA/c/fully_classified.csv'
output_dir = os.path.dirname(OUTPUT_FILE)
if not os.path.exists(output_dir):
    print(f"Creating output directory: {output_dir}")
    os.makedirs(output_dir)
processed_data = []
error_summary = {}

print(f"Reading from: {LOCAL_DOWNLOAD_PATH}")

with open(LOCAL_DOWNLOAD_PATH, 'r') as f:
    lines = list(f)
    for i, line in enumerate(tqdm(lines, desc="Processing Lines")):
        
        # --- Initialize a dictionary with default/empty values ---
        # This ensures all our records have the same keys
        record = {
            'original_line_num': i + 1,
            'key': None,
            'request_text': None,
            'is_bug_fix': None,
            'category': None,
            'reasoning': None,
            'parsing_error_type': None,
            'parsing_error_payload': None,
        }

        try:
            # 1. Parse the main JSONL line
            data = json.loads(line)
            
            # --- Populate the keys we know should exist ---
            record['key'] = data.get('key')
            if 'request' in data and 'contents' in data['request'] and data['request']['contents']:
                record['request_text'] = data['request']['contents'][0]['parts'][0].get('text')

            # 2. Safely access and parse the nested text
            if 'response' in data and 'candidates' in data['response'] and data['response']['candidates']:
                raw_text = data['response']['candidates'][0]['content']['parts'][0]['text']
                
                match = re.search(r'\{.*\}', raw_text, re.DOTALL)
                if not match:
                    raise ValueError("Could not find a JSON object within the text.")
                
                json_string = match.group(0)
                parsed_text = json.loads(json_string, strict=False)
                
                # --- Populate the successfully parsed data ---
                record['is_bug_fix'] = parsed_text.get('is_bug_fix')
                record['category'] = parsed_text.get('category')
                record['reasoning'] = parsed_text.get('reasoning')

            else:
                # This handles cases where the 'response' or 'candidates' keys are missing
                raise KeyError("Path to 'response' or 'candidates' not found.")

        except Exception as e:
            # --- If anything fails, log the error and save the raw text ---
            error_type = type(e).__name__
            record['parsing_error_type'] = error_type
            
            # Try to get the problematic text if it exists, otherwise store the whole line
            try:
                record['parsing_error_payload'] = data['response']['candidates'][0]['content']['parts'][0]['text']
            except (KeyError, IndexError, NameError):
                record['parsing_error_payload'] = line.strip()

            error_summary[error_type] = error_summary.get(error_type, 0) + 1
        
        # --- Add the fully populated record to our results ---
        processed_data.append(record)


# --- Save the results to a new JSONL file ---
print(f"\nSaving {len(processed_data)} processed records to: {OUTPUT_FILE}")
final_classified_data = pd.DataFrame(processed_data)
final_classified_data.to_csv(OUTPUT_FILE,index=False)


# --- Final Summary ---
successful_parses = len(processed_data) - sum(error_summary.values())
failed_parses = sum(error_summary.values())

print("\n====================")
print("Processing Complete.")
print(f"Successfully processed records: {successful_parses}")
print(f"Records with parsing errors: {failed_parses}")
print("\nError Summary:")
if not error_summary:
    print("No errors found!")
else:
    for error, count in sorted(error_summary.items()):
        print(f"- {error}: {count} times")
print("====================")