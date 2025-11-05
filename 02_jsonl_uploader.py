import os
import json
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

# --- Configuration ---
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME") # e.g., "my-vertex-ai-finetuning-bucket"
BLOB_TRAINING_DESTINATION = os.getenv("BLOB_TRAINING_DESTINATION")
BLOB_BATCHING_TO_CLASSIFY_DESTINATION = os.getenv("BLOB_BATCHING_TO_CLASSIFY_DESTINATION")
LOCAL_REPO_PATH = "./repos/c/libxml2"
EXISTING_GOLD_STANDARD_CSV = "gold_standard_sample/labeled_commits.csv" # Your V1 classified data
OUTPUT_FOR_REVIEW_JSONL = "gold_standard_sample/500_sample.jsonl" # The final output for you to review
FULL_COMMIT_JSONL = "full_commit_jsonl/FULL_commit_toclassify.jsonl"


# --- ADD THIS VALIDATION BLOCK ---
if not PROJECT_ID:
    raise ValueError("FATAL: Environment variable PROJECT_ID is not set.")
if not REGION:
    raise ValueError("FATAL: Environment variable REGION is not set.")
if not GCS_BUCKET_NAME:
    raise ValueError("FATAL: Environment variable GCS_BUCKET_NAME is not set.")
if not BLOB_TRAINING_DESTINATION:
    raise ValueError("FATAL: Environment variable BLOB_TRAINING_DESTINATION is not set.")
if not BLOB_BATCHING_TO_CLASSIFY_DESTINATION:
    raise ValueError("FATAL: Environment variable BLOB_BATCHING_DESTINATION is not set.")

# for use in my human in the loop classifcation

# The "decoupled" prompt for the general model
ALL_CATEGORIES = [
    "Parser Logic", "Memory", "General Logic Error", "API Logic", "Security Vulnerability (CVE)",
    "Integer", "Error Handling", "Concurrency", "Type System", "State Management", "Performance",
    "Incorrect Output/Calculation", "Standard Library Misuse", "Build/CI/Tests", "Refactoring",
    "Documentation", "Feature/Enhancement", "Non-Maintenance"
]
INSTRUCTION_PROMPT = (
    "You are a world-class software engineering analyst specializing in C code. "
    "Your primary evidence must be the code_diff. Analyze it to determine the bug's root cause. "
    "Use the commit_message only as a secondary clue if the code is ambiguous. "
    f"Respond ONLY with a valid JSON object containing 'is_bug_fix', 'category', and 'reasoning'. Available categories are: {json.dumps(ALL_CATEGORIES)}"
)


def upload_to_gcs(project_id,bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client(project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to gs://{bucket_name}/{destination_blob_name}")
    return f"gs://{bucket_name}/{destination_blob_name}"

def create_jsonl_from_df_training(df, output_filename):
    """Converts a DataFrame to a JSONL file in the required format."""
    prompt_template = (
       "You are a world-class software engineering analyst specializing in the libxml2 library. "
            "Analyze the following commit message and code diff, then classify it. "
            "Respond ONLY with a valid JSON object containing 'is_bug_fix', 'category', and 'reasoning'.\n\n"
             f"AVAILABLE CATEGORIES:\n{json.dumps(ALL_CATEGORIES)}\n\n"
            "--- COMMIT MESSAGE ---\n{commit_message}"
            "--- CODE DIFF ---\n{diff_text}"
    )
    with open(output_filename, 'w') as f:
        for index, row in df.iterrows():
            commit_message = row['message_x']
            diff_text = row['diff']
            is_bug = bool(row['is_bug_fix'])
            
            # The structure for the fine-tuning job
            assistant_output_dict = {
                "is_bug_fix": is_bug,
                "category": row['category_v2_general_model'],
                "reasoning": row['reasoning_v2']
            }
            # This is the final structure for a single training example,
            # mirroring the Gemini API 'GenerateContent' request format.
            training_example = {
                "contents": [
                    {
                        "role": "user",
                        "parts": [{"text": prompt_template.format(commit_message=commit_message,diff_text=diff_text)}]
                    },
                    {
                        "role": "model",  # Use 'model' for the assistant's role
                        "parts": [{"text": json.dumps(assistant_output_dict)}]
                    }
                ]
            }
            f.write(json.dumps(training_example) + "\n")
    print(f"Successfully created {output_filename}")


       
if __name__ == "__main__":
    # 1. Load your existing V1 gold standard data
    #df_gold_sample = pd.read_csv(EXISTING_GOLD_STANDARD_CSV)

    #df_upload = df_gold_sample[['commit_id','message','diff','category_v2_general_model','reasoning_v2']].copy()
    #print(f"Loaded {len(df_gold_sample)} commits from the existing gold standard.")
    

    #df_gold_sample.dropna(subset=['diff'], inplace=True)


    
    #create_jsonl_from_df_training(df_gold_sample,OUTPUT_FOR_REVIEW_JSONL)

    #upload_to_gcs(PROJECT_ID,GCS_BUCKET_NAME,OUTPUT_FOR_REVIEW_JSONL,BLOB_TRAINING_DESTINATION)
    upload_to_gcs(PROJECT_ID,GCS_BUCKET_NAME,FULL_COMMIT_JSONL,BLOB_BATCHING_TO_CLASSIFY_DESTINATION)
    
    print("\n--- Success! ---")
    print(f"Created review file: {OUTPUT_FOR_REVIEW_JSONL}")
     
   