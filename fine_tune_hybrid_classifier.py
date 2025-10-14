import os
import re
import time
import json
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from google.cloud import aiplatform_v1
from git import Repo
import vertexai.preview
from vertexai.generative_models import GenerativeModel, Part

# --- NEW: Vertex AI and Google Cloud Configuration ---
# You will need to install the library: pip install google-cloud-aiplatform


load_dotenv()

# --- Configuration ---



ENDPOINT_ID = os.getenv("ENDPOINT_ID")
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")

REPO_URL = "https://gitlab.gnome.org/GNOME/libxml2.git"
LOCAL_REPO_PATH = "./repos/c/libxml2"





# --- Configuration ---
#INPUT_DATASET_PATH = "gold_standard_500.csv" # The large dataset you want to classify
OUTPUT_CSV_PATH = "fine_tune_test.csv"
REQUEST_DELAY_SECONDS = 0.2 # We can make this much faster for a dedicated endpoint


# --- Heuristic Logic (Unchanged) ---
# (Your complete, proven heuristic functions: HIGH_CONFIDENCE_BUGS, ORDERED_BUG_TOPICS,
# NON_BUG_TOPICS, is_bug_fix_indicator, and classify_commit_heuristically go here)
HIGH_CONFIDENCE_BUGS = { 'Memory': ['use-after-free', 'uaf', 'null deref', 'double free', 'memory leak', 'heap-buffer-overflow', 'stack-buffer-overflow', 'invalid free', 'segfault'], 'Security Vulnerability (CVE)': [r'cve-\d{4}-\d{4,7}'], 'Integer': ['integer overflow', 'division-by-zero']}
ORDERED_BUG_TOPICS = {
    "Concurrency": ['race condition', 'deadlock', 'thread safe', 'mutex', 'atomic'],
    "Memory": ['memory', 'malloc', 'free', 'asan', 'ubsan', 'coverity', 'oss-fuzz'],
    "Parser Logic": ['parser', 'parsing', 'xpath', 'xquery', 'schema', 'validation', 'namespace', 'entity', 'relaxng', 'schematron', 'dtd'],
    "Error Handling": ['error handling', 'error message', 'error report', 'error recovery', 'xmlerror'],
    "Integer": ['signedness', 'truncation', 'arithmetic'],
}
NON_BUG_TOPICS = {'Build/CI/Tests': ['build', 'ci', 'test', 'compilation', 'compiler', 'warning', 'python', 'cmake', 'meson'], 'Refactoring': ['refactor', 'cleanup', 'rename', 'style', 'cosmetic', 'tidy', 'reformat'], 'Documentation': ['doc', 'docs', 'doxygen', 'man page', 'readme', 'comment'], 'Non-Maintenance': ['bump', 'release', 'revert', 'remove', 'merge', 'version'], 'Feature/Enhancement': ['add', 'feat', 'implement', 'support for', 'introduce']}
FIX_KEYWORDS = ['fix', 'bug', 'solve', 'correct', 'prevent', 'resolve', 'crash', 'fail', 'error']

def is_bug_fix_indicator(message: str) -> bool:
    msg_lower = message.lower()
    if re.search(r'\b(fix(es|ed)?|resolv(es|ed)|clos(es|ed))\s+(#|issue\s+#)\d+', msg_lower): return True
    if 'regress' in msg_lower: return True
    if any(re.search(r'\b' + word + r'\b', msg_lower) for word in FIX_KEYWORDS): return True
    return False

def classify_commit_heuristically(message):
    msg_lower = message.lower()
    for category, phrases in HIGH_CONFIDENCE_BUGS.items():
        for phrase in phrases:
            if re.search(phrase, msg_lower): return {"is_bug_fix": True, "category": category, "reasoning": f"Tier 1 match: '{phrase}'"}
    if is_bug_fix_indicator(message):
        for category, topics in ORDERED_BUG_TOPICS.items():
            for topic in topics:
                if re.search(r'\b' + topic + r'\b', msg_lower): return {"is_bug_fix": True, "category": category, "reasoning": f"Tier 2 match: bug indicator + priority topic '{topic}'"}
        return None
    else:
        for category, topics in NON_BUG_TOPICS.items():
            for topic in topics:
                if re.search(r'\b' + topic + r'\b', msg_lower): return {"is_bug_fix": False, "category": category, "reasoning": f"Tier 3 match: topic '{topic}'"}
        return {"is_bug_fix": False, "category": "Feature/Enhancement", "reasoning": "Heuristic miss, assumed non-bug"}

# --- NEW: Function to Call Your Fine-Tuned Model ---




# The function no longer needs project_id or region
def classify_batch_with_tuned_model(endpoint_id, messages_batch,project_id,region):
    """
    Sends a batch of commit messages to a fine-tuned Gemini model.
    Assumes vertexai.init() has already been called.
    """
    # The initialization is now done outside the function
    # vertexai.init(project=project_id, location=region) 

    # We can get the project and region from the global config now
  
    
    endpoint_name = f"projects/{project_id}/locations/{region}/endpoints/{endpoint_id}"
    
    tuned_model = GenerativeModel(endpoint_name)

    all_predictions = []
    
    for message in messages_batch:
        try:
            prompt = (
        "You are a world-class software engineering analyst specializing in the libxml2 library. "
        "Analyze the following commit message and classify it. Respond ONLY with a valid JSON object "
        "containing three keys: a boolean 'is_bug_fix', the string 'category', and a one-sentence 'reasoning'.\n\n"
        "AVAILABLE CATEGORIES:\n"
        "[\"Parser Logic\", \"Memory\", \"General Logic Error\", \"API Logic\", \"Security Vulnerability (CVE)\", \"Integer\", \"Error Handling\", \"Concurrency\", \"Type System\", \"State Management\", \"Performance\", \"Incorrect Output/Calculation\", \"Standard Library Misuse\", \"Build/CI/Tests\", \"Refactoring\", \"Documentation\", \"Feature/Enhancement\", \"Non-Maintenance\"]\n\n"
        "--- COMMIT MESSAGE ---\n"
        f"{message}"
    )


            response = tuned_model.generate_content([prompt])
            raw_response_str = response.candidates[0].content.parts[0].text
            result = json.loads(raw_response_str)
            all_predictions.append(result)
        except Exception as e:
            print(f"    [!] Error processing message with Gemini model: {type(e).__name__}: {e}")
            all_predictions.append(None)
            
    return all_predictions
# --- Main Execution with the FINAL Hybrid Logic ---
if __name__ == "__main__":
    try:
        PROJECT_ID = PROJECT_ID
        REGION = REGION
        vertexai.init(project=PROJECT_ID, location=REGION)
        print("Vertex AI SDK Initialized successfully.")
    except Exception as e:
        exit(f"Error initializing Vertex AI SDK: {e}")

    repo = Repo(LOCAL_REPO_PATH)
    commits = list(repo.iter_commits())
    df = pd.DataFrame([{"commit_hash": c.hexsha, "message": c.message} for c in tqdm(commits, desc="Extracting Commits")])
    
    
    # --- PHASE 1: SEPARATE HEURISTIC AND MODEL WORK ---
    print("Phase 1: Running heuristics and preparing model batch...")
    
    # Lists to store the plan
    results_placeholder = [None] * df.shape[0]  # A list to hold final results in order
    model_batch_indices = []                   # Indices of rows that need the model
    model_batch_messages = []                  # Messages for the rows that need the model
    heuristic_count = 0

    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Running Heuristics"):
        message = row['message']
        heuristic_result = classify_commit_heuristically(message)
        
        if heuristic_result:
            # If heuristic is confident, store its result immediately
            results_placeholder[index] = heuristic_result
            heuristic_count += 1
        else:
            # Otherwise, add the message and its index to the batch to be processed by the model
            model_batch_indices.append(index)
            model_batch_messages.append(message)

    model_count = len(model_batch_messages)
    print(f"Heuristics handled {heuristic_count} commits. {model_count} commits require the fine-tuned model.")

    # --- PHASE 2: BATCH MODEL INFERENCE ---
    if model_batch_messages:
        print(f"Phase 2: Calling fine-tuned model for a batch of {model_count} commits...")
        
        # This makes ONE single, efficient API call for all remaining commits


      

        model_results = classify_batch_with_tuned_model(
            ENDPOINT_ID, # Just pass the Endpoint ID
            model_batch_messages,PROJECT_ID,REGION
        )
        # Now, distribute the model results back to their original positions
        for i, result in enumerate(model_results):
            original_index = model_batch_indices[i]
            if result:
                results_placeholder[original_index] = result
            else:
                # Fallback in case the model call failed or a single result was bad
                results_placeholder[original_index] = {
                    "is_bug_fix": True,
                    "category": "General Logic Error",
                    "reasoning": "Model inference failed. Defaulting to fallback."
                }
        print("Batch processing complete.")

    # --- PHASE 3: COMBINE AND SAVE ---
    print("Phase 3: Combining results and saving to CSV...")
    
    # Create the final list of dictionaries for the output DataFrame
    all_results_data = []
    for index, row in df.iterrows():
        final_result = results_placeholder[index]
        all_results_data.append({
            "commit_hash": row['commit_hash'],
            "message": row['message'],
            "is_bug_fix": final_result.get('is_bug_fix'),
            "category": final_result.get('category'),
            "reasoning": final_result.get('reasoning')
        })

    # Combine results and save to the final CSV
    final_df = pd.DataFrame(all_results_data)
    final_df.to_csv(OUTPUT_CSV_PATH, index=False)

    # --- FINAL ANALYSIS (Unchanged) ---
    maintenance_commits = final_df[final_df['is_bug_fix'] == True]
    if not maintenance_commits.empty:
        print("\n--- Error Profile (Distribution of Bug Fixes) ---")
        dist = maintenance_commits['category'].value_counts(normalize=True) * 100
        profile = pd.DataFrame({'Percentage': dist.map('{:.2f}%'.format)})
        print(profile.to_string())
        # print("\n--- Generating 'Other Bug' Dataset for Phase 2 Deep Dive ---") # Optional

    print("\n--- Classification Complete ---")
    print(f"Results for {len(final_df)} commits saved to '{OUTPUT_CSV_PATH}'")
    print(f"Handled by Heuristics: {heuristic_count} ({heuristic_count/len(final_df):.2%})")
    print(f"Handled by Fine-Tuned Model: {model_count} ({model_count/len(final_df):.2%})")