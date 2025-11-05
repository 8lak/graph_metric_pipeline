import os
import requests
import pandas as pd
from git import Repo
from tqdm import tqdm
from dotenv import load_dotenv
import datetime
import time

# --- (Import your classifier and diff functions as before) ---
# For demonstration, we use placeholders

def get_commit_diff_only(repo, commit_hash):
    try:
        commit = repo.commit(commit_hash)
        if not commit.parents: return ""
        diffs = [item.diff.decode('utf-8', errors='ignore') for item in commit.diff(commit.parents[0], create_patch=True)]
        return "\n".join(diffs)
    except Exception: return None

# --- Configuration ---
load_dotenv()
DELTA_OUTPUT_CSV = "delta_commits.csv"
GITHUB_OWNER = "GNOME"
GITHUB_REPO = "libxml2" # Note: libxml2 is on GitLab, this is an example for a GitHub repo
GITHUB_PAT = os.getenv("GITHUB_API_KEY")

LOCAL_REPO_PATH = "./repos/c/libxml2" # Update path if using a different repo
EXISTING_GOLD_STANDARD_CSV = "gold_standard_500.csv"
FINAL_OUTPUT_CSV = "pr_gold_standard_github.csv"

# --- Core API Functions (Implementation of your pseudocode) ---
API_BASE_URL = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}"
HEADERS = {
    "Accept": "application/vnd.github.v3+json",
    "Authorization": f"Bearer {GITHUB_PAT}"
}

def make_api_request_with_pagination(endpoint):
    """Makes a request and handles GitHub's Link header for pagination."""
    results = []
    url = f"{API_BASE_URL}{endpoint}"
    
    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 403: # Rate limit hit
            print("Rate limit hit. Waiting for 60 seconds...")
            time.sleep(60)
            continue # Retry the same URL
            
        response.raise_for_status() # Raise an exception for other bad status codes
        results.extend(response.json())
        
        # Check for next page in 'Link' header
        if 'next' in response.links:
            url = response.links['next']['url']
        else:
            url = None
    return results



# --- API Setup ---


# --- Your Gold Standard Dataset ---
# TODO: This is your starting list of 500 commit IDs.
# In a real scenario, you would load this from a file (e.g., a CSV or text file).
gold = pd.read_csv("gold_standard_500.csv")
COMMIT_IDS_TO_PROCESS = gold['commit_hash']

def process_all_commits(commit_ids):
    """
    Main function to orchestrate the data retrieval and enrichment process.

    """
    all_processed_shas = set(commit_ids) # This will be our master mask
    found_pr_numbers = set() # To store unique PRs to investigate
    enriched_commits_data = []
    print(f"Starting processing for {len(commit_ids)} commits...")

    for i, commit_sha in enumerate(commit_ids):
        print(f"\nProcessing commit {i+1}/{len(commit_ids)}: {commit_sha}")

        # --- Step 1: Find all pull requests associated with the commit ---
        # METHOD: GET
        # ENDPOINT: /repos/{owner}/{repo}/commits/{commit_sha}/pulls
        
        prs_url = f"{API_BASE_URL}/commits/{commit_sha}/pulls"
        try:
            response = requests.get(prs_url, headers=HEADERS)
            response.raise_for_status() # Raises an exception for bad status codes (4xx or 5xx)
            associated_prs = response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"  [ERROR] Could not fetch PRs for commit {commit_sha}. Error: {e}")
            continue

        if not associated_prs:
            print(f"  [INFO] Commit {commit_sha} is not associated with any pull request (e.g., direct push). Skipping.")
            continue

        # --- Step 2: Apply "First-Come, First-Served" logic ---
        # We only care about PRs that were actually merged to determine the "first" one.
        merged_prs_list = []
        merged_pr_numbers_set = set()

# Loop through the data once, adding to both structures
        for pr in associated_prs:
            if pr.get("merged_at"):
                # Add the full dictionary to the list
                merged_prs_list.append({
                    "number": pr["number"],
                    "created_at": pr["created_at"],
                    "merged_at": pr["merged_at"],
                })
                # Add the PR number to the set for fast lookups
   

        if not merged_pr_numbers_set:
            print(f"  [INFO] Commit {commit_sha} is in open PR(s) but none are merged yet. Skipping.")
            continue

        # Sort the merged PRs by their merge date to find the earliest one.
        merged_prs_list.sort(key=lambda pr: datetime.fromisoformat(pr['merged_at'].replace('Z', '+00:00')))
        pr_number = merged_prs_list[0]['number']
        merged_pr_numbers_set.add(pr_number)


        if len(merged_prs_list) > 1:
            print(f"  [INFO] Commit found in {len(merged_prs_list)} merged PRs. Using PR #{pr_number} (the first one merged).")
        else:
            print(f"  [INFO] Found commit in PR #{pr_number}.")

        # --- Step 3: Retrieve full PR metadata for timestamps ---
        # 

        # --- Step 4: Retrieve detailed commit message and file diffs ---
        # METHOD: GET
        # ENDPOINT: /repos/{owner}/{repo}/commits/{commit_sha}

        commit_delta_sha = all_processed_shas - found_pr_numbers
        commit_details_url = f"{API_BASE_URL}/commits/{commit_delta_sha}"
        try:
            response = requests.get(commit_details_url, headers=HEADERS)
            response.raise_for_status()
            commit_details = response.json()

            commit_message = commit_details['commit']['message']
            # The 'patch' contains the diff content for each file changed in the commit.
            file_diffs = [file.get('patch', '') for file in commit_details.get('files', [])]

        except requests.exceptions.RequestException as e:
            print(f"  [ERROR] Could not fetch details for commit {commit_sha}. Error: {e}")
            continue

        # --- Step 5: Structure the data for your final dataset ---
        final_record = {
            "commit_sha": commit_sha,
            "associated_pr_number": pr_number,
            "pr_created_at": pr_created_at,
            "pr_merged_at": pr_merged_at,
            "commit_message": commit_message,
            "file_diffs": file_diffs,
            "classification": None # Placeholder for your model's output
        }
        enriched_commits_data.append(final_record)

    return enriched_commits_data

# --- Main execution block ---
if __name__ == "__main__":
    final_dataset = process_all_commits(COMMIT_IDS_TO_PROCESS)

    print(f"\n\n--- Processing Complete ---")
    print(f"Successfully enriched {len(final_dataset)} commits.")

    # --- Next Steps ---
    # 1. Save this `final_dataset` to a file (e.g., JSON or CSV).
    final_df = pd.DataFrame(final_dataset)
    final_df.to_csv(FINAL_OUTPUT_CSV,index=False)
    # 2. Use the 'commit_message' and 'file_diffs' as input to your classification model.
    # 3. Fill in the 'classification' field for each record.
    # 4. Use the 'pr_merged_at' timestamps to build your time series.
    # 5. Group by PR number and analyze the error profile distribution over time.

    # Example: Print the first record as a sample of the output
    if final_dataset:
        import json
        print("\n--- Example Output for the first processed commit ---")
        print(json.dumps(final_dataset[0], indent=2))