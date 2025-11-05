import requests
import os
import pandas as pd
import time
from datetime import datetime
from tqdm import tqdm
from dotenv import load_dotenv
import gitlab

# --- 1. CONFIGURATION (FOR GITLAB) ---
# Project ID for GNOME/libxml2 is 6
GITLAB_PROJECT_ID = 1665 

gitlab_url = "https://gitlab.gnome.org"

project_path = 1665

# TODO: Make sure these file paths are correct
load_dotenv()
INPUT_COMMITS_CSV = "full_commit_data.csv" 
FINAL_ENRICHED_CSV = "gitlab_commits_with_mr_metadata.csv" # New output file name

# --- 2. AUTHENTICATION & API SETUP (FOR GITLAB) ---
# Make sure you have set the GITLAB_PAT environment variable
GITLAB_PAT = os.getenv("GNOME_GITLAB_PAT")
if not GITLAB_PAT:
    raise ValueError("Error: GITLAB_PAT environment variable not set.")


try:
    gl = gitlab.Gitlab(gitlab_url, private_token=GITLAB_PAT)
    # The 'gl.auth()' call is optional but verifies the connection.
    gl.auth() 
except gitlab.exceptions.GitlabError as e:
    print(f"Error connecting to GitLab: {e}")
    exit()

# --- Get the project using the full path ---
try:
    project = gl.projects.get(project_path)
except gitlab.exceptions.GitlabError as e:
    print(f"Error getting project {project_path}: {e}")
    exit()

def enrich_commits_with_mr_data(commit_shas):
    """
    Takes a list of commit SHAs and enriches them with GitLab Merge Request (MR) metadata.
    """
    enriched_data = []
    print(f"--- Starting GitLab enrichment process for {len(commit_shas)} commits ---")

    for commit_sha in tqdm(commit_shas, desc="Enriching Commits via GitLab"):
        
       
        commit = project.commits.get(commit_sha)

        merge_requests = commit.merge_requests()


        try:
      
            if not merge_requests:
                record = {"commit_hash": commit_sha, "mr_iid": None}
                enriched_data.append(record)
                continue

            # "First-come, first-served" logic for the rare case of multiple MRs
            merged_mrs = [mr for mr in merge_requests if mr.get("state") == 'merged' and mr.get("merged_at")]
            if not merged_mrs:
                record = {"commit_hash": commit_sha, "mr_iid": merge_requests[0]['iid'], "mr_merged_at": None}
                enriched_data.append(record)
                continue

            merged_mrs.sort(key=lambda mr: datetime.fromisoformat(mr['merged_at']))
            primary_mr = merged_mrs[0]

            # <<< CHANGE 4: Extract data using GitLab's key names ('iid')
            record = {
                "commit_hash": commit_sha,
                "mr_iid": primary_mr['iid'], # GitLab uses 'iid' for the MR number
                "mr_created_at": primary_mr['created_at'],
                "mr_merged_at": primary_mr['merged_at']
            }
            enriched_data.append(record)

        except requests.exceptions.RequestException as e:
            print(f"\n  [ERROR] Failed for commit {commit_sha[:7]}. Error: {e}")
            record = {"commit_hash": commit_sha, "error": str(e)}
            enriched_data.append(record)

        # GitLab has a default rate limit of 2000 requests/minute, which is very generous.
        # A small delay is still good practice.
        time.sleep(0.1) # We can be much faster with GitLab
        
    return enriched_data

# --- MAIN EXECUTION BLOCK (Modified for resuming) ---
if __name__ == "__main__":
    print("--- Phase 1: Preparation ---")
    try:
        commits_df = pd.read_csv(INPUT_COMMITS_CSV)
        all_commit_shas = set(commits_df['commit_hash'].iloc[0])
    except FileNotFoundError:
        print(f"[FATAL] Input file not found: {INPUT_COMMITS_CSV}.")
        exit()
    
    commit = "745644b9d7057baa7a737d873f19526b8b53dddd"
    print(commit)
   
    commit = project.commits.get(commit)

    merge_requests = commit.merge_requests()


    

    print(merge_requests)


    '''
    already_processed_shas = set()
    try:
        results_df = pd.read_csv(FINAL_ENRICHED_CSV)
        already_processed_shas = set(results_df['commit_hash'].tolist())
        print(f"Found {len(already_processed_shas)} commits already processed. Resuming.")
    except FileNotFoundError:
        print("Output file not found. Starting a new run.")

    
    commits_to_process = list(all_commit_shas - already_processed_shas)
    
    if not commits_to_process:
        print("All commits have already been processed. Nothing to do.")
        exit()
        
    print(f"Total commits to process in this run: {len(commits_to_process)}")

    print("\n--- Phase 2: Data Retrieval from GitLab API ---")
    new_results = enrich_commits_with_mr_data(commits_to_process)

    print("\n--- Phase 3: Saving and Analysis ---")
    if new_results:
        new_results_df = pd.DataFrame(new_results)
        
        # Append to the existing file if it exists, otherwise create it
        # This makes the resume logic work seamlessly
        if os.path.exists(FINAL_ENRICHED_CSV):
            new_results_df.to_csv(FINAL_ENRICHED_CSV, mode='a', header=False, index=False)
        else:
            new_results_df.to_csv(FINAL_ENRICHED_CSV, mode='w', header=True, index=False)
        
        print(f"Successfully processed {len(new_results_df)} new commits.")
        print(f"Data saved to {FINAL_ENRICHED_CSV}")
    else:
        print("No new data was collected.")

        '''