import os
import json
import pandas as pd
from tqdm import tqdm
from git import Repo


REPO_URL = "https://gitlab.gnome.org/GNOME/libxml2.git"
LOCAL_REPO_PATH = "./repos/c/libxml2"
# /filename
full_commit_set = "full_commit_with_author_data/full_commit_libxml2.csv"
FULL_COMMIT_JSONL = "full_commit_jsonl/FULL_commit_toclassify.jsonl"
GOLD_STANDARD_CSV = "gold_standard_sample/labeled_commits.csv" # CSV with commit_hash and its true JSON label
ALL_CATEGORIES = [
    "Parser Logic", "Memory", "General Logic Error", "API Logic",
    "Security Vulnerability (CVE)", "Integer", "Error Handling",
    "Concurrency", "Type System", "State Management", "Performance",
    "Incorrect Output/Calculation", "Standard Library Misuse",
    "Build/CI/Tests", "Refactoring", "Documentation",
    "Feature/Enhancement", "Non-Maintenance"
]
INSTRUCTION_PROMPT = (
    "You are a world-class software engineering analyst specializing in C code. "
    "Your primary evidence must be the code_diff. Analyze it to determine the bug's root cause. "
    "Use the commit_message only as a secondary clue if the code is ambiguous. "
    f"Respond ONLY with a valid JSON object. Available categories are: {json.dumps(ALL_CATEGORIES)}"
)


def get_commit_diff_only(commit):
    # This correctly handles the root commit (Case 3)
    if not commit.parents:
        return "" 
    output_parts = []
    
    # always compare to main [0]
    diff_index = commit.diff(commit.parents[0], create_patch=True)
    for diff_item in diff_index:
        patch = diff_item.diff.decode('utf-8',errors='replace')
        output_parts.append(patch)
        # Join all the parts into a single string
    return "\n".join(output_parts)
        



def create_jsonl_from_df(df, output_filename):
    """Converts a DataFrame to a JSONL file in the required format."""
   
    with open(output_filename, 'w') as f:
        for index, row in df.iterrows():
            commit_id = row['commit_id']
            commit_message = row['message']
            diff_text = row['diff']

            prompt_template = (
       "You are a world-class software engineering analyst specializing in the libxml2 library. "
            "Analyze the following commit message and code diff, then classify it. "
            "Respond ONLY with a valid JSON object containing 'is_bug_fix', 'category', and 'reasoning'.\n\n"
             f"AVAILABLE CATEGORIES:\n{json.dumps(ALL_CATEGORIES)}\n\n"
            f"--- COMMIT MESSAGE ---\n{commit_message}"
            f"--- CODE DIFF ---\n{diff_text}"
    )

            final_line_object = {
                "request":{
                "contents": [{
                    "role": "user",
                    "parts": [{
                        "text": prompt_template
                    }]
                }]
                }, 
                "key": commit_id
            }
            f.write(json.dumps(final_line_object) + "\n")
    print(f"Successfully created {output_filename}")

if __name__ == "__main__":

    
    # 2. Extract Diffs and Messages from Git
    if os.path.exists(full_commit_set):
        commits_df = pd.read_csv(full_commit_set)
    else:
        repo = Repo(LOCAL_REPO_PATH)
        diff_data = []
        for commit in tqdm(repo.iter_commits(), desc="Extracting Commits & Diffs"):
            diff_text = get_commit_diff_only( commit)
            diff_data.append({
                "commit_id": commit.hexsha,
                "message" : commit.message,
                "diff": diff_text,
                 "author_name": commit.author.name,
                "authored_datetime": commit.authored_datetime 
            })
        commits_df = pd.DataFrame(diff_data)
        commits_df.to_csv(full_commit_set,index=False)


    # 6. Create JSONL files
    create_jsonl_from_df(commits_df, FULL_COMMIT_JSONL)


    # create sample 
    gold_sample = commits_df.sample(frac=0.1, random_state=42)
    gold_sample[['commit_id', 'message','diff']].to_csv(GOLD_STANDARD_CSV, index=False)
    print(f"Successfully saved a sample of {GOLD_STANDARD_CSV} commits to '{GOLD_STANDARD_CSV}'")
   


