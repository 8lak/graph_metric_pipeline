import os
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from git import Repo

REPO_URL = "https://gitlab.gnome.org/GNOME/libxml2.git"
LOCAL_REPO_PATH = "./repos/c/libxml2"
OUTPUT_CSV_PATH = "full_commit_data.csv"



if __name__ == "__main__":
    
    # 1. Check if the local repository exists
    if not os.path.isdir(LOCAL_REPO_PATH):
        print(f"Local repository not found at {LOCAL_REPO_PATH}")
        # Optional: Add cloning logic here if you want to automate it
        # from git import Git
        # print(f"Cloning repository from {REPO_URL}...")
        # Git().clone(REPO_URL, LOCAL_REPO_PATH)
        # print("Cloning complete.")
    
    # 2. Open the repository
    print(f"Opening repository at: {LOCAL_REPO_PATH}")
    repo = Repo(LOCAL_REPO_PATH)

    # 3. Extract all commits from the default branch's history
    # This is efficient and correct.

    commits = list(repo.iter_commits())
    
    # 4. Create a DataFrame using a list comprehension with a tqdm progress bar
    # This is the best way to do it - fast and provides user feedback.
    print(f"Found {len(commits)} commits. Extracting full details...")
    full_commit_data = []
    for c in tqdm(commits, desc="Extracting Commits"):
        full_commit_data.append({
            "commit_id": c.hexsha,
            "message": c.message,
            "author_name": c.author.name,
            "authored_datetime": c.authored_datetime # This is the key timestamp!
        })

    df = pd.DataFrame(full_commit_data)
    df.to_csv(OUTPUT_CSV_PATH, index=False)

    print(f"\nSuccessfully saved {len(df)} commits to {OUTPUT_CSV_PATH}")



    df = pd.read_csv("gold_standard_500.csv")
    df_1 = pd.read_csv("gold_standard_with_second_opinion.csv")
   
    df_gold = pd.merge(df_1,df,on='commit_hash')


    df_final = df_gold[['commit_hash','is_bug_fix','message_x','diff','category_v2_general_model','reasoning_v2']].copy()
    df_final.to_csv("gold_standard_sample/500_sample.csv",index=False)
