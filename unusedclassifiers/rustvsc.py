import os
import re
import pandas as pd
from git import Repo, GitCommandError
from tqdm import tqdm

# --- Configuration ---
# Official libxml2 git mirror
REPO_URL = "https://gitlab.gnome.org/GNOME/libxml2.git"
LOCAL_REPO_PATH = "./repos/c/libxml2"

# --- Classification Engine ---

# Keywords to identify a commit as being a bug fix or maintenance task
MAINTENANCE_KEYWORDS = [
    'fix', 'bug', 'issue', 'solve', 'patch', 'repair', 'correct',
    'resolve', 'prevent', 'crash', 'fail', 'error', 'asan', 'ubsan',
    'coverity', 'oss-fuzz' # Add tools that find bugs
]
MAINTENANCE_REGEX = re.compile(r'\b(' + '|'.join(MAINTENANCE_KEYWORDS) + r')\b', re.IGNORECASE)

def is_maintenance_commit(message: str) -> bool:
    """Returns True if a commit message likely represents maintenance."""
    if not isinstance(message, str):
        return False
    return bool(MAINTENANCE_REGEX.search(message))

# C-specific error categories and their associated keywords
# In analyze_libxml2.py, replace the C_ERROR_BUCKETS with this definitive version.
C_ERROR_BUCKETS = {
    "Memory": [
        'buffer overflow', 'out-of-bounds', 'oob', 'use-after-free', 'uaf',
        'memory leak', 'dangling pointer', 'segfault', 'segmentation fault',
        'null pointer', 'dereference', 'invalid read', 'invalid write',
        'heap-buffer-overflow', 'stack-buffer-overflow', 'double free',
        'coverity', 'oss-fuzz', 'asan', 'ubsan',
        # --- Keywords from our discovery ---
        'malloc fail', 'memory management', 'invalid free', 'null deref'
    ],
    "Parser Logic": [
        'parser', 'parsing', 'xpath', 'xquery', 'schemas', 'validation',
        'namespace', 'entities', 'wrong result', 'incorrect result', 'html parser'
    ],
    "Error Handling": [
        'error handling', 'error message', 'error reporting', 'error recovery',
        'error code', 'xmlerror', 'structured error', 'error handler'
    ],
    "Build/CI/Tests": [
        'build fix', 'fix build', 'ci fix', 'fix compilation', 'compiler warning',
        'compilation error', 'fix warning', 'python tests', 'regression test',
        'added test'
    ],
    "Integer": [
        'integer overflow', 'wrap around', 'signedness', 'truncation',
        'arithmetic overflow', 'divide by zero', 'division-by-zero'
    ],
    "Security": [
        'cve-', 'vulnerability', 'exploit', 'security', 'directory traversal'
    ]
    # Dropping low-signal categories like Pointer/Aliasing and Concurrency for now
}
# Pre-compile regex for performance
C_ERROR_REGEX = {cat: re.compile(r'(' + '|'.join(keys) + r')', re.IGNORECASE) for cat, keys in C_ERROR_BUCKETS.items()}

def classify_c_commit(message: str) -> str:
    """
    Classifies a C commit message into the *first* matching category.
    Returns 'Other Fix' if it's a maintenance commit but doesn't match a specific bucket.
    """
    if not isinstance(message, str):
        return "Other Fix"
    
    # Iterate through categories and return the first one that matches
    for category, regex in C_ERROR_REGEX.items():
        if regex.search(message):
            return category
            
    # If it was identified as a maintenance commit but didn't fit a specific
    # category, we can label it as a general logic fix.
    return "Other Fix"

# --- Main Execution ---

if __name__ == "__main__":
    # 1. Acquire Data: Clone or open the repository
    print(f"Target repository: {REPO_URL}")
    os.makedirs(LOCAL_REPO_PATH, exist_ok=True)
    try:
        if os.listdir(LOCAL_REPO_PATH):
             print(f"Repository already exists at {LOCAL_REPO_PATH}. Using existing data.")
             repo = Repo(LOCAL_REPO_PATH)
        else:
            print(f"Cloning repository into {LOCAL_REPO_PATH}...")
            repo = Repo.clone_from(REPO_URL, LOCAL_REPO_PATH)
            print("Clone complete.")
    except GitCommandError as e:
        print(f"Error interacting with the repository: {e}")
        exit()

    # 2. Extract Data: Load all commits into a list
    print("Extracting commit history... (this may take a moment)")
    try:
        commits = list(repo.iter_commits())
        commit_data = [{
            "commit_hash": c.hexsha,
            "message": c.message
        } for c in tqdm(commits, desc="Processing commits")]
        
        df = pd.DataFrame(commit_data)
        print(f"Extracted a total of {len(df)} commits.")
    except Exception as e:
        print(f"Failed to extract commits: {e}")
        exit()
        
    # 3. Classify Commits
    print("Classifying commits...")
    
    # First, identify all maintenance commits
    df['is_maintenance'] = df['message'].apply(is_maintenance_commit)
    
    # Filter down to only the maintenance commits
    maintenance_df = df[df['is_maintenance']].copy()
    
    # Categorize the maintenance commits into specific error buckets
    if not maintenance_df.empty:
        maintenance_df['error_category'] = maintenance_df['message'].apply(classify_c_commit)
    
    # 4. Analyze and Report Results
    total_commits = len(df)
    maintenance_commits_count = len(maintenance_df)
    
    if total_commits == 0:
        print("No commits found to analyze.")
    else:
        # Calculate Maintenance Ratio
        maintenance_ratio = (maintenance_commits_count / total_commits) * 100
        
        print("\n--- Analysis Results for libxml2 ---")
        print(f"Total Commits Analyzed: {total_commits}")
        print(f"Commits Classified as Maintenance/Fix: {maintenance_commits_count}")
        print(f"Maintenance Ratio: {maintenance_ratio:.2f}%")
        
        # Calculate Error Profile
        if maintenance_commits_count > 0:
            error_distribution = maintenance_df['error_category'].value_counts()
            error_distribution_percent = maintenance_df['error_category'].value_counts(normalize=True) * 100
            
            # Combine counts and percentages for a nice table
            error_profile_df = pd.DataFrame({
                'Count': error_distribution,
                'Percentage': error_distribution_percent.map('{:.2f}%'.format)
            })
            
            print("\n--- Error Profile (Distribution of Maintenance Commits) ---")
            print(error_profile_df.to_string())
        else:
            print("\nNo maintenance commits were identified based on the keywords.")

        # (Add this code to the end of the `if __name__ == "__main__"` block in analyze_libxml2.py)

        # --- Keyword Discovery Step ---
        # After printing the analysis, save the 'Other Fix' messages for further study.
        if maintenance_commits_count > 0:
            other_fix_df = maintenance_df[maintenance_df['error_category'] == 'Other Fix']
            if not other_fix_df.empty:
                output_filename = "other_fix_messages.txt"
                with open(output_filename, 'w', encoding='utf-8') as f:
                    for message in other_fix_df['message']:
                        f.write(message + '\n---\n') # Use a separator for clarity
                print(f"\nSaved {len(other_fix_df)} 'Other Fix' commit messages to {output_filename} for keyword analysis.")