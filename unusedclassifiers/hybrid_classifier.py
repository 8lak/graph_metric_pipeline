import os
import re
import time
import json
import pandas as pd
from git import Repo, GitCommandError
from tqdm import tqdm
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
REPO_URL = "https://gitlab.gnome.org/GNOME/libxml2.git"
LOCAL_REPO_PATH = "./repos/c/libxml2"
OUTPUT_CSV_PATH = "robust_hybrid_classified.csv"

try:
    GOOGLE_API_KEY = os.getenv("GEMINI_KEY")
    genai.configure(api_key=GOOGLE_API_KEY)
    model = genai.GenerativeModel('gemini-2.5-flash-lite')
except Exception as e:
    exit(f"Error configuring the Gemini API: {e}")

REQUEST_DELAY_SECONDS = 4.5
SAMPLE_SIZE = 500 # Keep the 500 sample size for this test run

# --- New, Simplified LLM Prompt (For Bug Categorization ONLY) ---
UNIFIED_BUG_CATEGORIES = [
    "API Logic",
    "Performance",
    "Incorrect Output/Calculation",
    "State Management",
    "Parser Logic", # Add this specific category
    "Error Handling", # Add this specific category
    "Memory",
    "Integer",
    "Concurrency",
    "Type System",
    "Standard Library Misuse",
    "General Logic Error", # Final fallback
]

def create_unified_prompt(commit_message):
    """
    Creates a single, comprehensive prompt for an LLM to classify a known but ambiguous bug fix.
    """
    prompt = f"""
    You are an expert C software engineering analyst specializing in the libxml2 library. The following commit message is a confirmed bug fix that was too ambiguous for a simple heuristic classifier. Your goal is to provide the most specific classification possible.

    --- TASK ---
    Analyze the commit message and classify it into the single most appropriate, specific category from the list below.

    --- BUG-FIX CATEGORIES ---
    - **API Logic**: A bug in the public-facing API, like incorrect parameter handling, or function behavior that violates its documented contract.
    - **Performance**: A fix that specifically addresses a performance issue, like slowness, excessive memory usage, or bottlenecks.
    - **Incorrect Output/Calculation**: A bug that causes the software to produce the wrong result or data, even if it doesn't crash.
    - **State Management**: A bug related to incorrect internal state, like uninitialized variables or corrupted object data.
    - **Parser Logic**: A bug in the core XML/HTML parsing, validation, schema, DTD, or XPath logic.
    - **Error Handling**: A bug in how the program reports or recovers from errors.
    - **Memory**: A classic memory safety bug (e.g., null deref, leak, overflow) that the initial heuristic missed.
    - **Integer**: An integer-related bug (e.g., overflow, truncation) that the initial heuristic missed.
    - **Concurrency**: A threading or race condition bug that the initial heuristic missed.
    - **Type System**: A bug related to incorrect type conversions, casts, or `const` misuse.
    - **Standard Library Misuse**: Fixes incorrect usage of standard C library functions.
    - **General Logic Error**: A bug in the program's logic that does not fit any of the more specific categories above. This is your fallback.

    --- COMMIT MESSAGE TO ANALYZE ---
    "{commit_message}"

    --- INSTRUCTIONS ---
    Respond ONLY with a valid JSON object containing two keys:
    1. "category": The single best category from the list above.
    2. "reasoning": A brief, one-sentence explanation for your choice.

    **JSON OUTPUT**:
    """
    return prompt

# --- The Tiered Heuristic Classifier (Unchanged, it's good!) ---
# (Your tiered heuristic function `classify_commit_heuristically` and its keyword dictionaries go here, exactly as they were in the last script)
HIGH_CONFIDENCE_BUGS = { 'Memory': ['use-after-free', 'uaf', 'null deref', 'double free', 'memory leak', 'heap-buffer-overflow', 'stack-buffer-overflow', 'invalid free', 'segfault'], 'Security Vulnerability (CVE)': [r'cve-\d{4}-\d{4,7}'], 'Integer': ['integer overflow', 'division-by-zero']}
ORDERED_BUG_TOPICS = {
    "Concurrency": ['race condition', 'deadlock', 'thread safe', 'mutex', 'atomic'],
    "Memory": ['memory', 'malloc', 'free', 'asan', 'ubsan', 'coverity', 'oss-fuzz'],
    "Parser Logic": ['parser', 'parsing', 'xpath', 'xquery', 'schema', 'validation', 'namespace', 'entity', 'relaxng', 'schematron', 'dtd'],
    "Error Handling": ['error handling', 'error message', 'error report', 'error recovery', 'xmlerror'],
    "Integer": ['signedness', 'truncation', 'arithmetic'],
}

NON_BUG_TOPICS = {'Build/CI/Tests': ['build', 'ci', 'test', 'compilation', 'compiler', 'warning', 'python', 'cmake', 'meson'], 
                  'Refactoring': ['refactor', 'cleanup', 'rename', 'style', 'cosmetic', 'tidy', 'reformat'], 
                  'Documentation': ['doc', 'docs', 'doxygen', 'man page', 'readme', 'comment'], 
                  'Non-Maintenance': ['bump', 'release', 'revert', 'remove', 'merge', 'version'], 
                  'Feature/Enhancement': ['add', 'feat', 'implement', 'support for', 'introduce']}

#TIER_2_BUG_PRIORITY = ["Concurrency", "Memory", "Parser Logic", "Error Handling", "Integer"]


FIX_KEYWORDS = ['fix', 'bug', 'solve', 'correct', 'prevent', 'resolve', 'crash', 'fail', 'error']

def is_bug_fix_indicator(message: str) -> bool:
    """
    Checks for high-confidence bug-fix indicators in the commit message.
    This is more robust than just checking for 'fix' keywords.
    """
    msg_lower = message.lower()
    
    # Pattern 1: Explicit issue closing keywords (most reliable)
    # e.g., "Fixes #123", "Resolves issue #456"
    if re.search(r'\b(fix(es|ed)?|resolv(es|ed)|clos(es|ed))\s+(#|issue\s+#)\d+', msg_lower):
        return True
        
    # Pattern 2: Regression indicators
    # e.g., "Regressed with abc1234", "Short-lived regression"
    if 'regress' in msg_lower:
        return True
        
    # Pattern 3: The original fix keywords (still valuable)
    if any(re.search(r'\b' + word + r'\b', msg_lower) for word in FIX_KEYWORDS):
        return True
        
    return False

def classify_commit_heuristically(message):
    msg_lower = message.lower()

    # --- TIER 1: Unambiguous, high-confidence bug patterns (No change needed here) ---
    for category, phrases in HIGH_CONFIDENCE_BUGS.items():
        for phrase in phrases:
            if re.search(phrase, msg_lower):
                return {"is_bug_fix": True, "category": category, "reasoning": f"Tier 1 match: '{phrase}'"}

    # --- TIER 2: Broader bug identification and classification ---
    if is_bug_fix_indicator(message):
        # Now that we are confident it's a bug, find the best category
        for category, frases  in ORDERED_BUG_TOPICS.items():
            for p in frases:
                if re.search(r'\b' + p + r'\b', msg_lower):
                    return {"is_bug_fix": True, "category": category, "reasoning": f"Tier 2 match: bug indicator + priority topic '{p}'"}
        
        # If it's a bug but doesn't match a priority topic, let the LLM decide
        return None # Let LLM handle ambiguous bugs

    # --- TIER 3: Non-bug maintenance commits (Runs ONLY if no bug indicator was found) ---
    else:
        for category, topics in NON_BUG_TOPICS.items():
            for topic in topics:
                if re.search(r'\b' + topic + r'\b', msg_lower):
                    return {"is_bug_fix": False, "category": category, "reasoning": f"Tier 3 match: topic '{topic}'"}

    # --- FALLBACK: If no heuristic matches at all ---
    return {"is_bug_fix": False, "category": "Feature/Enhancement", "reasoning": "Heuristic miss, assumed non-bug"}

# CORRECTED LLM Parsing Function
def classify_with_llm(commit_message):
    """
    Calls the LLM for categorization and correctly parses the JSON response.
    """
    prompt = create_unified_prompt(commit_message)
    for attempt in range(3): # Retry loop is good practice
        try:
            response = model.generate_content(prompt)
            # Clean up potential markdown formatting from the LLM response
            json_string = response.text.strip().replace("```json", "").replace("```", "")
            
            # Parse the JSON string into a Python dictionary
            result = json.loads(json_string)
            
            # Extract the category from the dictionary
            category = result.get("category")
            reasoning = result.get("reasoning", "LLM reasoning not provided.")

            # Validate that the extracted category is one of our approved categories
            if category and category in UNIFIED_BUG_CATEGORIES:
                return {"is_bug_fix": True, "category": category, "reasoning": reasoning}
            else:
                print(f"Warning: LLM returned an invalid category: '{category}'")

        except (json.JSONDecodeError, Exception) as e:
            print(f"Warning: LLM response failed (attempt {attempt + 1}). Error: {e}")
            time.sleep(REQUEST_DELAY_SECONDS) # Wait before retrying
            
    # If all retries fail, fallback to the safest, most general category
    return {"is_bug_fix": True, "category": "General Logic Error", "reasoning": "LLM classification failed after retries."}

# --- Main Execution with NEW Logic ---
if __name__ == "__main__":
    repo = Repo(LOCAL_REPO_PATH)
    commits = list(repo.iter_commits())
    df = pd.DataFrame([{"commit_hash": c.hexsha, "message": c.message} for c in tqdm(commits, desc="Extracting Commits")])
    
    if SAMPLE_SIZE: df = df.head(SAMPLE_SIZE)

    full_results = []
    llm_call_count = 0

    print("Starting robust hybrid classification...")
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Classifying Commits"):
        message = row['message']
        commit_hash = row['commit_hash']

        # 1. Try the heuristic classifier first.
        heuristic_result = classify_commit_heuristically(message)
        
        if heuristic_result is not None:
            # Heuristic was confident, use its result.
            result = heuristic_result
        else:
            # Heuristic was unsure (returned None). This is where we call the LLM.
            result = classify_with_llm(message)
            llm_call_count += 1
            time.sleep(REQUEST_DELAY_SECONDS) # Keep the delay for API rate limits
        
        # Combine original commit info with the classification result
        full_results.append({
            "commit_hash": commit_hash,
            "message": message,
            "is_bug_fix": result["is_bug_fix"],
            "category": result["category"],
            "reasoning": result["reasoning"]
        })

    # Create the final DataFrame from the list of dictionaries
    final_df = pd.DataFrame(full_results)
    final_df.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"\nClassification complete. Results saved to '{OUTPUT_CSV_PATH}'")

    # Final Analysis (same as before)
    total_commits = len(final_df)
    heuristic_classifications = total_commits - llm_call_count
    maintenance_commits = final_df[final_df['is_bug_fix'] == True]
    
    print("\n--- Hybrid Classification Performance ---")
    print(f"Total Commits Analyzed: {total_commits}")
    print(f"Handled by Heuristics: {heuristic_classifications} ({heuristic_classifications/total_commits:.2%})")
    print(f"Handled by LLM (for categorization only): {llm_call_count} ({llm_call_count/total_commits:.2%})")

    print("\n--- Maintenance Analysis ---")
    print(f"Maintenance Ratio: {len(maintenance_commits) / total_commits:.2%}")

    if not maintenance_commits.empty:
        print("\n--- Error Profile (Distribution of Bug Fixes) ---")
        dist = maintenance_commits['category'].value_counts(normalize=True) * 100
        profile = pd.DataFrame({'Percentage': dist.map('{:.2f}%'.format)})
        print(profile.to_string())

        print("\n--- Generating 'Other Bug' Dataset for Phase 2 Deep Dive ---")
    other_bug_df = final_df[final_df['category'] == 'Other Bug']

if not other_bug_df.empty:
    other_bug_output_path = "other_bug_review_needed.csv"
    # We only need the message and original hash for the review
    other_bug_df[['commit_hash', 'message']].to_csv(other_bug_output_path, index=False)
    print(f"Successfully saved {len(other_bug_df)} 'Other Bug' commits to '{other_bug_output_path}' for the next phase.")
else:
    print("No 'Other Bug' commits were found.")