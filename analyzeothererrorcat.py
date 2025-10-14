import re
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd

# --- Configuration ---
INPUT_FILE = "other_fix_messages.txt"
N_GRAM_RANGE = (2, 3) # Find 2-word and 3-word phrases
TOP_N_RESULTS = 40

def clean_message(message: str) -> str:
    """Pre-processes a commit message to remove common noise."""
    # Remove SVN paths and revisions
    message = re.sub(r'path trunk revision \d+', '', message, flags=re.IGNORECASE)
    message = re.sub(r'path trunk', '', message, flags=re.IGNORECASE)
    # Remove Bugzilla-style references
    message = re.sub(r'bug\s+\d+', '', message, flags=re.IGNORECASE)
    message = re.sub(r'bugzilla\.gnome\.org', '', message, flags=re.IGNORECASE)
    message = re.sub(r'show_bug\.cgi\?id=\d+', '', message, flags=re.IGNORECASE)
    # Remove "Patch by..." or "Fix by..." attributions
    message = re.sub(r'(patch|fix|fixes|reported)\s+by\s+\w+', '', message, flags=re.IGNORECASE)
    return message

def find_common_phrases(text_data: list):
    """Uses n-gram analysis to find the most common phrases in a list of strings."""
    # Expanded stop words based on our findings
    stop_words = list(CountVectorizer(stop_words='english').get_stop_words())
    stop_words.extend([
        'http', 'https', 'com', 'cve', 'org', 'git', 'svn', 'xml', 'td', 'tr', 'th', 'www',
        'daniel', 'veillard', 'applied', 'patch', 'trunk', 'revision', 'bug', 'cgi',
        'gnome', 'bugzilla', 'id', 'libxml' # Project-specific noise
    ])

    vectorizer = CountVectorizer(
        ngram_range=N_GRAM_RANGE,
        stop_words=stop_words,
        token_pattern=r'\b[a-zA-Z][a-zA-Z-]+\b' # Allow words with hyphens
    ).fit(text_data)

    bag_of_words = vectorizer.transform(text_data)
    sum_words = bag_of_words.sum(axis=0)
    words_freq = [(word, sum_words[0, idx]) for word, idx in vectorizer.vocabulary_.items()]
    words_freq = sorted(words_freq, key=lambda x: x[1], reverse=True)
    return words_freq

# --- Main Execution ---
if __name__ == "__main__":
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            content = f.read()
            messages = content.split('\n---\n')
        
        # Clean the messages before analysis
        cleaned_messages = [clean_message(msg) for msg in messages]
        print(f"Cleaned and processed {len(cleaned_messages)} commit messages.")

        top_phrases = find_common_phrases(cleaned_messages)

        print(f"\n--- Top {TOP_N_RESULTS} Candidate Phrases (After Cleaning) ---")
        for phrase, count in top_phrases[:TOP_N_RESULTS]:
            print(f"{count:<5} | {phrase}")

    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")