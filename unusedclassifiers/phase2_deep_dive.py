import pandas as pd
import json
import os

# --- Configuration ---
INPUT_CSV_PATH = "gold_standard_500.csv"
OUTPUT_JSONL_PATH = "unified_dataset.jsonl" # Renamed for clarity

# --- Full List of All Categories (for the prompt instructions) ---
# This ensures the model knows its full range of possible answers.
ALL_CATEGORIES = [
    # Bug-Fix Categories
    "Parser Logic", "Memory", "General Logic Error", "API Logic",
    "Security Vulnerability (CVE)", "Integer", "Error Handling",
    "Concurrency", "Type System", "State Management", "Performance",
    "Incorrect Output/Calculation", "Standard Library Misuse",
    # Non-Bug-Fix Categories
    "Build/CI/Tests", "Refactoring", "Documentation",
    "Feature/Enhancement", "Non-Maintenance"
]

def create_finetuning_dataset(input_csv: str, output_jsonl: str):
    """
    Reads the complete gold-standard dataset and converts it into the
    unified 'messages' JSONL format required for robust fine-tuning.
    """
    if not os.path.exists(input_csv):
        exit(f"Error: Input file not found at '{input_csv}'.")

    print(f"Reading gold-standard data from '{input_csv}'...")
    df = pd.read_csv(input_csv)

    # --- No need to filter! We want the model to learn from ALL examples. ---

    # Validate that the necessary columns are present.
    required_columns = ['message', 'is_bug_fix', 'category', 'reasoning']
    if not all(col in df.columns for col in required_columns):
        exit(f"Error: The input CSV is missing one of the required columns: {required_columns}")

    print(f"Preparing {len(df)} examples for fine-tuning...")
    
    # Create a list to hold all our final JSON objects
    finetuning_examples = []
    
    # Create the user prompt template. This is consistent for all examples.
    # We include the full list of categories to guide the model.
    prompt_template = (
        "You are a world-class software engineering analyst specializing in the libxml2 library. "
        "Analyze the following commit message and classify it. Respond ONLY with a valid JSON object "
        "containing three keys: a boolean 'is_bug_fix', the string 'category', and a one-sentence 'reasoning'.\n\n"
        f"AVAILABLE CATEGORIES:\n{json.dumps(ALL_CATEGORIES)}\n\n"
        "--- COMMIT MESSAGE ---\n{commit_message}"
    )

    for index, row in df.iterrows():
        commit_message = row['message']
        
        # Ensure the 'is_bug_fix' column is a proper boolean, not a string
        is_bug = bool(row['is_bug_fix'])
        
        # This is the 'ground truth' that the model must learn to replicate.
        assistant_output = {
            "is_bug_fix": is_bug,
            "category": row['category'],
            "reasoning": row['reasoning']
        }

        # This is the final structure for a single training example.
        example = {
            "messages": [
                {
                    "role": "user",
                    "content": prompt_template.format(commit_message=commit_message)
                },
                {
                    "role": "assistant",
                    "content": json.dumps(assistant_output) # Convert the dictionary to a JSON string
                }
            ]
        }
        finetuning_examples.append(example)

    # Write all the examples to the .jsonl file.
    print(f"Writing data to '{output_jsonl}'...")
    with open(output_jsonl, 'w', encoding='utf-8') as f:
        for entry in finetuning_examples:
            f.write(json.dumps(entry) + '\n')

    print("\n--- Preparation Complete ---")
    print(f"Successfully prepared {len(finetuning_examples)} examples for fine-tuning.")
    print(f"Data saved to '{output_jsonl}'.")
    print("\nNext Step: Upload this file to your chosen fine-tuning platform (e.g., Google AI Studio) to train your model.")

# --- Main Execution ---
if __name__ == "__main__":
    create_finetuning_dataset(INPUT_CSV_PATH, OUTPUT_JSONL_PATH)