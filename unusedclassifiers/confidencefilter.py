import pandas as pd
import json
import os

# --- Configuration ---
INPUT_CSV_PATH = "gold_standard_500.csv"
OUTPUT_JSONL_PATH = "final_finetuning_dataset.jsonl" # New name for the final file

# --- Full List of All Categories (for the prompt instructions) ---
ALL_CATEGORIES = [
    "Parser Logic", "Memory", "General Logic Error", "API Logic",
    "Security Vulnerability (CVE)", "Integer", "Error Handling",
    "Concurrency", "Type System", "State Management", "Performance",
    "Incorrect Output/Calculation", "Standard Library Misuse",
    "Build/CI/Tests", "Refactoring", "Documentation",
    "Feature/Enhancement", "Non-Maintenance"
]

def create_final_finetuning_dataset(input_csv: str, output_jsonl: str):
    """
    Reads the gold-standard CSV and converts it into the final 'contents'
    JSONL format required by the Gemini fine-tuning platform.
    """
    if not os.path.exists(input_csv):
        exit(f"Error: Input file not found at '{input_csv}'.")

    print(f"Reading gold-standard data from '{input_csv}'...")
    df = pd.read_csv(input_csv)

    print(f"Preparing {len(df)} examples in the final 'contents' format...")
    
    # Create the user prompt template.
    prompt_template = (
        "You are a world-class software engineering analyst specializing in the libxml2 library. "
        "Analyze the following commit message and classify it. Respond ONLY with a valid JSON object "
        "containing three keys: a boolean 'is_bug_fix', the string 'category', and a one-sentence 'reasoning'.\n\n"
        f"AVAILABLE CATEGORIES:\n{json.dumps(ALL_CATEGORIES)}\n\n"
        "--- COMMIT MESSAGE ---\n{commit_message}"
    )

    with open(output_jsonl, 'w', encoding='utf-8') as f:
        for index, row in df.iterrows():
            commit_message = row['message']
            is_bug = bool(row['is_bug_fix'])
            
            # This is the 'ground truth' JSON the model must learn to output.
            assistant_output_dict = {
                "is_bug_fix": is_bug,
                "category": row['category'],
                "reasoning": row['reasoning']
            }
            
            # This is the final structure for a single training example,
            # mirroring the Gemini API 'GenerateContent' request format.
            training_example = {
                "contents": [
                    {
                        "role": "user",
                        "parts": [{"text": prompt_template.format(commit_message=commit_message)}]
                    },
                    {
                        "role": "model",  # Use 'model' for the assistant's role
                        "parts": [{"text": json.dumps(assistant_output_dict)}]
                    }
                ]
            }
            
            # Write the complete JSON object as a single line in the file.
            f.write(json.dumps(training_example) + '\n')

    print("\n--- Final Preparation Complete ---")
    print(f"Successfully prepared {len(df)} examples for fine-tuning.")
    print(f"Data saved to '{output_jsonl}'.")

# --- Main Execution ---
if __name__ == "__main__":
    create_final_finetuning_dataset(INPUT_CSV_PATH, OUTPUT_JSONL_PATH)