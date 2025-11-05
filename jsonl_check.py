import json

def validate_gemini_jsonl(file_path):
    line_num = 0
    with open(file_path, 'r') as f:
        for line in f:
            line_num += 1
            try:
                data = json.loads(line)
                # Check top-level keys
                if "contents" not in data:
                    print(f"Error on line {line_num}: Missing 'contents' key.")
                    continue
                # Check contents structure (simplified)
                if not isinstance(data["contents"], list) or not data["contents"]:
                     print(f"Error on line {line_num}: 'contents' must be a non-empty list.")
                     continue
                # Add more specific checks here if needed (e.g., role="user", parts, text)
                
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line {line_num}: {e}")
            except Exception as e:
                print(f"An unexpected error occurred on line {line_num}: {e}")
    print("Validation finished.")

# Replace 'your_input_file.jsonl' with the path to your file
validate_gemini_jsonl('full_commit_jsonl/FULL_commit_toclassify.jsonl')



