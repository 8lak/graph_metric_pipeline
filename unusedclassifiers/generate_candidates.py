import pandas as pd

# Load your main classified dataset
main_df = pd.read_csv("robust_hybrid_classified.csv")
print(f"Loaded {len(main_df)} commits from main dataset.")

# Load the file with our manual corrections, now including reasoning
changes_df = pd.read_csv("changes_with_reasoning.csv")
print(f"Loaded {len(changes_df)} corrections with reasoning.")

GLE_count = main_df[main_df['category'] == 'General Logic Error'].shape[0]

GLE2_count = changes_df['new_category'].shape[0]
# Use the commit_hash as the index for an efficient update
main_df.set_index('commit_hash', inplace=True)
changes_df.set_index('commit_hash', inplace=True)


print(GLE_count)
print(GLE2_count)
# Update the 'category', 'is_bug_fix', and 'reasoning' columns in the main DataFrame
# based on the corrections in the changes DataFrame

for commit_hash, row in changes_df.iterrows():
    if commit_hash in main_df.index:
        main_df.loc[commit_hash, 'category'] = row['new_category']
        main_df.loc[commit_hash, 'is_bug_fix'] = row['new_is_bug_fix']
        main_df.loc[commit_hash, 'reasoning'] = row['new_reasoning']
    else:
        print(f"Warning: Commit hash {commit_hash} from changes file not found in main dataset.")

# Restore the DataFrame to its original structure
main_df.reset_index(inplace=True)

# Save the final, perfected dataset
final_output_path = "gold_standard_500.csv"
main_df.to_csv(final_output_path, index=False)

print(f"\nMerge complete! Perfected dataset with updated reasoning saved to '{final_output_path}'")
print("\nThis file is now ready for conversion to JSONL for fine-tuning.")
