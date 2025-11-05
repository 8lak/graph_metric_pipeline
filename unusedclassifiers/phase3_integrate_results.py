import pandas as pd

# --- Configuration ---

# 1. INPUT FILES
MAIN_DATASET_PATH = "robust_hybrid_classified.csv"
DEEP_DIVE_RESULTS_PATH = "other_bug_deep_dive_results.csv"

# 2. OUTPUT FILE
FINAL_DATASET_PATH = "final_classified_dataset.csv"

def merge_results(main_path, deep_dive_path, final_path):
    """
    Merges the deep dive results back into the main dataset.
    """
    try:
        # Load both datasets
        main_df = pd.read_csv(main_path)
        deep_dive_df = pd.read_csv(deep_dive_path)
        print(f"Loaded {len(main_df)} records from the main dataset.")
        print(f"Loaded {len(deep_dive_df)} refined records from the deep dive.")
    except FileNotFoundError as e:
        exit(f"Error: Could not find a required input file. {e}")

    # --- The Core Logic ---
    # For efficient merging, we'll set the commit_hash as the index.
    # This is much faster than iterating through rows.
    main_df.set_index('commit_hash', inplace=True)
    deep_dive_df.set_index('commit_hash', inplace=True)

    # The 'update' method is perfect for this. It modifies main_df in place.
    # For every commit_hash that exists in deep_dive_df, it will update the
    # columns in main_df with the values from deep_dive_df.
    
    # We only want to update the 'category' and 'reasoning' columns.
    updates_to_apply = deep_dive_df[['new_category', 'reasoning']].rename(columns={'new_category': 'category'})
    
    # Before updating, let's see how many 'Other Bug' we have
    

    main_df.update(updates_to_apply)
    
    # After updating, let's verify
    final_other_bug_count = main_df[main_df['category'] == 'Other Bug'].shape[0]

    # Reset the index to turn commit_hash back into a column
    main_df.reset_index(inplace=True)

    # Save the final, merged dataset
    main_df.to_csv(final_path, index=False)
    
    print("\nMerge complete.")
    print(f"Updated {final_other_bug_count} 'Other Bug' records with specific labels.")
    
    # This assertion is a good sanity check
    if 'General Logic Error' in main_df['category'].unique():
        print("Verification successful: New 'General Logic Error' category is present.")
    
    print(f"\nFinal, high-quality dataset saved to '{final_path}'.")

    return main_df

# --- Main Execution ---
if __name__ == "__main__":
    final_df = merge_results(MAIN_DATASET_PATH, DEEP_DIVE_RESULTS_PATH, FINAL_DATASET_PATH)

    # Display the final, refined error profile
    print("\n--- Final, Refined Error Profile ---")
    
    maintenance_commits = final_df[final_df['is_bug_fix'] == True]
    
    if not maintenance_commits.empty:
        dist = maintenance_commits['category'].value_counts(normalize=True) * 100
        profile = pd.DataFrame({'Percentage': dist.map('{:.2f}%'.format)})
        print(profile.to_string())
    else:
        print("No maintenance commits found in the final dataset.")