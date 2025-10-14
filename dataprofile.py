import pandas as pd

# --- Configuration ---
IN_CSV_PATH = "fine_tune_test.csv" 
OUT_CSV_PATH = "error_profile_distribution.csv"

# 1. Load the dataset
try:
    final_df = pd.read_csv(IN_CSV_PATH)
except FileNotFoundError:
    exit(f"Error: Input file not found at '{IN_CSV_PATH}'.")

# 2. Filter for only the maintenance commits (bug fixes)
maintenance_commits = final_df[final_df['is_bug_fix'] == True]

if not maintenance_commits.empty:
    print("\n--- Error Profile (Distribution of Bug Fixes) ---")
    
    # 3. Calculate the percentage distribution
    dist = maintenance_commits['category'].value_counts(normalize=True) * 100
    
    # 4. Create the initial DataFrame
    profile = pd.DataFrame({'Percentage': dist})
    
    # --- THIS IS THE KEY FIX ---
    # 5. Reset the index to turn the category names from an index into a column.
    profile.reset_index(inplace=True)
    
    # 6. (Optional but good practice) Rename the new column from 'index' to 'Category'.
    profile.rename(columns={'index': 'Category'}, inplace=True)
    
    # 7. Format the 'Percentage' column for readability
    profile['Percentage'] = profile['Percentage'].map('{:.2f}%'.format)

    # 8. Save the corrected DataFrame to a new CSV file.
    #    This time, we DO want the index in the CSV, so we don't set index=False.
    #    Actually, since we have a 'Category' column, we don't need pandas' default index.
    profile.to_csv(OUT_CSV_PATH, index=False)
    
    # 9. Print the final, well-formatted table to the console.
    print(profile.to_string(index=False))
    
    print(f"\nSuccessfully saved the distribution to '{OUT_CSV_PATH}'.")

else:
    print("No maintenance commits (is_bug_fix == True) were found in the input file.")