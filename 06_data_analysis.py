import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# --- 1. Configuration: Update these file paths ---
CLASSIFIED_DATA_PATH = 'FINAL_CLASSIFIED_FULL_DATA/c/fully_classified.csv'
# Replace with the actual path to your full commit data CSV
FULL_COMMIT_DATA_PATH = 'full_commit_with_author_data/full_commit_libxml2.csv' 
OUTPUT_DIR = 'visualizations'

# --- Create output directory if it doesn't exist ---
if not os.path.exists(OUTPUT_DIR):
    print(f"Creating output directory: {OUTPUT_DIR}")
    os.makedirs(OUTPUT_DIR)

# --- 2. Load the Datasets ---
print("Loading data...")
try:
    classified_df = pd.read_csv(CLASSIFIED_DATA_PATH)
    commits_df = pd.read_csv(FULL_COMMIT_DATA_PATH)
    print("Data loaded successfully.")
except FileNotFoundError as e:
    print(f"Error loading files: {e}")
    print("Please make sure the file paths in the 'Configuration' section are correct.")
    exit()

# --- 3. Prepare and Merge the Data ---
print("Preparing and merging datasets...")
classified_df.rename(columns={'key': 'commit_id'}, inplace=True)
merged_df = pd.merge(commits_df, classified_df, on='commit_id', how='left')

# --- 4. Select and Clean the Final Columns ---
final_df = merged_df[['is_bug_fix', 'category', 'authored_datetime']].copy()
final_df.dropna(subset=['category', 'authored_datetime'], inplace=True)
print(f"Merge complete. Shape of the final cleaned data: {final_df.shape}")


# --- 5. Create Yearly Time Steps ---
print("Creating yearly time steps...")
final_df['authored_datetime'] = pd.to_datetime(final_df['authored_datetime'], errors='coerce', utc=True)
final_df.dropna(subset=['authored_datetime'], inplace=True)

# <-- THIS IS THE PRIMARY CHANGE HERE
final_df['year'] = final_df['authored_datetime'].dt.to_period('Y').astype(str)


# --- 6. Prepare Data for Stacked Bar Chart ---
print("Reshaping data for visualization...")

top_categories = final_df['category'].value_counts().nlargest(10).index
final_df['category_plot'] = final_df['category'].apply(lambda x: x if x in top_categories else 'Other')

# <-- UPDATED TO GROUP BY 'year'
pivot_df = final_df.groupby(['year', 'category_plot']).size().unstack(fill_value=0)

if 'Other' in pivot_df.columns:
    pivot_df = pivot_df[[col for col in pivot_df.columns if col != 'Other'] + ['Other']]

print("Data successfully pivoted. Preview:")
print(pivot_df.tail())


# --- 7. Plot 1: Standard Stacked Bar Chart (Counts) ---
print("\nGenerating standard stacked bar chart...")
fig, ax = plt.subplots(figsize=(20, 10))
pivot_df.plot(
    kind='bar', 
    stacked=True, 
    ax=ax,
    colormap='viridis'
)

# <-- UPDATED TITLES AND LABELS
ax.set_title('Yearly Distribution of Commit Categories', fontsize=18)
ax.set_xlabel('Year', fontsize=14)
ax.set_ylabel('Number of Commits', fontsize=14)
ax.tick_params(axis='x', rotation=45, labelsize=12) # Increased label size for readability
ax.grid(axis='y', linestyle='--', alpha=0.7)
ax.legend(title='Category', bbox_to_anchor=(1.02, 1), loc='upper left')

plt.tight_layout()
# <-- UPDATED FILENAME
plt.savefig(os.path.join(OUTPUT_DIR, 'yearly_commit_distribution_counts.png'))
plt.close()
print("Saved: yearly_commit_distribution_counts.png")


# --- 8. Plot 2: 100% Stacked Bar Chart (Proportions) ---
print("\nGenerating 100% stacked bar chart...")
pivot_normalized_df = pivot_df.div(pivot_df.sum(axis=1), axis=0) * 100

fig, ax = plt.subplots(figsize=(20, 10))
pivot_normalized_df.plot(
    kind='bar', 
    stacked=True, 
    ax=ax,
    colormap='viridis'
)

# <-- UPDATED TITLES AND LABELS
ax.set_title('Yearly Proportion of Commit Categories', fontsize=18)
ax.set_xlabel('Year', fontsize=14)
ax.set_ylabel('Percentage of Commits (%)', fontsize=14)
ax.tick_params(axis='x', rotation=45, labelsize=12)
ax.legend(title='Category', bbox_to_anchor=(1.02, 1), loc='upper left')

plt.tight_layout()
# <-- UPDATED FILENAME
plt.savefig(os.path.join(OUTPUT_DIR, 'yearly_commit_distribution_proportions.png'))
plt.close()
print("Saved: yearly_commit_distribution_proportions.png")


print("\nAll visualizations have been saved to the 'visualizations' directory.")
print("Script finished.")