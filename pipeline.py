import os
import json
import time
import requests
import pandas as pd
import networkx as nx
from dotenv import load_dotenv


# https://libraries.io/api/ particular language/ library inside langauge
# platform examples PyPI (for Python), npm (for Node.js), Maven (for Java), RubyGems (for Ruby)

load_dotenv()

BASE_URL = "https://libraries.io/api"
BASE_URL_Python = "https://libraries.io/api/pypi"
RAW_DATA_DIR = "data/raw_json"
PACKAGE_LIST_PATH = "packages.txt"
API_KEY = os.getenv('LIBRARIES_IO_API_KEY') # Make sure to set this environment variable

# --- Phase 1: Extract ---
def phase1_extract(packages):
    """
    Fetches dependency data for a list of packages from the Libraries.io API
    and saves each response as a raw JSON file.
    """
    print("--- Starting Phase 1: Extract ---")

    # loop through package text file 
    for i, package_name in enumerate(packages):
        print(f"({i+1}/{len(packages)}) Fetching dependencies for: {package_name}")

        output_path = os.path.join(RAW_DATA_DIR, f"{package_name}.json")

        if os.path.exists(output_path):
            print(f"    -> File already exists. Skipping download.")
            continue # This command immediately jumps to the next iteration of the loop

        # 1. Construct the API request URL and parameters
        # in api documentation in library.io its under Project 
        url = f"{BASE_URL_Python}/{package_name}/latest/dependencies"
        params = {'api_key': API_KEY}

        try:
            # 2. Make the API call
            response = requests.get(url, params=params)

            # 3. Check for errors (e.g., package not found)
            response.raise_for_status() # This will raise an HTTPError for 4xx or 5xx statuses

            # 4. Define the output path and save the data
            output_path = os.path.join(RAW_DATA_DIR, f"{package_name}.json")
            with open(output_path, 'w') as f:
                json.dump(response.json(), f, indent=4)

            print(f"    -> Successfully saved to {output_path}")

        except requests.exceptions.HTTPError as e:
            # Handle cases where the package might not exist on Libraries.io (404)
            if e.response.status_code == 404:
                print(f"    -> ERROR: Package '{package_name}' not found (404). Skipping.")
            else:
                print(f"    -> ERROR: HTTP Error for '{package_name}': {e}")
        except requests.exceptions.RequestException as e:
            # Handle other network-related errors (e.g., DNS failure)
            print(f"    -> ERROR: A network error occurred for '{package_name}': {e}")
        except json.JSONDecodeError:
            # Handle cases where the response isn't valid JSON
            print(f"    -> ERROR: Failed to decode JSON for '{package_name}'.")

        # 5. Be a good API citizen: wait a moment before the next request
        time.sleep(1) # Wait 1 second to avoid hitting rate limits
    print("--- Finished Phase 1: Extract ---")


# --- Phase 2: Transform ---
def phase2_transform():
    """
    Loads raw JSON files, parses them to create a dependency graph,
    and calculates the in-degree centrality for each package.
    """
    print("--- Starting Phase 2: Transform ---")
    edge_list = []
    all_nodes = set() # Using a set is the perfect way to handle this.

    json_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith('.json')]

    for file_name in json_files:
        source_package = file_name.replace('.json', '')
        all_nodes.add(source_package)
        file_path = os.path.join(RAW_DATA_DIR, file_name)

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            # This is the key. We get the top-level 'dependencies' list.
            dependencies = data.get('dependencies', [])

            # Loop through the list just as you said.
            for dep in dependencies:
                # The name of the package is in the 'name' field of each dependency object.
                dependency_package = dep.get('name')
                if dependency_package: # Ensure the name exists
                    edge_list.append((dependency_package, source_package))
                    all_nodes.add(dependency_package)

        except (json.JSONDecodeError, KeyError) as e:
            print(f"    -> WARNING: Could not parse or find keys in {file_name}. Skipping. Error: {e}")

    print(f"    -> Successfully parsed {len(json_files)} files.")
    print(f"    -> Found {len(all_nodes)} total unique packages (nodes).")
    print(f"    -> Created {len(edge_list)} dependency relationships (edges).")

    # Build the graph and calculate centrality
    G = nx.DiGraph()
    G.add_nodes_from(all_nodes)
    G.add_edges_from(edge_list)
    in_centrality = nx.in_degree_centrality(G)
    out_centrality = nx.out_degree_centrality(G)
    print("    -> Calculated both in-degree and out-degree centrality.")

    combined_data = []
    for node in all_nodes:
        combined_data.append({
            'package': node,
            'in_degree_centrality': in_centrality.get(node, 0.0),
            'out_degree_centrality': out_centrality.get(node, 0.0)
        })


    print("--- Finished Phase 2: Transform ---")
    # This function will return the centrality dictionary.
    return combined_data


# --- Phase 3: Load ---
def phase3_load(centrality_data):
    """
    Takes the centrality data, converts it to a pandas DataFrame,
    sorts it, and saves it to a CSV file.
    """
    print("--- Starting Phase 3: Load ---")
    if not centrality_data:
        print("    -> Centrality data is empty. Skipping CSV creation.")
        print("--- Finished Phase 3: Load ---")
        return

    # 2. Convert the dictionary to a pandas DataFrame.
    # The .items() method creates a list of (key, value) pairs,
    # which is a perfect format for creating a two-column DataFrame.
    df = pd.DataFrame(centrality_data)

    # 3. Sort the DataFrame by centrality score, from highest to lowest.
    df_sorted = df.sort_values(by='out_degree_centrality', ascending=False)

    # Let's print the top 5 most central packages as a preview
    print("    -> Top 5 packages by metric:")
    print("    --- Most Foundational (Highest Out-Degree) ---")
    print(df_sorted[['package', 'out_degree_centrality']].head().to_string())
    print("\n    --- Most Complex (Highest In-Degree) ---")
    print(df.sort_values(by='in_degree_centrality', ascending=False)[['package', 'in_degree_centrality']].head().to_string())

    # 4. Define the output path and save the DataFrame to a CSV file.
    # `index=False` is very important; it prevents pandas from writing
    # the DataFrame's row numbers into the file.
    output_path = "analysis_ready_data.csv"
    df_sorted.to_csv(output_path, index=False)

    print(f"\n    -> Successfully saved enhanced analysis data to {output_path}")
    print("--- Finished Phase 3: Load ---")


# --- Main Execution Block ---
def main():
    """
    Orchestrates the execution of the three pipeline phases.
    """

     # 2. LOAD THE .ENV FILE
    # Check for API Key
    if not API_KEY:
        raise ValueError("LIBRARIES_IO_API_KEY environment variable not set.")

    # Create data directory if it doesn't exist
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    # Read the list of packages from the text file
    with open(PACKAGE_LIST_PATH, 'r') as f:
        # Read lines, strip whitespace, and filter out empty lines
        packages_to_query = [line.strip() for line in f if line.strip()]

    # Run the pipeline phases
    phase1_extract(packages_to_query)
    centrality_results = phase2_transform()
    phase3_load(centrality_results)

    print("\nPipeline complete. Output saved to analysis_ready_data.csv")


if __name__ == "__main__":
    main()