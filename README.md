### Data Dictionary 

| Column Name | Data Type | Description & Source |
| :--- | :--- | :--- |
| `package_name` | String | **Primary Key.** The official, normalized name of the package on PyPI. |
| **Structural Metrics** | | *(Calculated via `NetworkX` on the package-to-package dependency graph)* |
| `in_degree_centrality` | Float | The fraction of nodes in the graph that have a direct dependency on this package. Measures importance *within the library ecosystem*. |
| `out_degree_centrality`| Float | The fraction of nodes in the graph that this package directly depends on. Measures a package's own direct dependency complexity. |
| `eigenvector_centrality`| Float | A measure of transitive influence. A high score means a package is depended on by other highly central packages. |
| **Community & Adoption Metrics**| | *(Sourced primarily from the Libraries.io API)* |
| `stars` | Integer | The total number of "stars" the package's repository has on GitHub. A proxy for developer popularity and appreciation. |
| `forks` | Integer | The total number of times the repository has been forked. A proxy for community interest in modification or contribution. |
| `dependent_repos_count`| Integer | **Breadth of Adoption.** The total number of **unique** source code repositories that depend on this package. |
| `dependents_count` | Integer | **Depth of Integration.** The total number of **packages** (including multiple from the same repository) that depend on this package. |
| `libraries_io_rank` | Integer | A proprietary, composite popularity score calculated by Libraries.io. |
| **Project Health & Complexity Metrics**| | *(Sourced from multiple APIs)* |
| `total_code_bytes` | Integer | **Intrinsic Size.** The total size (in bytes) of all `.py` files in the repository. (Source: Google BigQuery). |
| `total_contributions_count`| Integer | **Historical Effort.** The total lifetime contributions (commits, issues, etc.) to the project. (Source: Libraries.io). |
| `commit_activity_last_year`| Integer | **Current Health.** The total number of commits to the main branch in the last 52 weeks. (Source: GitHub Stats API). |
| `bus_factor_1` | Float | **Project Risk.** The percentage of total commits made by the single, top contributor. (Source: GitHub Stats API). |
| **Derived / Engineered Metrics** | | *(Calculated in the final 'Transform' phase of the pipeline)* |
| `integration_ratio` | Float | **Integration Profile.** Calculated as `dependents_count / dependent_repos_count`. A ratio > 1 suggests deep integration within complex monorepos. |
| `intrinsic_replacement_cost`| Float | **Composite Complexity Score.** A weighted formula combining metrics like `total_code_bytes`, `total_contributions_count`, etc. |
| `systemic_impact_value`| Float | **Final Impact Score.** The project's primary novel metric, combining intrinsic cost with network influence (e.g., `intrinsic_replacement_cost * (1 + eigenvector_centrality)`). |

Recent work from Hoffmann et al. (2024) at Harvard Business School has provided a powerful framework for estimating the total economic value of OSS by multiplying a package's creation cost by its total usage. Our project builds upon and extends this research.
We adopt their rigorous COCOMO II model for calculating the creation cost. However, where their work uses a simple usage count to measure demand-side value, our work proposes a more nuanced multiplier: network centrality.
We argue that for questions of systemic risk and identifying critical infrastructure, a package's position within the dependency graph is a more powerful indicator of its importance than raw usage alone. 
Our 'Systemic Impact Value' score, therefore, reframes the question from 'what is the total value saved?' to 'which components pose the greatest cascading failure risk?

#### Delineation of the formula for value estimation leveraging network centrality metrics

**`SIV(package) = IRC(package) * (1 + Eigenvector_Centrality(package))`**

Where:

*   **`SIV(package)`:** The **Systemic Impact Value** of a specific software package. This is your final, high-level output. It represents a package's total importance to the ecosystem, considering both its own complexity and its transitive influence.

*   **`IRC(package)`:** The **Intrinsic Replacement Cost** of that package. This is the "supply-side" value, or the estimated cost to recreate the package from scratch. You will now calculate this rigorously using the method from Hoffmann et al. (2024):
    1.  Get **Lines of Code (LOC)** for the package (e.g., from BigQuery).
    2.  Apply the **COCOMO II formula** to convert LOC into **Person-Months** of effort.
    3.  Multiply Person-Months by a **programmer's average monthly wage** to get a dollar value.

*   **`Eigenvector_Centrality(package)`:** The **Centrality Multiplier**. This is your primary novel contribution. It is a score from 0 to 1 that represents the package's transitive influence within the dependency graph. A high score means it is depended on by other highly depended-on packages.
    *   *(The `+ 1` is a standard technique to ensure that packages with a centrality of 0 still have their own intrinsic value and are not zeroed out by the multiplication).*
