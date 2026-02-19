import pandas as pd


def explore_raw_data(joined_df):
    """
    Perform initial exploratory data analysis on the joined dataset.

    Displays basic information about the dataset including shape, columns,
    missing values analysis, and site-level summaries. This helps identify
    data quality issues before cleaning.

    Args:
        joined_df: DataFrame with daily values joined with location data

    Returns:
        None. Prints analysis results to console
    """
    # Get basic information
    # print("\n--- BASIC INFO (info) ---")
    # joined_df.info()

    # print("\n--- SHAPE (rows, cols) ---")
    # print(joined_df.shape)

    # print("\n--- COLUMNS ---")
    # print(list(joined_df.columns))

    # print("\n--- HEAD (first 5 rows) ---")
    # print(joined_df.head())

    # Missing values table (count and %)
    missing_table = pd.DataFrame({
        "missing_count": joined_df.isnull().sum(),
        "missing_pct": (joined_df.isnull().sum() / len(joined_df) * 100).round(2)
    }).sort_values("missing_count", ascending=False)

    # print("\n--- MISSING VALUES (count and %) ---")
    # print(missing_table)

    # Unique monitoring locations
    # print("\n--- UNIQUE MONITORING LOCATIONS ---")
    unique_sites = joined_df["monitoring_location_id"].nunique()
    # print("Unique monitoring locations:", unique_sites)

    tmp = joined_df.copy()

    # Convert value to numeric
    tmp["value_num"] = pd.to_numeric(tmp["value"], errors="coerce")

    site_summary = (
        tmp.groupby("monitoring_location_id")["value_num"]
        .agg(
            total_rows="size",
            total_non_null_obs="count",
            missing_value_count=lambda x: x.isnull().sum(),
            zero_value_count=lambda x: (x == 0).sum(),
        )
        .reset_index()
    )

    site_summary["missing_pct"] = (site_summary["missing_value_count"] / site_summary["total_rows"] * 100).round(2)

    site_summary["zero_pct"] = (site_summary["zero_value_count"] / site_summary["total_non_null_obs"] * 100).round(2)

    # print("\n--- Sites summary (real obs , zero count and zero %) ---")
    # print(site_summary.sort_values("zero_value_count", ascending=False).head(20))


def clean_and_transform_data(joined_df: pd.DataFrame, param_df: pd.DataFrame, stat_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform the joined dataset for analysis.

    Performs the following operations:
    - Converts time and last_modified columns from strings to datetime objects
    - Converts value column from string to numeric
    - Removes rows with missing values
    - Drops the qualifier column (82.7 percent missing)
    - Joins parameter names and statistic descriptions from lookup tables
    - Selects and orders final columns for the cleaned dataset

    Args:
        joined_df: DataFrame with daily values joined with location data
        param_df: DataFrame with parameter code lookup table
        stat_df: DataFrame with statistic code lookup table

    Returns:
        Cleaned DataFrame ready for analysis with proper data types and enriched columns
    """
    df = joined_df.copy()

    # Parse dates
    df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
    df["last_modified"] = pd.to_datetime(df["last_modified"], errors="coerce", utc=True)

    # Convert value to numeric
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Drop rows with missing value
    # print("Rows before drop:", len(df))
    df = df.dropna(subset=["value"]).reset_index(drop=True)
    # print("Rows after drop:", len(df))

    # Drop qualifier column (if present)
    if "qualifier" in df.columns:
        df = df.drop(columns=["qualifier"])

    # ---- Join Parameter Name  ----
    param_small = param_df[["id", "parameter_name"]].drop_duplicates()
    df = df.merge(param_small, how="left", left_on="parameter_code", right_on="id")
    df = df.drop(columns=["id"])  # drop lookup id after merge

    # ---- Join Statistic Description  ----
    stat_small = stat_df[["id", "statistic_description"]].drop_duplicates()
    df = df.merge(stat_small, how="left", left_on="statistic_id", right_on="id")
    df = df.drop(columns=["id"])  # drop lookup id after merge

    # Rename id_x to id
    df = df.rename(columns={"id_x": "id"})

    # Select and order final columns (drop everything else)
    final_cols = [
        "id",
        "monitoring_location_id",
        "monitoring_location_name",
        "site_type",
        "time",
        "last_modified",
        "parameter_code",
        "parameter_name",
        "value",
        "unit_of_measure",
        "statistic_id",
        "statistic_description",
        "approval_status",
        "agency_name",
        "state_name",
        "county_name",
    ]

    df = df[final_cols]

    # df.info()
    return df

def produce_summary_by_site(cleaned_df: pd.DataFrame) -> None:
    """
    Generate and print a summary report for each monitoring site and parameter.
    
    Calculates key statistics for each site and parameter combination including
    total observations, min/max/average values, most recent measurement time,
    and most recent value. Results are sorted by site and parameter.
    
    Args:
        cleaned_df: DataFrame with cleaned daily values data
        
    Returns:
        None. Prints the summary table to console
    """
    print("\n" + "="*130)
    print("SUMMARY BY SITE AND PARAMETER".center(130))
    print("="*130 + "\n")

    # Make sure time is datetime (should already be from clean_and_transform_data)
    df = cleaned_df.copy()

    summary = (
        df.groupby(["monitoring_location_name", "parameter_name"], as_index=False)
        .agg(
            observations=("value", "count"),
            min_value=("value", "min"),
            max_value=("value", "max"),
            avg_value=("value", "mean"),
            most_recent_time=("time", "max"),
        )
    )

    # Get the most recent value per site and parameter
    recent_rows = df.sort_values("time").groupby(["monitoring_location_name", "parameter_name"]).tail(1)
    recent_values = recent_rows[["monitoring_location_name", "parameter_name", "value", "unit_of_measure"]].rename(
        columns={"value": "most_recent_value"}
    )

    summary = summary.merge(recent_values, on=["monitoring_location_name", "parameter_name"], how="left")

    # Round values for readability
    summary["avg_value"] = summary["avg_value"].round(2)
    summary["min_value"] = summary["min_value"].round(2)
    summary["max_value"] = summary["max_value"].round(2)
    summary["most_recent_value"] = summary["most_recent_value"].round(2)

    # Format the date column (it's already datetime)
    summary["most_recent_time"] = summary["most_recent_time"].dt.strftime("%Y-%m-%d")

    # Sort by site name and parameter
    summary = summary.sort_values(["monitoring_location_name", "parameter_name"])

    # Shorten site names for better display
    summary["monitoring_location_name"] = summary["monitoring_location_name"].str.replace(", NM", "")
    
    # Rename columns for prettier display
    summary.columns = [
        "Site",
        "Parameter",
        "Obs",
        "Min",
        "Max",
        "Avg",
        "Recent Date",
        "Recent",
        "Unit"
    ]
    
    # Reorder columns
    summary = summary[[
        "Site",
        "Parameter",
        "Unit",
        "Obs",
        "Min",
        "Max",
        "Avg",
        "Recent",
        "Recent Date"
    ]]

    # Set pandas display options for better formatting
    pd.set_option('display.max_colwidth', 45)
    pd.set_option('display.width', 130)
    
    # Print using pandas to_string with better formatting
    print(summary.to_string(index=False))
    
    print("\n" + "="*130)
    print(f"Total Sites: {summary['Site'].nunique()} | "
          f"Total Parameters: {summary['Parameter'].nunique()} | "
          f"Total Observations: {summary['Obs'].sum()}")
    print("="*130 + "\n")
    
    # Reset pandas display options
    pd.reset_option('display.max_colwidth')
    pd.reset_option('display.width')
