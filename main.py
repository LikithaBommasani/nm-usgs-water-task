from lookups import get_locations, get_parameter_codes, get_statistic_codes
from daily_values import fetch_daily_values_for_locations, save_daily_values
from data_loader import load_daily_values_and_locations, join_daily_values_with_locations, load_lookup_tables_as_df, save_dataframe
from eda import explore_raw_data, clean_and_transform_data, produce_summary_by_site
from plots import plot_discharge_and_temperature, plot_temperature_vs_discharge_scatter
import os
from dotenv import load_dotenv


def main() -> None:
    """
    Main pipeline for fetching, processing, and analyzing USGS water data.
    
    This function orchestrates the complete workflow:
    1. Fetch monitoring locations and lookup tables from USGS API
    2. Fetch daily values for all locations (last 90 days)
    3. Load data into pandas DataFrames
    4. Join daily values with location information
    5. Perform exploratory data analysis
    6. Clean and transform the data
    7. Generate summary statistics by site
    
    All intermediate and final results are saved to the outputs folder.
    """
    # Load environment variables
    load_dotenv()
  
    # 1) Fetch and save locations
    locations_json, loc_path = get_locations()

    # 1b) Fetch and save lookup tables (cached)
    param_path = get_parameter_codes()
    stat_path = get_statistic_codes()


    # 2) Fetch and save daily values (JSON)
    dv_json = fetch_daily_values_for_locations(locations_json)
    dv_path = save_daily_values(dv_json)
    # print(f"Saved DV -> {dv_path}")

    # 3) Load both into pandas
    dv_df, loc_df = load_daily_values_and_locations(str(dv_path), str(loc_path))
    # print(f"DV df shape: {dv_df.shape}")
    # print(f"Locations df shape: {loc_df.shape}")

    param_df, stat_df = load_lookup_tables_as_df(str(param_path), str(stat_path))
    # print(f"Parameter codes df shape: {param_df.shape}")
    # print(f"Statistic codes df shape: {stat_df.shape}")

    # Get output file paths from environment variables
    output_dv_df = os.getenv("OUTPUT_DV_DATAFRAME")
    output_loc_df = os.getenv("OUTPUT_LOCATIONS_DATAFRAME")
    output_joined_df = os.getenv("OUTPUT_JOINED_DATAFRAME")
    output_cleaned_df = os.getenv("OUTPUT_CLEANED_DATAFRAME")

    # 4) Save as CSV
    save_dataframe(dv_df, output_dv_df)
    save_dataframe(loc_df, output_loc_df)

    # print(f"Saved DV dataframe -> {output_dv_df}")
    # print(f"Saved Locations dataframe -> {output_loc_df}")

    joined_df = join_daily_values_with_locations(dv_df, loc_df)
    save_dataframe(joined_df, output_joined_df)
    # print(f"Saved joined dataframe -> {output_joined_df}")

    # explore_raw_data(joined_df)

    parsed_df = clean_and_transform_data(joined_df, param_df, stat_df)
    # print(parsed_df[["time", "last_modified"]].head())
    # print(parsed_df[["time", "last_modified"]].dtypes)

    save_dataframe(parsed_df, output_cleaned_df)
    print(f"Saved cleaned dataset -> {output_cleaned_df}")
    
    produce_summary_by_site(parsed_df)
    
    # 5) Generate visualizations
    # print("\n--- Generating Visualizations ---")
    plot_discharge_and_temperature(parsed_df)
    plot_temperature_vs_discharge_scatter(parsed_df)
    # print("All visualizations saved to outputs/ directory")
  

if __name__ == "__main__":
    main()
