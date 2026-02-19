import json
from pathlib import Path
import pandas as pd


def convert_geojson_to_dataframe(path: str) -> pd.DataFrame:
    """
    Convert a GeoJSON FeatureCollection file to a pandas DataFrame.

    Reads a JSON file containing a FeatureCollection, extracts the features array,
    normalizes the nested properties, and returns a flat DataFrame with only the
    property columns (geometry is excluded).

    Args:
        path: File path to the JSON file containing the FeatureCollection

    Returns:
        DataFrame with columns from the properties of each feature
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    features = data.get("features", [])
    # each feature is a dict with "properties" inside
    return pd.json_normalize(features, record_path=None).filter(regex=r"^properties\.").rename(
        columns=lambda c: c.replace("properties.", "")
    )


def load_daily_values_and_locations(dv_path: str, locations_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load daily values and locations data from JSON files into DataFrames.

    Args:
        dv_path: File path to the daily values JSON file
        locations_path: File path to the locations JSON file

    Returns:
        A tuple containing:
            - DataFrame with daily values data
            - DataFrame with locations data
    """
    dv_df = convert_geojson_to_dataframe(dv_path)
    loc_df = convert_geojson_to_dataframe(locations_path)
    return dv_df, loc_df

def join_daily_values_with_locations(dv_df: pd.DataFrame, loc_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join daily values with location information.

    Performs a left join to add location details (agency name, site name, location,
    and site type) to each daily value record. This enriches the measurement data
    with contextual information about where the measurement was taken.

    Args:
        dv_df: DataFrame containing daily values data
        loc_df: DataFrame containing locations data

    Returns:
        DataFrame with daily values joined with selected location columns
    """
    loc_keep = [
        "id",  # required join key
        "agency_name",
        "monitoring_location_name",
        "country_name",
        "state_name",
        "county_name",
        "site_type",
    ]

    loc_filtered = loc_df[loc_keep]

    return dv_df.merge(
        loc_filtered,
        how="left",
        left_on="monitoring_location_id",
        right_on="id",
    )

def load_lookup_tables_as_df(param_codes_path: str, stat_codes_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load parameter codes and statistic codes lookup tables into DataFrames.

    These lookup tables provide descriptions for the codes
    used in the daily values data.

    Args:
        param_codes_path: File path to the parameter codes JSON file
        stat_codes_path: File path to the statistic codes JSON file

    Returns:
        A tuple containing:
            - DataFrame with parameter codes and their descriptions
            - DataFrame with statistic codes and their descriptions
    """
    param_df = convert_geojson_to_dataframe(param_codes_path)
    stat_df = convert_geojson_to_dataframe(stat_codes_path)
    return param_df, stat_df


def save_dataframe(df: pd.DataFrame, output_path: str) -> None:
    """
    Save a DataFrame to CSV, creating parent directories if needed.
    
    Args:
        df: DataFrame to save
        output_path: Path where CSV should be saved
        
    Returns:
        None
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
