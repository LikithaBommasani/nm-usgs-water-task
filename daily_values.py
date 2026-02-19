import os
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
import requests
from dotenv import load_dotenv 


def calculate_time_range() -> str:
    """
    Calculate a time range string for the last N days (configured in .env).

    Creates a time range in ISO 8601 format from N days ago until now,
    both in UTC timezone. This format is required by the USGS API.

    Returns:
        String in format "YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ"
        representing the start and end of the time period
    """
    load_dotenv()
    days = int(os.getenv("DV_TIME_RANGE_DAYS"))

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)
    start = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"{start}/{end}"


def extract_location_ids(locations_json: dict) -> list[str]:
    """
    Extract monitoring location IDs from a GeoJSON FeatureCollection.

    Searches for IDs in both the properties object and at the feature level.
    Removes duplicates while preserving the original order.

    Args:
        locations_json: Dictionary containing a GeoJSON FeatureCollection

    Returns:
        List of unique monitoring location IDs as strings
    """
    features = locations_json.get("features", [])
    ids: list[str] = []

    for f in features:
        props = f.get("properties", {})
        if isinstance(props, dict) and props.get("id"):
            ids.append(str(props["id"]))
        elif f.get("id"):
            ids.append(str(f["id"]))

    # de-dupe, preserve order
    seen = set()
    out = []
    for x in ids:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def chunk_list(items: list[str], chunk_size: int) -> list[list[str]]:
    """
    Split a list into smaller chunks of a specified size.
    
    This is used to batch API requests since the USGS API has limits on
    how many location IDs can be requested at once.
    
    Args:
        items: List of items to split into chunks
        chunk_size: Maximum number of items per chunk
        
    Returns:
        List of lists, where each inner list contains up to chunk_size items
    """
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def fetch_daily_values_for_locations(locations_json: dict) -> dict:
    """
    Fetch daily values data for all monitoring locations from USGS API.

    Extracts location IDs from the locations JSON, splits them into batches,
    and makes multiple API requests to fetch daily values for the last N days.
    All results are combined into a single FeatureCollection.

    Args:
        locations_json: Dictionary containing location data with monitoring location IDs

    Returns:
        Dictionary containing a GeoJSON FeatureCollection with all daily values

    Raises:
        RuntimeError: If required environment variables are missing or invalid
    """
    load_dotenv()

    dv_base_url = os.getenv("USGS_DV_BASE_URL")
    if not dv_base_url:
        raise RuntimeError("Missing USGS_DV_BASE_URL in .env")

    raw_params = os.getenv("DV_QUERY_PARAMS")
    if not raw_params:
        raise RuntimeError("Missing DV_QUERY_PARAMS in .env (must be JSON)")

    try:
        dv_params_base = json.loads(raw_params)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"DV_QUERY_PARAMS is not valid JSON: {e}") from e

    batch_size = int(os.getenv("DV_BATCH_SIZE"))
    timeout = int(os.getenv("API_TIMEOUT_SECONDS"))

    ids = extract_location_ids(locations_json)
    if not ids:
        raise RuntimeError("No monitoring location IDs found in locations JSON")

    time_range = calculate_time_range()

    all_features = []
    first_page_meta = None

    batches = chunk_list(ids, batch_size)
    # print(f"DV batching: {len(ids)} IDs -> {len(batches)} batches (batch size={batch_size})")
    # print(f"DV time range: {time_range}")

    for idx, batch in enumerate(batches, start=1):
        dv_params = dict(dv_params_base)
        dv_params["monitoring_location_id"] = ",".join(batch)
        dv_params["time"] = time_range

        r = requests.get(dv_base_url, params=dv_params, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        if first_page_meta is None:
            # keep everything except features as "meta"
            first_page_meta = {k: v for k, v in data.items() if k != "features"}

        features = data.get("features", [])
        if isinstance(features, list):
            all_features.extend(features)

        # print(f"[{idx}/{len(batches)}] Batch features: {len(features) if isinstance(features, list) else 0}")

    combined = {
        **(first_page_meta or {}),
        "features": all_features,
        "numberReturned": len(all_features),
        "total_features": len(all_features),
        "batch_size": batch_size,
        "batches": len(batches),
        "time_range": time_range,
    }

    return combined


def save_daily_values(data: dict) -> Path:
    """
    Save daily values data to a JSON file.

    Creates the output directory if it does not exist and writes the data
    as formatted JSON with 2-space indentation.

    Args:
        data: Dictionary containing the daily values data to save

    Returns:
        Path object pointing to the saved JSON file
    """
    load_dotenv()

    output_file = os.getenv("DV_OUTPUT_FILE")
    out_path = Path(output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return out_path
