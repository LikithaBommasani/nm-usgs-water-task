import os
import json
from pathlib import Path

import requests
from dotenv import load_dotenv


def _load_json_env(env_key: str) -> dict:
    """
    Load and parse a JSON string from an environment variable.
    
    Args:
        env_key: The name of the environment variable to read
        
    Returns:
        Parsed JSON as a dictionary
        
    Raises:
        RuntimeError: If the environment variable is missing or contains invalid JSON
    """
    raw = os.getenv(env_key)
    if not raw:
        raise RuntimeError(f"Missing {env_key} in .env ")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"{env_key} is not valid JSON: {e}") from e


def get_or_fetch_json(base_url_env: str, params_env: str, output_env: str, refresh: bool = False) -> Path:
    """
    Fetch data from USGS API with caching support.
    
    This function checks if cached data exists. If it does and refresh is False,
    it returns the cached file path. Otherwise, it fetches fresh data from the API
    and saves it to the cache file.
    
    Args:
        base_url_env: Environment variable name containing the API base URL
        params_env: Environment variable name containing query parameters as JSON
        output_env: Environment variable name containing the output file path
        refresh: If True, fetch fresh data even if cache exists. Default is False
        
    Returns:
        Path object pointing to the cached JSON file
        
    Raises:
        RuntimeError: If required environment variables are missing
    """
    load_dotenv()

    base_url = os.getenv(base_url_env)
    if not base_url:
        raise RuntimeError(f"Missing {base_url_env} in .env")

    out_file = os.getenv(output_env)
    if not out_file:
        raise RuntimeError(f"Missing {output_env} in .env")

    out_path = Path(out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and not refresh:
        return out_path

    params = _load_json_env(params_env)
    timeout = int(os.getenv("API_TIMEOUT_SECONDS"))

    r = requests.get(base_url, params=params, timeout=timeout)
    r.raise_for_status()

    out_path.write_text(json.dumps(r.json(), indent=2), encoding="utf-8")
    return out_path


# ---------- Locations  ----------
def get_locations() -> tuple[dict, Path]:
    """
    Fetch monitoring location data from USGS API.
    
    Retrieves information about water monitoring stations including their IDs,
    names, geographic locations, and site types.
    
    Refresh setting is read from REFRESH_LOCATIONS in .env file.
        
    Returns:
        A tuple containing:
            - Dictionary with the parsed JSON data
            - Path object pointing to the cached file
    """
    load_dotenv()
    
    # Get refresh setting from .env
    refresh = os.getenv("REFRESH_LOCATIONS").lower() == "true"
    
    path = get_or_fetch_json(
        base_url_env="USGS_LOCATIONS_BASE_URL",
        params_env="QUERY_PARAMS",
        output_env="OUTPUT_FILE",
        refresh=refresh,
    )
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return data, path




# ---------- Parameter Codes ----------
def get_parameter_codes() -> Path:
    """
    Fetch parameter code lookup table from USGS API.
    
    Parameter codes identify what is being measured, such as discharge,
    temperature, or gage height. This function retrieves the mapping
    between parameter codes and their descriptive names.
    
    Refresh setting is read from REFRESH_PARAMETER_CODES in .env file.
        
    Returns:
        Path object pointing to the cached parameter codes JSON file
    """
    load_dotenv()
    
    # Get refresh setting from .env
    refresh = os.getenv("REFRESH_PARAMETER_CODES").lower() == "true"
    
    return get_or_fetch_json(
        base_url_env="USGS_PARAMETER_CODES_BASE_URL",
        params_env="PARAM_CODES_QUERY_PARAMS",
        output_env="PARAM_CODES_OUTPUT_FILE",
        refresh=refresh,
    )


# ---------- Statistic Codes ----------
def get_statistic_codes() -> Path:
    """
    Fetch statistic code lookup table from USGS API.
    
    Statistic codes describe how the data was calculated, such as mean,
    maximum, or minimum. This function retrieves the mapping
    between statistic codes and their descriptions.
    
    Refresh setting is read from REFRESH_STATISTIC_CODES in .env file.
        
    Returns:
        Path object pointing to the cached statistic codes JSON file
    """
    load_dotenv()
    
    # Get refresh setting from .env
    refresh = os.getenv("REFRESH_STATISTIC_CODES").lower() == "true"
    
    return get_or_fetch_json(
        base_url_env="USGS_STATISTIC_CODES_BASE_URL",
        params_env="STAT_CODES_QUERY_PARAMS",
        output_env="STAT_CODES_OUTPUT_FILE",
        refresh=refresh,
    )

