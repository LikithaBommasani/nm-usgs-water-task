### Project Structure

```
.
├── main.py                    # Main pipeline orchestrating the workflow
├── lookups.py                 # Functions to fetch lookup tables and locations
├── daily_values.py            # Functions to fetch and save daily values data
├── data_loader.py             # Functions to load, join, and save DataFrames
├── eda.py                     # Exploratory data analysis and cleaning functions
├── plots.py                   # Visualization functions for interactive charts
├── .env                       # Environment variables (configuration)
├── requirements.txt           # Python dependencies
├── eda_action_list.txt        # Documentation of data cleaning decisions
└── outputs/                   # Organized output directory
    ├── raw/                   # Raw API responses (JSON)
    ├── processed/             # Intermediate DataFrames (CSV)
    ├── final/                 # Final cleaned output (CSV)
    └── visualizations/        # Interactive plots (HTML)
```

### Data Source

**USGS NWIS (National Water Information System)**
- Main Website: https://waterdata.usgs.gov/

**API Endpoints Used:**
1. **Monitoring Locations API**
   - `https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items`
   - Retrieves information about monitoring stations (site names, IDs, locations, types)

2. **Daily Values API**
   - `https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items`
   - Retrieves daily measurement data (discharge, temperature) for the last 90 days

3. **Parameter Codes API**
   - `https://api.waterdata.usgs.gov/ogcapi/v0/collections/parameter-codes/items`
   - Lookup table for parameter codes (e.g., 00060 = Discharge, 00010 = Temperature)

4. **Statistic Codes API**
   - `https://api.waterdata.usgs.gov/ogcapi/v0/collections/statistic-codes/items`
   - Lookup table for statistic codes (e.g., 00003 = Mean, 00001 = Maximum)

### Data Retrieved
- **Region**: Santa Fe County, New Mexico (9 monitoring sites)
- **Parameters**: 
  - Discharge (00060) - measured in ft³/s
  - Water Temperature (00010, 00011) - measured in °C
- **Time Period**: Last 90 days of daily values
- **Total Observations**: 811 measurements

### How to Run
1. Activate virtual environment: `source .venv/bin/activate` (macOS/Linux) or `.venv\Scripts\activate` (Windows)
2. Install dependencies: `pip install -r requirements.txt`
3. Run the script: `python main.py`
4. View results in the `outputs/` directory and console output

### Summary Output
The script produces:
- **Number of observations**: Displayed per site and parameter
- **Min/Max/Average values**: Calculated for each site-parameter combination
- **Most recent measurement**: Shows latest value and date for each site
- **Two interactive plots**: 
  - Dual-axis time series (Discharge vs Temperature over time)
  - Scatter plot (Temperature vs Discharge correlation)
- **Cleaned dataset**: Saved as `outputs/final/cleaned.csv`

## Future Improvements

With more time, I would improve this project by building a **full-stack web application** with the following architecture:

### Web Dashboard with REST API

**Architecture**: FastAPI (Backend) + React + Refine.dev (Frontend)

**Backend (FastAPI)**:
- Wrap the existing `main()` function as a single REST API endpoint
- `POST /api/fetch-data` - Accepts user selections and returns processed data
  - Request body: `{ "county": "Santa Fe", "parameters": ["00060"], "days": 90 }`
  - Calls the existing Python pipeline with user parameters
  - Returns cleaned data and summary statistics as JSON


**Frontend (React and Refine.dev)**:
- Build an interactive dashboard where users can:
  - Select county, monitoring sites, and parameters from dropdowns
  - Choose time range (last 30/60/90 days or custom date range)
  - Click "Fetch & Analyze" to trigger the pipeline
- Display results in real-time:
  - Summary statistics cards (total observations, min/max/avg values)
  - Interactive data table with sorting, filtering, and pagination
  - Embedded Plotly charts for visualization
  - Download button for CSV export


**Benefits**:
- Non-technical users can explore USGS water data without coding
- Dynamic and real-time data fetching based on user selections
- Scalable architecture that can handle multiple concurrent users
- Production-ready application suitable for deployment


### Other Improvements:

1. **Data Validation**: Implement validation checks for outliers and suspicious values (e.g., negative discharge, temperature outside reasonable ranges)

2. **Automated Testing**: Add unit tests for data cleaning functions.

