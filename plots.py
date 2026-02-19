import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import webbrowser
import os
from dotenv import load_dotenv
from pathlib import Path


def get_sites_with_multiple_parameters(cleaned_df: pd.DataFrame, param1: str, param2: str) -> list:
    """
    Find monitoring sites that have data for both specified parameters.
    
    Args:
        cleaned_df: DataFrame with cleaned daily values data
        param1: First parameter name to check
        param2: Second parameter name to check
        
    Returns:
        List of site names that have both parameters
    """
    # Get sites with param1
    sites_param1 = set(cleaned_df[cleaned_df['parameter_name'] == param1]['monitoring_location_name'].unique())
    
    # Get sites with param2
    sites_param2 = set(cleaned_df[cleaned_df['parameter_name'] == param2]['monitoring_location_name'].unique())
    
    # Return sites that have both parameters
    return list(sites_param1.intersection(sites_param2))


def plot_discharge_and_temperature(cleaned_df: pd.DataFrame) -> None:
    """
    Create a dual-axis plot showing both parameters over time for sites with multiple parameters.
    
    Automatically finds sites that have data for both parameters specified in .env
    and creates a dual-axis time series plot. If multiple sites have both parameters,
    it will plot the first one found.
    
    Args:
        cleaned_df: DataFrame with cleaned daily values data
        
    Returns:
        None. Saves an interactive HTML plot to the specified path from .env
    """
    load_dotenv()
    
    # Get parameters and output path from environment variables
    param1 = os.getenv("PLOT_PARAMETER_1")
    param2 = os.getenv("PLOT_PARAMETER_2")
    output_path = os.getenv("OUTPUT_DUAL_AXIS_PLOT")
    
    # Find sites with both parameters
    sites_with_both = get_sites_with_multiple_parameters(cleaned_df, param1, param2)
    
    if not sites_with_both:
        # print(f"No sites found with both {param1} and {param2}")
        return
    
    # Use the first site with both parameters
    selected_site = sites_with_both[0]
    # print(f"Creating dual-parameter plot for: {selected_site}")
    
    # Filter for selected site only
    df = cleaned_df.copy()
    df['time'] = pd.to_datetime(df['time'])
    
    site_data = df[df['monitoring_location_name'] == selected_site].copy()
    
    if site_data.empty:
        print(f"No data found for {selected_site}")
        return
    
    # Separate data by parameter
    param1_data = site_data[site_data['parameter_name'] == param1].sort_values('time')
    param2_data = site_data[site_data['parameter_name'] == param2].sort_values('time')
    
    # Get units
    param1_unit = param1_data['unit_of_measure'].iloc[0] if not param1_data.empty else ""
    param2_unit = param2_data['unit_of_measure'].iloc[0] if not param2_data.empty else ""
    
    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add first parameter trace (primary y-axis)
    fig.add_trace(
        go.Scatter(
            x=param1_data['time'],
            y=param1_data['value'],
            name=param1,
            mode='lines+markers',
            line=dict(color='steelblue', width=2),
            marker=dict(size=6),
            hovertemplate=f'<b>{param1}</b><br>' +
                          'Date: %{x|%Y-%m-%d}<br>' +
                          f'Value: %{{y:.2f}} {param1_unit}<br>' +
                          '<extra></extra>'
        ),
        secondary_y=False
    )
    
    # Add second parameter trace (secondary y-axis)
    fig.add_trace(
        go.Scatter(
            x=param2_data['time'],
            y=param2_data['value'],
            name=param2,
            mode='lines+markers',
            line=dict(color='orange', width=2),
            marker=dict(size=6),
            hovertemplate=f'<b>{param2}</b><br>' +
                          'Date: %{x|%Y-%m-%d}<br>' +
                          f'Value: %{{y:.2f}} {param2_unit}<br>' +
                          '<extra></extra>'
        ),
        secondary_y=True
    )
    
    # Update layout
    fig.update_layout(
        title=f'{param1} and {param2} Over Time<br>{selected_site}',
        xaxis_title='Date',
        hovermode='x unified',
        width=1400,
        height=700,
        template='plotly_white',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Set y-axes titles
    fig.update_yaxes(title_text=f"{param1} ({param1_unit})", secondary_y=False, color='steelblue')
    fig.update_yaxes(title_text=f"{param2} ({param2_unit})", secondary_y=True, color='orange')
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray', secondary_y=False)
    
    # Ensure output directory exists and save
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(output_path)
    # print(f"Dual-parameter time series plot saved to {output_path}")
    
    # Open in browser
    webbrowser.open('file://' + os.path.abspath(output_path))


def plot_temperature_vs_discharge_scatter(cleaned_df: pd.DataFrame) -> None:
    """
    Create a scatter plot showing the relationship between two parameters.
    
    Automatically finds sites that have data for both parameters specified in .env
    and creates a scatter plot to show their correlation. Each point represents one day.
    
    Args:
        cleaned_df: DataFrame with cleaned daily values data
        
    Returns:
        None. Saves an interactive HTML plot to the specified path from .env
    """
    load_dotenv()
    
    # Get parameters and output path from environment variables
    param1 = os.getenv("PLOT_PARAMETER_1")
    param2 = os.getenv("PLOT_PARAMETER_2")
    output_path = os.getenv("OUTPUT_SCATTER_PLOT")
    
    # Find sites with both parameters
    sites_with_both = get_sites_with_multiple_parameters(cleaned_df, param1, param2)
    
    if not sites_with_both:
        print(f"No sites found with both {param1} and {param2}")
        return
    
    # Use the first site with both parameters
    selected_site = sites_with_both[0]
    # print(f"Creating scatter plot for: {selected_site}")
    
    # Filter for selected site only
    df = cleaned_df.copy()
    df['time'] = pd.to_datetime(df['time'])
    
    site_data = df[df['monitoring_location_name'] == selected_site].copy()
    
    if site_data.empty:
        print(f"No data found for {selected_site}")
        return
    
    # Pivot to get both parameters on same row
    param1_data = site_data[site_data['parameter_name'] == param1][['time', 'value', 'unit_of_measure']].rename(
        columns={'value': 'param1_value', 'unit_of_measure': 'param1_unit'}
    )
    param2_data = site_data[site_data['parameter_name'] == param2][['time', 'value', 'unit_of_measure']].rename(
        columns={'value': 'param2_value', 'unit_of_measure': 'param2_unit'}
    )
    
    # Merge on date
    merged = pd.merge(param1_data, param2_data, on='time', how='inner')
    
    if merged.empty:
        # print(f"No matching dates for {param1} and {param2}")
        return
    
    # Get units
    param1_unit = merged['param1_unit'].iloc[0]
    param2_unit = merged['param2_unit'].iloc[0]
    
    # Create scatter plot
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=merged['param2_value'],
        y=merged['param1_value'],
        mode='markers',
        marker=dict(
            size=10,
            color=merged['param1_value'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title=f"{param1}<br>({param1_unit})")
        ),
        text=merged['time'].dt.strftime('%Y-%m-%d'),
        hovertemplate='<b>Date: %{text}</b><br>' +
                      f'{param2}: %{{x:.2f}} {param2_unit}<br>' +
                      f'{param1}: %{{y:.2f}} {param1_unit}<br>' +
                      '<extra></extra>'
    ))
    
    # Update layout
    fig.update_layout(
        title=f'Relationship Between {param2} and {param1}<br>{selected_site}',
        xaxis_title=f'{param2} ({param2_unit})',
        yaxis_title=f'{param1} ({param1_unit})',
        width=1000,
        height=700,
        template='plotly_white'
    )
    
    # Add grid
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    # Ensure output directory exists and save
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(output_path)
    # print(f"Parameter correlation scatter plot saved to {output_path}")
    
    # Open in browser
    webbrowser.open('file://' + os.path.abspath(output_path))






