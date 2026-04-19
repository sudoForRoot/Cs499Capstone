"""
Grazioso Salvare Rescue Animal Dashboard
CS 499 Capstone Project - Software Design and Engineering Enhancement

This application provides an interactive dashboard for viewing animal shelter data
from the Austin Animal Center. Users can filter animals by rescue type and view
results in a data table and map visualization.

Author: Steven Foltz
Course: CS 499 Capstone
Date: March 22, 2026
"""

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import logging
import os
from dotenv import load_dotenv
from crud import AnimalShelter

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# Initialize database connection with environment variables
try:
    logger.info("Initializing database connection...")
    db = AnimalShelter(
        username=os.getenv('DB_USERNAME', 'aacuser'),
        password=os.getenv('DB_PASSWORD', 'aacpassword'),
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 27017)),
        db_name=os.getenv('DB_NAME', 'AAC')
    )
    logger.info("Database connection established successfully")
except Exception as e:
    logger.error(f"Failed to connect to database: {e}")
    db = None

# Load initial data
initial_data = pd.DataFrame()
if db:
    try:
        initial_data = db.read({})
        logger.info(f"Loaded {len(initial_data)} initial records")
    except Exception as e:
        logger.error(f"Failed to load initial data: {e}")

# App layout
app.layout = dbc.Container([
    # Header with logo and title
    dbc.Row([
        dbc.Col(
            html.Div("🐕", className="display-4"),
            width=2,
            className="text-center"
        ),
        dbc.Col(
            html.H1("Grazioso Salvare Rescue Animal Dashboard",
                   className="text-center text-primary"),
            width=8
        ),
        dbc.Col(
            html.Div("CS 499 Capstone", className="text-right text-muted"),
            width=2
        )
    ], className="mb-4 align-items-center"),

    # Filter controls card
    dbc.Card([
        dbc.CardHeader(html.H4("Filter Controls", className="mb-0")),
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("Select Rescue Type:", className="font-weight-bold"),
                    dcc.RadioItems(
                        id='rescue-type',
                        options=[
                            {'label': '🐕 Water Rescue', 'value': 'water'},
                            {'label': '⛰️ Mountain/Wilderness Rescue', 'value': 'mountain'},
                            {'label': '🌊 Disaster/Individual Tracking', 'value': 'disaster'},
                            {'label': '🔄 Reset (Show All)', 'value': 'reset'}
                        ],
                        value='reset',
                        labelStyle={'display': 'block', 'margin': '10px 0'}
                    )
                ], width=3),
                dbc.Col([
                    html.Label("Status:", className="font-weight-bold"),
                    html.Div(id='record-count', className="h4 text-success")
                ], width=2)
            ])
        ])
    ], className="mb-4"),

    # Data table card
    dbc.Card([
        dbc.CardHeader(html.H4("Animal Records", className="mb-0")),
        dbc.CardBody([
            dash_table.DataTable(
                id='datatable',
                columns=[{"name": i, "id": i} for i in initial_data.columns] if not initial_data.empty else [],
                data=initial_data.to_dict('records') if not initial_data.empty else [],
                page_size=10,
                filter_action="native",
                sort_action="native",
                style_table={'overflowX': 'auto', 'height': '400px'},
                style_cell={
                    'height': 'auto',
                    'minWidth': '100px',
                    'width': '120px',
                    'maxWidth': '180px',
                    'whiteSpace': 'normal',
                    'textAlign': 'left',
                    'padding': '8px'
                },
                style_header={
                    'backgroundColor': '#2c3e50',
                    'color': 'white',
                    'fontWeight': 'bold',
                    'textAlign': 'center'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': '#f8f9fa'
                    }
                ]
            )
        ])
    ], className="mb-4"),

    # Charts row
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4("Animal Locations", className="mb-0")),
                dbc.CardBody([
                    dcc.Graph(id='map-graph')
                ])
            ])
        ], width=6),

        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4("Breed Distribution", className="mb-0")),
                dbc.CardBody([
                    dcc.Graph(id='breed-pie-chart')
                ])
            ])
        ], width=6)
    ])
], fluid=True, className="p-4")

# Helper function to create map figure
def create_map_figure(data, rescue_type):
    """Create a scatter mapbox figure from the data."""
    try:
        if data.empty or 'location_lat' not in data.columns:
            return px.scatter_mapbox(lat=[0], lon=[0]).update_layout(
                title="No location data available",
                mapbox_style="open-street-map"
            )

        # Color mapping based on rescue type
        color_map = {
            'water': '#1f77b4',
            'mountain': '#2ca02c',
            'disaster': '#d62728',
            'reset': '#2c3e50'
        }
        color = color_map.get(rescue_type, '#2c3e50')

        map_fig = px.scatter_mapbox(
            data,
            lat="location_lat",
            lon="location_long",
            hover_name="name" if "name" in data.columns else None,
            hover_data=["breed", "age_upon_outcome_in_weeks"] if "breed" in data.columns else None,
            color_discrete_sequence=[color],
            zoom=10,
            height=400
        )
        map_fig.update_layout(
            mapbox_style="open-street-map",
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            title=f"Locations ({len(data)} animals)"
        )
        return map_fig
    except Exception as e:
        logger.error(f"Error creating map: {e}")
        return px.scatter_mapbox(lat=[0], lon=[0]).update_layout(title="Error loading map")

# Helper function to create pie chart
def create_pie_chart(data):
    """Create a pie chart of breed distribution."""
    try:
        if data.empty or 'breed' not in data.columns:
            return px.pie(values=[1], names=['No Data']).update_layout(title="No breed data available")

        breed_counts = data['breed'].value_counts().reset_index()
        breed_counts.columns = ['breed', 'count']

        # Limit to top 10 breeds for readability
        if len(breed_counts) > 10:
            top_10 = breed_counts.head(10)
            other_count = breed_counts.iloc[10:]['count'].sum()
            if other_count > 0:
                other_row = pd.DataFrame({'breed': ['Other Breeds'], 'count': [other_count]})
                breed_counts = pd.concat([top_10, other_row], ignore_index=True)

        pie_fig = px.pie(
            breed_counts,
            values='count',
            names='breed',
            title='Top 10 Breeds',
            height=400,
            color_discrete_sequence=px.colors.sequential.Blues_r,
            hole=0.3
        )
        pie_fig.update_traces(textposition='inside', textinfo='percent+label')
        return pie_fig
    except Exception as e:
        logger.error(f"Error creating pie chart: {e}")
        return px.pie(values=[1], names=['Error']).update_layout(title="Error loading breed data")

# Callbacks for interactivity
@app.callback(
    [Output('datatable', 'columns'),
     Output('datatable', 'data'),
     Output('map-graph', 'figure'),
     Output('breed-pie-chart', 'figure'),
     Output('record-count', 'children')],
    [Input('rescue-type', 'value')]
)
def update_dashboard(rescue_type):
    """
    Update all dashboard components based on rescue type selection.

    Args:
        rescue_type (str): Selected rescue type ('water', 'mountain', 'disaster', 'reset')

    Returns:
        tuple: (columns, data, map_figure, pie_figure, record_count)
    """
    try:
        logger.info(f"Updating dashboard with rescue type: {rescue_type}")

        if not db:
            error_msg = "Database connection unavailable"
            logger.error(error_msg)
            return [], [], create_map_figure(pd.DataFrame(), 'reset'), create_pie_chart(pd.DataFrame()), error_msg

        # Get data based on rescue type
        if rescue_type == 'water':
            data = db.get_water_rescue_dogs()
        elif rescue_type == 'mountain':
            data = db.get_mountain_rescue_dogs()
        elif rescue_type == 'disaster':
            data = db.get_disaster_rescue_dogs()
        else:  # reset
            data = db.read({})

        # Handle empty data
        if data.empty:
            logger.warning(f"No data returned for rescue type: {rescue_type}")
            record_count = f"0 records found"
            return [], [], create_map_figure(data, rescue_type), create_pie_chart(data), record_count

        # Create columns for datatable
        columns = [{"name": col.replace('_', ' ').title(), "id": col} for col in data.columns]

        # Create map figure
        map_fig = create_map_figure(data, rescue_type)

        # Create pie chart
        pie_fig = create_pie_chart(data)

        # Record count
        record_count = f"{len(data)} records found"

        logger.info(f"Dashboard updated successfully: {record_count}")

        return columns, data.to_dict('records'), map_fig, pie_fig, record_count

    except Exception as e:
        logger.error(f"Error updating dashboard: {e}")
        error_msg = f"Error: {str(e)}"
        return [], [], create_map_figure(pd.DataFrame(), 'reset'), create_pie_chart(pd.DataFrame()), error_msg

if __name__ == '__main__':
    app.run_server(
        debug=os.getenv('DEBUG', 'True').lower() == 'true',
        host=os.getenv('HOST', '0.0.0.0'),
        port=int(os.getenv('PORT', 8050))
    )
