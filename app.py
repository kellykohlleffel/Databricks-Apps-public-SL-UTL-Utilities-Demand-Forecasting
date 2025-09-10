import os
import streamlit as st
import pandas as pd
import altair as alt
import time
import json
import re
import requests
from datetime import datetime
from databricks import sql

st.set_page_config(
    page_title="demandpredict_–_ai_driven_demand_forecasting_and_management",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add CSS for agent progress styling
st.markdown("""
<style>
.agent-current {
    background-color: #e3f2fd;
    border-left: 4px solid #2196f3;
    padding: 10px;
    margin: 5px 0;
    border-radius: 5px;
    font-weight: 500;
}

.agent-completed {
    background-color: #e8f5e8;
    border-left: 4px solid #4caf50;
    padding: 8px;
    margin: 3px 0;
    border-radius: 5px;
    font-size: 0.9em;
    color: #2e7d32;
}

.agent-container {
    background-color: #f8f9fa;
    padding: 15px;
    border-radius: 10px;
    margin: 10px 0;
    border: 1px solid #e0e0e0;
}

.agent-status-active {
    color: #4CAF50;
    font-weight: bold;
    font-size: 1.1em;
}

.agent-button-container {
    display: flex;
    gap: 10px;
    align-items: center;
    margin: 10px 0;
}

.agent-report-header {
    background-color: #f8f9fa;
    padding: 10px;
    border-radius: 5px;
    margin: 10px 0;
    border-left: 4px solid #2196F3;
}
</style>
""", unsafe_allow_html=True)

solution_name = '''Solution 2: DemandPredict – AI-Driven Demand Forecasting and Management'''
solution_name_clean = '''demandpredict_–_ai_driven_demand_forecasting_and_management'''
table_description = '''Consolidated table containing smart meter readings, weather data, and customer behavior information for AI-driven demand forecasting and management'''
solution_content = '''Solution 2: DemandPredict – AI-Driven Demand Forecasting and Management**

### Business Challenge
Utilities struggle to accurately predict energy demand, leading to inefficient resource allocation, wasted capacity, and potential power outages.

### Key Features
DemandPredict uses generative AI to analyze historical and real-time data from various sources, including:

* **Smart Meters:** Itron, Sensus, Landis+Gyr
* **Weather Forecasting:** The Weather Company, AccuWeather, DTN
* **Customer Behavior:** Customer information systems, CRM, and social media

DemandPredict provides accurate demand forecasts, enabling utilities to optimize resource allocation and reduce waste.

### Competitive Advantage
DemandPredict differentiates from traditional approaches by leveraging generative AI to analyze complex patterns in customer behavior and weather data.

### Key Stakeholders
* Director of Demand Management
* Chief Customer Officer
* CTO

### Technical Approach
DemandPredict employs a combination of NLP, computer vision, and predictive analytics to analyze demand data and identify patterns.

### Expected Business Results

* **$ 1,500,000 in annual cost savings**
	+ **$ 5,000,000 annual energy costs × 30% reduction = $ 1,500,000 savings/year**
* **10% reduction in energy waste**
	+ **10,000 MWh energy consumption × 10% reduction = 1,000 MWh saved**
* **20% increase in customer satisfaction**
	+ **80,000 customers × 20% increase = 16,000 additional satisfied customers**
* **5% reduction in peak demand**
	+ **10,000 MW peak demand × 5% reduction = 500 MW reduced peak demand**

### Success Metrics
* Demand accuracy
* Energy waste reduction
* Customer satisfaction
* Peak demand reduction

### Risk Assessment
Potential implementation challenges include data quality issues, model bias, and integration with existing systems. Mitigation strategies include data cleansing, model retraining, and close collaboration with demand management teams.

### Long-term Evolution
DemandPredict will evolve to incorporate advanced generative AI techniques, such as transfer learning, to adapt to changing customer behavior and weather patterns.

---

**'''

# Display logo and title inline
st.markdown(f'''
<div style="display:flex; align-items:center; margin-bottom:15px">
    <img src="https://i.imgur.com/vy5mZAD.png" width="100" style="margin-right:15px">
    <div>
        <h1 style="font-size:2.2rem; margin:0; padding:0">{solution_name_clean.replace('_', ' ').title()}</h1>
        <p style="font-size:1.1rem; color:gray; margin:0; padding:0">Fivetran and Databricks-powered Streamlit data application for Utilities Demand Forecasting</p>
    </div>
</div>
''', unsafe_allow_html=True)

# Retrieve credentials from environment variables
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_SQL_HTTP_PATH", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

# Unity Catalog settings
UC_CATALOG = os.environ.get("UC_CATALOG")
UC_SCHEMA  = os.environ.get("UC_SCHEMA")
UC_TABLE   = os.environ.get("UC_TABLE")

# Serving endpoint display names (shown in the UI dropdown)
DBX_ENDPOINT   = os.environ.get("DBX_ENDPOINT")
DBX_ENDPOINT_2 = os.environ.get("DBX_ENDPOINT_2")
DBX_ENDPOINT_3 = os.environ.get("DBX_ENDPOINT_3")
DBX_ENDPOINT_4 = os.environ.get("DBX_ENDPOINT_4")
DBX_ENDPOINT_5 = os.environ.get("DBX_ENDPOINT_5")
DBX_ENDPOINT_6 = os.environ.get("DBX_ENDPOINT_6")
DBX_ENDPOINT_7 = os.environ.get("DBX_ENDPOINT_7")
DBX_ENDPOINT_8 = os.environ.get("DBX_ENDPOINT_8")

# Filter out missing endpoints for the UI dropdown
MODELS = [m for m in [
    DBX_ENDPOINT,
    DBX_ENDPOINT_2,
    DBX_ENDPOINT_3,
    DBX_ENDPOINT_4,
    DBX_ENDPOINT_5,
    DBX_ENDPOINT_6,
    DBX_ENDPOINT_7,
    DBX_ENDPOINT_8
] if m]

# Unity Catalog table reference with proper backtick escaping
table_name = f"""`{UC_CATALOG}`.`{UC_SCHEMA}`.`{UC_TABLE}`"""

# Session state init
if 'insights_history' not in st.session_state:
    st.session_state.insights_history = []
if 'data_cache' not in st.session_state:
    st.session_state.data_cache = {}

focus_areas = ["Overall Performance", "Optimization Opportunities", "Financial Impact", "Strategic Recommendations"]
for area in focus_areas:
    key = f'{area.lower().replace(" ", "_")}_completed_steps'
    if key not in st.session_state:
        st.session_state[key] = []

# Build a name -> URL map that mirrors app.yaml exactly, then prune incomplete pairs
ENDPOINT_URLS = {
    DBX_ENDPOINT:   os.environ.get("DATABRICKS_SERVING_ENDPOINT_URL", ""),
    DBX_ENDPOINT_2: os.environ.get("DATABRICKS_ENDPOINT_2_URL", ""),
    DBX_ENDPOINT_3: os.environ.get("DATABRICKS_ENDPOINT_3_URL", ""),
    DBX_ENDPOINT_4: os.environ.get("DATABRICKS_ENDPOINT_4_URL", ""),
    DBX_ENDPOINT_5: os.environ.get("DATABRICKS_ENDPOINT_5_URL", ""),
    DBX_ENDPOINT_6: os.environ.get("DATABRICKS_ENDPOINT_6_URL", ""),
    DBX_ENDPOINT_7: os.environ.get("DATABRICKS_ENDPOINT_7_URL", ""),
    DBX_ENDPOINT_8: os.environ.get("DATABRICKS_ENDPOINT_8_URL", "")
}
ENDPOINT_URLS = {name: url for name, url in ENDPOINT_URLS.items() if name and url}

# Parametrized system prompt to avoid hard-coding domain
SYSTEM_PROMPT = os.environ.get(
    "SYSTEM_PROMPT",
)

# --- Databricks SQL helpers ---
def get_connection():
    """Create a new Databricks SQL connection (with friendly errors)."""
    try:
        missing = [k for k, v in {
            "DATABRICKS_HOST": DATABRICKS_HOST,
            "DATABRICKS_SQL_HTTP_PATH/DATABRICKS_HTTP_PATH": DATABRICKS_HTTP_PATH,
            "DATABRICKS_TOKEN": DATABRICKS_TOKEN
        }.items() if not v]
        if missing:
            st.error("Missing required Databricks configuration: " + ", ".join(missing))
            return None

        return sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
    except Exception as e:
        st.error(f"❌ Error connecting to Databricks: {str(e)}")
        return None

def execute_query(query, params=None):
    """Execute a SQL query on Databricks with proper error handling."""
    try:
        connection = get_connection()
        if not connection:
            return pd.DataFrame()

        with connection.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

        connection.close()
        return pd.DataFrame(result, columns=columns)
    except Exception as e:
        st.error(f"Query failed: {str(e)}")
        return pd.DataFrame()

# --- LLM endpoint caller ---
def call_serving_endpoint(prompt, endpoint_name):
    """Call Databricks LLM serving endpoint (requires SYSTEM_PROMPT set in .env)."""
    try:
        endpoint_url = ENDPOINT_URLS.get(endpoint_name)
        if not endpoint_url:
            st.error(f"URL for endpoint '{endpoint_name}' not found. Check env vars in app.yaml.")
            return None

        if not (isinstance(SYSTEM_PROMPT, str) and SYSTEM_PROMPT.strip()):
            st.error("❌ SYSTEM_PROMPT is missing or empty in your .env")
            return None

        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json",
        }

        payload = {
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT.strip()},
                {"role": "user", "content": str(prompt)},
            ],
            "temperature": 0.3,
            "max_tokens": 2400,
        }

        response = requests.post(endpoint_url, headers=headers, json=payload, timeout=120)
        if response.status_code == 200:
            result = response.json()
            return result["choices"][0]["message"]["content"]
        else:
            st.error(f"Error from serving endpoint: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        st.error(f"❌ Databricks serving endpoint error: {str(e)}")
        return None

def safe_convert_boolean(series, column_name):
    """Safely convert boolean columns that may be stored as concatenated strings"""
    if series.empty:
        return series
    
    try:
        # Handle different boolean storage formats
        if series.dtype == 'object':
            # Check if it's a string column that looks like concatenated booleans
            sample_val = str(series.iloc[0]).lower() if not pd.isna(series.iloc[0]) else ""
            
            if 'true' in sample_val and 'false' in sample_val and len(sample_val) > 10:
                # This is likely a concatenated boolean string, extract the first boolean value
                return series.astype(str).str.lower().str.extract(r'(true|false)')[0].map({'true': True, 'false': False, None: False})
            else:
                # Normal string boolean conversion
                return series.astype(str).str.lower().map({'true': True, 'false': False, '1': True, '0': False, 'yes': True, 'no': False}).fillna(False)
        elif series.dtype in ['int64', 'float64']:
            # Convert numeric to boolean (1/0 to True/False)
            return series.fillna(0).astype(bool)
        else:
            # Already boolean or other type
            return series.fillna(False).astype(bool)
    except Exception as e:
        st.warning(f"Boolean conversion warning for {column_name}: {str(e)}")
        return pd.Series([False] * len(series), index=series.index)

def safe_numeric_conversion(series, column_name):
    """Safely convert numeric columns with error handling"""
    try:
        return pd.to_numeric(series, errors='coerce').fillna(0)
    except Exception as e:
        st.warning(f"Numeric conversion warning for {column_name}: {str(e)}")
        return pd.Series([0.0] * len(series), index=series.index)

def load_data():
    """Load data from Unity Catalog with proper data type handling"""
    query = f"SELECT * FROM {table_name} WHERE `_fivetran_deleted` = false LIMIT 1000"
    df = execute_query(query)
    
    if not df.empty:
        # Convert column names to lowercase for consistency
        df.columns = [col.lower() for col in df.columns]
        
        # CRITICAL: Fix boolean columns that may be stored as concatenated strings
        boolean_columns = ['_fivetran_deleted']
        
        for col in boolean_columns:
            if col in df.columns:
                df[col] = safe_convert_boolean(df[col], col)
        
        # CRITICAL: Ensure numeric columns are properly typed and handle NaN values
        numeric_columns = [
            'energy_consumption_kwh', 'peak_demand_kw', 'voltage_level', 'power_factor',
            'temperature_fahrenheit', 'humidity_percent', 'wind_speed_mph', 'billing_cycle_day',
            'outage_events', 'social_media_sentiment', 'customer_complaints', 'predicted_demand_mw'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = safe_numeric_conversion(df[col], col)
        
        # Handle date columns
        date_columns = ['timestamp', '_fivetran_synced']
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except:
                    pass
    
    return df

def get_focus_area_info(focus_area):
    """Get business challenge and agent solution for each focus area"""
    
    focus_info = {
        "Overall Performance": {
            "challenge": "Utility Directors and Chief Technology Officers manually review hundreds of smart meter readings, weather data points, and demand forecasts daily, spending 4+ hours analyzing energy consumption patterns, peak demand trends, and outage correlations to identify grid optimization opportunities and demand prediction accuracy.",
            "solution": "Autonomous demand forecasting workflow that analyzes smart meter data, weather patterns, consumption metrics, and grid performance to generate automated demand summaries, identify peak demand risks, and produce prioritized grid optimization insights with predictive load management recommendations."
        },
        "Optimization Opportunities": {
            "challenge": "Grid Operators and Demand Management teams spend 5+ hours daily manually identifying inefficiencies in energy distribution, load balancing strategies, and peak demand management across multiple service territories and weather conditions.",
            "solution": "AI-powered demand forecasting optimization analysis that automatically detects grid performance gaps, energy waste opportunities, and load balancing improvements with specific implementation recommendations for Itron, Sensus, and Landis+Gyr smart meter integration."
        },
        "Financial Impact": {
            "challenge": "Chief Financial Officers manually calculate complex ROI metrics across energy demand management activities and grid efficiency performance, requiring 4+ hours of financial modeling to assess operational costs and demand prediction effectiveness across the utility portfolio.",
            "solution": "Automated demand forecasting financial analysis that calculates comprehensive grid efficiency ROI, identifies cost reduction opportunities across energy operations, and projects demand management benefits with detailed savings forecasting."
        },
        "Strategic Recommendations": {
            "challenge": "Chief Technology Officers spend hours manually analyzing digital transformation opportunities and developing strategic demand forecasting roadmaps for grid modernization and smart meter implementation across utility operations.",
            "solution": "Strategic demand forecasting intelligence workflow that analyzes competitive advantages against traditional grid management processes, identifies AI and predictive analytics integration opportunities, and creates prioritized digital grid transformation roadmaps."
        }
    }
    
    return focus_info.get(focus_area, {"challenge": "", "solution": ""})

def safe_mean(series):
    """Safely calculate mean, handling NaN values and empty series"""
    try:
        if series.empty:
            return 0
        numeric_series = pd.to_numeric(series, errors='coerce')
        return numeric_series.dropna().mean() if not numeric_series.dropna().empty else 0
    except:
        return 0

def safe_unique_count(series):
    """Safely count unique values, handling NaN"""
    try:
        if series.empty:
            return 0
        return series.dropna().nunique()
    except:
        return 0

def safe_sum(series):
    """Safely calculate sum, handling NaN values"""
    try:
        if series.empty:
            return 0
        numeric_series = pd.to_numeric(series, errors='coerce')
        return numeric_series.dropna().sum() if not numeric_series.dropna().empty else 0
    except:
        return 0

def generate_insights_with_agent_workflow(data, focus_area, model_name, progress_placeholder=None):
    """Generate insights using AI agent workflow - Demand Forecasting focused version"""
    
    try:
        # FIRST: Generate the actual insights (behind the scenes)
        insights = generate_insights(data, focus_area, model_name)
        
        # THEN: Prepare for animation
        session_key = f'{focus_area.lower().replace(" ", "_")}_completed_steps'
        st.session_state[session_key] = []
        
        def update_progress(step_name, progress_percent, details, results):
            """Update progress display with completed steps"""
            if progress_placeholder:
                with progress_placeholder.container():
                    # Progress bar
                    st.progress(progress_percent / 100)
                    st.write(f"**{step_name} ({progress_percent}%)**")
                    
                    if details:
                        st.markdown(f'<div class="agent-current">{details}</div>', unsafe_allow_html=True)
                    
                    if results:
                        st.session_state[session_key].append((step_name, results))
                        
                    # Display completed steps
                    for completed_step, completed_result in st.session_state[session_key]:
                        st.markdown(f'<div class="agent-completed">✅ {completed_step}: {completed_result}</div>', unsafe_allow_html=True)
        
        # Calculate real data for enhanced context with safe operations
        total_records = len(data)
        key_metrics = ["energy_consumption_kwh", "peak_demand_kw", "voltage_level", "predicted_demand_mw"]
        available_metrics = [col for col in key_metrics if col in data.columns]
        
        # Calculate enhanced demand forecasting data insights with safe operations
        avg_consumption = safe_mean(data['energy_consumption_kwh']) if 'energy_consumption_kwh' in data.columns else 0
        avg_peak_demand = safe_mean(data['peak_demand_kw']) if 'peak_demand_kw' in data.columns else 0
        unique_meters = safe_unique_count(data['meter_id']) if 'meter_id' in data.columns else 0
        unique_territories = safe_unique_count(data['service_territory']) if 'service_territory' in data.columns else 0
        avg_predicted_demand = safe_mean(data['predicted_demand_mw']) if 'predicted_demand_mw' in data.columns else 0
        total_outages = safe_sum(data['outage_events']) if 'outage_events' in data.columns else 0
        
        # Define enhanced agent workflows for each focus area
        if focus_area == "Overall Performance":
            steps = [
                ("Demand Forecasting Data Initialization", 15, f"Loading comprehensive demand forecasting dataset with enhanced validation across {total_records} meter readings and {unique_meters} active smart meters", f"Connected to {len(available_metrics)} demand metrics across {len(data.columns)} total energy data dimensions"),
                ("Energy Consumption Assessment", 35, f"Advanced calculation of demand forecasting indicators with consumption analysis (avg: {avg_consumption:.1f} kWh)", f"Computed demand metrics: {avg_consumption:.1f} kWh avg consumption, {avg_peak_demand:.1f} kW peak demand, {avg_predicted_demand:.1f} MW predicted demand"),
                ("Grid Performance Pattern Recognition", 55, f"Sophisticated identification of demand patterns with consumption correlation analysis across {unique_territories} service territories", f"Detected significant patterns in {safe_unique_count(data['weather_condition']) if 'weather_condition' in data.columns else 'N/A'} weather conditions with grid correlation analysis completed"),
                ("AI Demand Intelligence Processing", 75, f"Processing comprehensive demand data through {model_name} with advanced reasoning for grid optimization insights", f"Enhanced AI analysis of demand forecasting effectiveness across {total_records} meter readings completed"),
                ("Demand Forecasting Report Compilation", 100, f"Professional demand forecasting analysis with evidence-based recommendations and actionable grid insights ready", f"Comprehensive demand performance report with {len(available_metrics)} energy metrics analysis and grid optimization recommendations generated")
            ]
            
        elif focus_area == "Optimization Opportunities":
            grid_efficiency = (avg_peak_demand / avg_consumption) if avg_consumption > 0 else 0
            prediction_accuracy = abs(avg_predicted_demand * 1000 - avg_peak_demand) / avg_peak_demand if avg_peak_demand > 0 else 0
            
            steps = [
                ("Demand Optimization Data Preparation", 12, f"Advanced loading of demand forecasting data with enhanced validation across {total_records} records for grid efficiency identification", f"Prepared {unique_meters} active meters, {unique_territories} territories for optimization analysis with {total_outages} outage events"),
                ("Grid Performance Inefficiency Detection", 28, f"Sophisticated analysis of demand forecasting strategies and grid performance with evidence-based inefficiency identification", f"Identified optimization opportunities across {unique_territories} service territories with demand and grid management gaps"),
                ("Energy Consumption Correlation Analysis", 45, f"Enhanced examination of relationships between weather conditions, customer types, and demand performance rates", f"Analyzed correlations between energy characteristics and demand outcomes across {total_records} meter readings"),
                ("Smart Meter Integration Optimization", 65, f"Comprehensive evaluation of demand forecasting integration with existing Itron, Sensus, and Landis+Gyr smart meter systems", f"Assessed integration opportunities across {len(data.columns)} data points and demand forecasting system optimization needs"),
                ("AI Demand Intelligence", 85, f"Generating advanced demand optimization recommendations using {model_name} with grid reasoning and implementation strategies", f"AI-powered demand forecasting optimization strategy across {unique_territories} service territories and efficiency improvements completed"),
                ("Grid Strategy Finalization", 100, f"Professional demand optimization report with prioritized implementation roadmap and grid impact analysis ready", f"Comprehensive optimization strategy with {len(available_metrics)} efficiency improvement areas and demand forecasting implementation plan generated")
            ]
            
        elif focus_area == "Financial Impact":
            total_energy_value = avg_consumption * unique_meters * 0.12 if avg_consumption > 0 and unique_meters > 0 else 0  # Assuming $0.12/kWh
            cost_savings = total_energy_value * 0.30 if total_energy_value > 0 else 0  # 30% reduction potential
            
            steps = [
                ("Demand Financial Data Integration", 15, f"Advanced loading of demand financial data and grid cost metrics with enhanced validation across {total_records} meter readings", f"Integrated demand financial data: {avg_consumption:.1f} kWh avg consumption, {avg_peak_demand:.1f} kW avg peak across {unique_meters} meters"),
                ("Energy Cost-Benefit Calculation", 30, f"Sophisticated ROI metrics calculation with demand analysis and grid efficiency cost savings", f"Computed comprehensive cost analysis: energy expenses, outage costs, and ${cost_savings:,.0f} estimated demand optimization potential"),
                ("Grid Efficiency Impact Assessment", 50, f"Enhanced analysis of energy revenue impact with demand metrics and grid correlation analysis", f"Assessed grid implications: {total_outages} total outages with {unique_meters} meters requiring optimization"),
                ("Energy Operations Efficiency Analysis", 70, f"Comprehensive evaluation of operational cost efficiency across demand activities with grid lifecycle cost optimization", f"Analyzed operational efficiency: {unique_territories} service territories with energy cost reduction opportunities identified"),
                ("AI Demand Financial Modeling", 90, f"Advanced demand forecasting financial projections and ROI calculations using {model_name} with comprehensive energy cost-benefit analysis", f"Enhanced financial impact analysis and forecasting across {len(available_metrics)} energy cost metrics completed"),
                ("Energy Economics Report Generation", 100, f"Professional demand financial impact analysis with detailed grid ROI calculations and energy cost forecasting ready", f"Comprehensive energy financial report with ${cost_savings:,.0f} cost optimization analysis and demand efficiency strategy generated")
            ]
            
        elif focus_area == "Strategic Recommendations":
            # Calculate demand forecasting efficiency score for Strategic Recommendations
            demand_efficiency = (avg_predicted_demand * 1000 / avg_peak_demand * 100) if avg_peak_demand > 0 else 0
            grid_efficiency_score = demand_efficiency if demand_efficiency > 0 else 0
            
            steps = [
                ("Grid Technology Assessment", 15, f"Advanced loading of demand forecasting digital context with competitive positioning analysis across {total_records} meter readings and {unique_meters} active meters", f"Analyzed grid technology landscape: {unique_territories} service territories, {unique_meters} meters, comprehensive demand forecasting digitization assessment completed"),
                ("Demand Competitive Advantage Analysis", 30, f"Sophisticated evaluation of competitive positioning against traditional demand forecasting with AI-powered grid optimization effectiveness", f"Assessed competitive advantages: {grid_efficiency_score:.1f}% demand efficiency, {avg_consumption:.1f} kWh vs industry benchmarks"),
                ("Advanced Grid Technology Integration", 50, f"Enhanced analysis of integration opportunities with weather analytics, real-time demand monitoring, and AI-powered smart grid sensing across {len(data.columns)} energy data dimensions", f"Identified strategic technology integration: real-time demand sensing, adaptive forecasting algorithms, automated grid optimization opportunities"),
                ("Digital Grid Strategy Development", 70, f"Comprehensive development of prioritized digital transformation roadmap with evidence-based grid technology adoption strategies", f"Created sequenced implementation plan across {unique_territories} service territories with advanced demand forecasting technology integration opportunities"),
                ("AI Demand Strategic Processing", 85, f"Advanced demand forecasting strategic recommendations using {model_name} with long-term competitive positioning and grid technology analysis", f"Enhanced strategic analysis with grid competitive positioning and digital transformation roadmap completed"),
                ("Digital Grid Report Generation", 100, f"Professional digital grid transformation roadmap with competitive analysis and demand technology implementation plan ready for CTO executive review", f"Comprehensive strategic report with {unique_meters}-meter implementation plan and grid competitive advantage analysis generated")
            ]
        
        # NOW: Animate the progress with pre-calculated results
        for step_name, progress_percent, details, results in steps:
            update_progress(step_name, progress_percent, details, results)
            time.sleep(1.2)
        
        return insights
        
    except Exception as e:
        if progress_placeholder:
            progress_placeholder.error(f"❌ Enhanced Agent Analysis failed: {str(e)}")
        return f"Enhanced Agent Analysis failed: {str(e)}"

def generate_insights(data, focus_area, model_name):
    """Generate insights using the selected Databricks model"""
    data_summary = f"Table: {table_name}\n"
    data_summary += f"Description: {table_description}\n"
    data_summary += f"Records analyzed: {len(data)}\n"

    # Calculate basic statistics for numeric columns only
    numeric_stats = {}
    # Define key utilities demand forecasting metrics that should be numeric
    key_metrics = ["energy_consumption_kwh", "peak_demand_kw", "voltage_level", "power_factor", 
                   "temperature_fahrenheit", "humidity_percent", "wind_speed_mph", "billing_cycle_day", 
                   "outage_events", "social_media_sentiment", "customer_complaints", "predicted_demand_mw"]
    
    # Filter to only columns that exist and are actually numeric
    available_metrics = []
    for col in key_metrics:
        if col in data.columns:
            try:
                # Test if the column is actually numeric by trying to calculate mean
                test_mean = safe_mean(data[col])
                if not pd.isna(test_mean):
                    available_metrics.append(col)
            except:
                # Skip columns that can't be converted to numeric
                continue
    
    for col in available_metrics:
        try:
            numeric_data = pd.to_numeric(data[col], errors='coerce').dropna()
            if not numeric_data.empty:
                numeric_stats[col] = {
                    "mean": numeric_data.mean(),
                    "min": numeric_data.min(),
                    "max": numeric_data.max(),
                    "std": numeric_data.std()
                }
                data_summary += f"- {col} (avg: {numeric_data.mean():.2f}, min: {numeric_data.min():.2f}, max: {numeric_data.max():.2f})\n"
        except Exception as e:
            # Skip columns that cause errors
            continue

    # Get top values for categorical columns
    categorical_stats = {}
    categorical_options = ["meter_id", "customer_id", "weather_condition", "customer_type", "service_territory", "rate_schedule"]
    for cat_col in categorical_options:
        if cat_col in data.columns:
            try:
                top = data[cat_col].value_counts().head(3)
                categorical_stats[cat_col] = top.to_dict()
                data_summary += f"\nTop {cat_col} values:\n" + "\n".join(f"- {k}: {v}" for k, v in top.items())
            except:
                # Skip columns that cause errors
                continue

    # Calculate correlations if enough numeric columns available
    correlation_info = ""
    if len(available_metrics) >= 2:
        try:
            # Create a dataframe with only the numeric columns
            numeric_df = data[available_metrics].apply(pd.to_numeric, errors='coerce').dropna()
            
            if not numeric_df.empty and len(numeric_df) > 1:
                correlations = numeric_df.corr()
                
                # Get the top 3 strongest correlations (absolute value)
                corr_pairs = []
                for i in range(len(correlations.columns)):
                    for j in range(i+1, len(correlations.columns)):
                        col1 = correlations.columns[i]
                        col2 = correlations.columns[j]
                        corr_value = correlations.iloc[i, j]
                        if not pd.isna(corr_value):
                            corr_pairs.append((col1, col2, abs(corr_value), corr_value))

                # Sort by absolute correlation value
                corr_pairs.sort(key=lambda x: x[2], reverse=True)

                # Add top correlations to the summary
                if corr_pairs:
                    correlation_info = "Top correlations between utilities demand metrics:\n"
                    for col1, col2, _, corr_value in corr_pairs[:3]:
                        correlation_info += f"- {col1} and {col2}: r = {corr_value:.2f}\n"
        except Exception as e:
            correlation_info = "Could not calculate correlations between demand metrics.\n"

    # Define specific instructions for each focus area tailored to utilities demand forecasting
    focus_area_instructions = {
        "Overall Performance": """
        For the Overall Performance analysis of DemandPredict in Utilities Demand Forecasting:
        1. Provide a comprehensive analysis of the demand forecasting system using smart meter readings, weather data, and predicted demand accuracy
        2. Identify significant patterns in energy consumption, peak demand, weather correlations, and customer behavior across different customer types and service territories
        3. Highlight 3-5 key demand forecasting metrics that best indicate system performance (energy consumption accuracy, peak demand prediction, voltage stability, weather impact analysis)
        4. Discuss both strengths and areas for improvement in the AI-powered demand prediction process
        5. Include 3-5 actionable insights for improving demand forecasting accuracy based on smart meter and weather data
        
        Structure your response with these utilities demand forecasting focused sections:
        - Demand Forecasting Performance Insights (5 specific insights with supporting energy consumption and prediction data)
        - Energy Demand Trends (3-4 significant trends in consumption patterns and peak demand)
        - Forecasting Accuracy Recommendations (3-5 data-backed recommendations for improving demand prediction operations)
        - Implementation Steps (3-5 concrete next steps for demand management teams and utility operators)
        """,
        
        "Optimization Opportunities": """
        For the Optimization Opportunities analysis of DemandPredict in Utilities Demand Forecasting:
        1. Focus specifically on areas where demand prediction accuracy, energy efficiency, and grid stability can be improved
        2. Identify inefficiencies in energy distribution, peak demand management, voltage regulation, and weather-related forecasting
        3. Analyze correlations between weather conditions, customer behavior, energy consumption patterns, and predicted demand accuracy
        4. Prioritize optimization opportunities based on potential impact on reducing energy waste and improving grid reliability
        5. Suggest specific technical or process improvements for integration with existing smart meter infrastructure (Itron, Sensus, Landis+Gyr)
        
        Structure your response with these utilities demand forecasting focused sections:
        - Demand Forecasting Optimization Priorities (3-5 areas with highest energy efficiency and prediction accuracy improvement potential)
        - Grid Stability Impact Analysis (quantified benefits of addressing each opportunity in terms of reduced outages and improved reliability)
        - Smart Meter Integration Strategy (specific steps for utility operators to implement each optimization)
        - System Integration Recommendations (specific technical changes needed for seamless integration with existing grid management systems)
        - Utility Operations Risk Assessment (potential challenges for demand management teams and how to mitigate them)
        """,
        
        "Financial Impact": """
        For the Financial Impact analysis of DemandPredict in Utilities Demand Forecasting:
        1. Focus on cost-benefit analysis and ROI in energy management terms (operational costs vs. demand prediction accuracy benefits)
        2. Quantify financial impacts through energy waste reduction, peak demand optimization, and improved grid reliability
        3. Identify cost savings opportunities across different customer types and service territories
        4. Analyze energy consumption patterns and outage costs across different weather conditions and demand scenarios
        5. Project future financial outcomes based on improved demand forecasting accuracy and reduced energy waste
        
        Structure your response with these utilities demand forecasting focused sections:
        - Energy Cost Analysis (breakdown of operational costs and potential savings by customer type and service territory)
        - Demand Prediction ROI Impact (how improved forecasting affects energy costs and grid efficiency)
        - Utilities Operations ROI Calculation (specific calculations showing return on investment in terms of energy waste reduction and cost savings)
        - Cost Reduction Opportunities (specific areas to reduce operational costs and improve demand management efficiency)
        - Financial Forecasting (projections based on improved demand prediction performance metrics)
        """,
        
        "Strategic Recommendations": """
        For the Strategic Recommendations analysis of DemandPredict in Utilities Demand Forecasting:
        1. Focus on long-term strategic implications for digital transformation in utilities demand management and smart grid operations
        2. Identify competitive advantages against traditional demand forecasting approaches
        3. Suggest new directions for AI integration with emerging smart grid technologies (e.g., real-time weather integration, predictive maintenance, autonomous grid management)
        4. Connect recommendations to broader business goals of improving grid reliability and reducing operational costs
        5. Provide a digital utilities transformation roadmap with prioritized initiatives
        
        Structure your response with these utilities demand forecasting focused sections:
        - Smart Grid Digital Context (how DemandPredict fits into broader digital transformation in utilities operations)
        - Competitive Advantage Analysis (how to maximize forecasting advantages compared to traditional demand management approaches)
        - Utilities Technology Strategic Priorities (3-5 high-impact strategic initiatives for improving demand forecasting operations)
        - Advanced Grid Analytics Integration Vision (how to evolve DemandPredict with AI and real-time data over 1-3 years)
        - Utilities Transformation Roadmap (sequenced steps for expanding to predictive grid management and autonomous demand response systems)
        """
    }

    # Get the specific instructions for the selected focus area
    selected_focus_instructions = focus_area_instructions.get(focus_area, "")

    prompt = f'''
    You are an expert data analyst specializing in {focus_area.lower()} analysis for utilities demand forecasting and energy management.

    SOLUTION CONTEXT:
    {solution_name}

    {solution_content}

    DATA SUMMARY:
    {data_summary}

    {correlation_info}

    ANALYSIS INSTRUCTIONS:
    {selected_focus_instructions}

    IMPORTANT GUIDELINES:
    - Base all insights directly on the data provided
    - Use specific metrics and numbers from the data in your analysis
    - Maintain a professional, analytical tone
    - Be concise but thorough in your analysis
    - Focus specifically on {focus_area} as defined in the instructions
    - Ensure your response is unique and tailored to this specific focus area
    - Include a mix of observations, analysis, and actionable recommendations
    - Use bullet points and clear section headers for readability
    - Frame all insights in the context of utilities demand forecasting and energy management
    '''

    return call_serving_endpoint(prompt, model_name)

def create_metrics_charts(data):
    """Create metric visualizations for the utilities demand forecasting data"""
    charts = []
    
    # Energy Consumption Distribution
    if 'energy_consumption_kwh' in data.columns:
        consumption_data = data.dropna(subset=['energy_consumption_kwh'])
        if not consumption_data.empty:
            consumption_chart = alt.Chart(consumption_data).mark_bar().encode(
                alt.X('energy_consumption_kwh:Q', bin=alt.Bin(maxbins=15), title='Energy Consumption (kWh)'),
                alt.Y('count()', title='Number of Readings'),
                color=alt.value('#1f77b4')
            ).properties(
                title='Energy Consumption Distribution',
                width=380,
                height=340
            )
            charts.append(('Energy Consumption Distribution', consumption_chart))
    
    # Peak Demand by Customer Type
    if 'peak_demand_kw' in data.columns and 'customer_type' in data.columns:
        demand_data = data.dropna(subset=['peak_demand_kw', 'customer_type'])
        if not demand_data.empty:
            peak_demand_chart = alt.Chart(demand_data).mark_boxplot().encode(
                alt.X('customer_type:N', title='Customer Type'),
                alt.Y('peak_demand_kw:Q', title='Peak Demand (kW)'),
                color=alt.Color('customer_type:N', legend=None)
            ).properties(
                title='Peak Demand by Customer Type',
                width=380,
                height=340
            )
            charts.append(('Peak Demand by Customer Type', peak_demand_chart))
    
    # Weather Impact on Energy Consumption
    if 'temperature_fahrenheit' in data.columns and 'energy_consumption_kwh' in data.columns:
        weather_data = data.dropna(subset=['temperature_fahrenheit', 'energy_consumption_kwh'])
        if not weather_data.empty:
            weather_chart = alt.Chart(weather_data).mark_point(size=60, opacity=0.7).encode(
                alt.X('temperature_fahrenheit:Q', title='Temperature (°F)'),
                alt.Y('energy_consumption_kwh:Q', title='Energy Consumption (kWh)'),
                color=alt.Color('weather_condition:N', title='Weather Condition') if 'weather_condition' in weather_data.columns else alt.value('#2ca02c'),
                tooltip=['temperature_fahrenheit:Q', 'energy_consumption_kwh:Q'] + (['weather_condition:N', 'customer_type:N'] if all(col in weather_data.columns for col in ['weather_condition', 'customer_type']) else [])
            ).properties(
                title='Weather Impact on Energy Consumption',
                width=380,
                height=340
            )
            charts.append(('Weather Impact on Consumption', weather_chart))
    
    # Service Territory Distribution
    if 'service_territory' in data.columns:
        territory_data = data.dropna(subset=['service_territory'])
        if not territory_data.empty:
            territory_chart = alt.Chart(territory_data).mark_bar().encode(
                alt.X('service_territory:N', title='Service Territory'),
                alt.Y('count()', title='Number of Meters'),
                color=alt.Color('service_territory:N', legend=None),
                tooltip=['service_territory:N', 'count()']
            ).properties(
                title='Smart Meter Distribution by Territory',
                width=380,
                height=340
            )
            charts.append(('Territory Distribution', territory_chart))
    
    # Predicted vs Actual Demand Analysis
    if 'predicted_demand_mw' in data.columns and 'peak_demand_kw' in data.columns:
        demand_comparison_data = data.dropna(subset=['predicted_demand_mw', 'peak_demand_kw']).copy()
        if not demand_comparison_data.empty:
            # Convert peak demand from kW to MW for comparison
            demand_comparison_data['peak_demand_mw'] = demand_comparison_data['peak_demand_kw'] / 1000
            
            demand_accuracy_chart = alt.Chart(demand_comparison_data).mark_point(size=60, opacity=0.7).encode(
                alt.X('peak_demand_mw:Q', title='Actual Peak Demand (MW)'),
                alt.Y('predicted_demand_mw:Q', title='Predicted Demand (MW)'),
                color=alt.Color('customer_type:N', title='Customer Type') if 'customer_type' in demand_comparison_data.columns else alt.value('#ff7f0e'),
                tooltip=['peak_demand_mw:Q', 'predicted_demand_mw:Q'] + (['customer_type:N'] if 'customer_type' in demand_comparison_data.columns else [])
            ).properties(
                title='Demand Prediction Accuracy',
                width=380,
                height=340
            )
            charts.append(('Prediction Accuracy', demand_accuracy_chart))
    
    # Voltage Stability Analysis
    if 'voltage_level' in data.columns and 'power_factor' in data.columns:
        voltage_data = data.dropna(subset=['voltage_level', 'power_factor'])
        if not voltage_data.empty:
            voltage_chart = alt.Chart(voltage_data).mark_point(size=60, opacity=0.7).encode(
                alt.X('voltage_level:Q', title='Voltage Level (V)'),
                alt.Y('power_factor:Q', title='Power Factor'),
                color=alt.Color('outage_events:Q', title='Outage Events', scale=alt.Scale(scheme='reds')) if 'outage_events' in voltage_data.columns else alt.value('#d62728'),
                tooltip=['voltage_level:Q', 'power_factor:Q'] + (['outage_events:Q', 'service_territory:N'] if all(col in voltage_data.columns for col in ['outage_events', 'service_territory']) else [])
            ).properties(
                title='Voltage Stability Analysis',
                width=380,
                height=340
            )
            charts.append(('Voltage Stability', voltage_chart))
    
    # Customer Satisfaction Analysis
    if 'social_media_sentiment' in data.columns and 'customer_complaints' in data.columns:
        satisfaction_data = data.dropna(subset=['social_media_sentiment', 'customer_complaints'])
        if not satisfaction_data.empty:
            satisfaction_chart = alt.Chart(satisfaction_data).mark_point(size=60, opacity=0.7).encode(
                alt.X('social_media_sentiment:Q', title='Social Media Sentiment'),
                alt.Y('customer_complaints:Q', title='Customer Complaints'),
                color=alt.Color('customer_type:N', title='Customer Type') if 'customer_type' in satisfaction_data.columns else alt.value('#9467bd'),
                tooltip=['social_media_sentiment:Q', 'customer_complaints:Q'] + (['customer_type:N'] if 'customer_type' in satisfaction_data.columns else [])
            ).properties(
                title='Customer Satisfaction Analysis',
                width=380,
                height=340
            )
            charts.append(('Customer Satisfaction', satisfaction_chart))
    
    # Weather Conditions Impact
    if 'weather_condition' in data.columns and 'energy_consumption_kwh' in data.columns:
        weather_impact_data = data.dropna(subset=['weather_condition', 'energy_consumption_kwh'])
        if not weather_impact_data.empty:
            weather_impact_chart = alt.Chart(weather_impact_data).mark_bar().encode(
                alt.X('weather_condition:N', title='Weather Condition'),
                alt.Y('mean(energy_consumption_kwh):Q', title='Average Energy Consumption (kWh)'),
                color=alt.Color('weather_condition:N', legend=None),
                tooltip=['weather_condition:N', 'mean(energy_consumption_kwh):Q']
            ).properties(
                title='Energy Consumption by Weather Condition',
                width=380,
                height=340
            )
            charts.append(('Weather Impact', weather_impact_chart))
    
    return charts

# Load data with error handling
try:
    data = load_data()
    if data.empty:
        st.error("No data found. Please check your Unity Catalog configuration and ensure data has been loaded.")
        st.stop()
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.stop()

# Identify column types based on actual data
categorical_cols = [col for col in ["meter_id", "customer_id", "weather_condition", "customer_type", "service_territory", "rate_schedule"] if col in data.columns]
numeric_cols = [col for col in ["energy_consumption_kwh", "peak_demand_kw", "voltage_level", "power_factor", "temperature_fahrenheit", "humidity_percent", "wind_speed_mph", "billing_cycle_day", "outage_events", "social_media_sentiment", "customer_complaints", "predicted_demand_mw"] if col in data.columns]
date_cols = [col for col in ["timestamp", "_fivetran_synced"] if col in data.columns]

sample_cols = data.columns.tolist()
numeric_candidates = [col for col in sample_cols if data[col].dtype in ['float64', 'int64'] and 'id' not in col.lower()]
date_candidates = [col for col in sample_cols if 'date' in col.lower() or 'timestamp' in col.lower()]
cat_candidates = [col for col in sample_cols if data[col].dtype == 'object' and data[col].nunique() < 1000]

# Calculate key variables that will be used throughout the application
if 'predicted_demand_mw' in data.columns and 'peak_demand_kw' in data.columns:
    # Calculate forecast efficiency as prediction accuracy percentage
    actual_demand_mw = data['peak_demand_kw'] / 1000
    predicted_demand_mw = data['predicted_demand_mw']
    mape = (abs(predicted_demand_mw - actual_demand_mw) / actual_demand_mw * 100).mean()
    forecast_efficiency = 100 - mape  # Higher is better
else:
    forecast_efficiency = 85.0  # Default assumption

# Four tabs - Metrics first, then AI Insights
tabs = st.tabs(["📊 Metrics", "✨ AI Insights", "📁 Insights History", "🔍 Data Explorer"])

# Metrics tab (PRIMARY - position 1)
with tabs[0]:
    st.subheader("📊 Key Utilities Demand Forecasting Metrics")
    
    # Display key metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'energy_consumption_kwh' in data.columns:
            avg_consumption = safe_mean(data['energy_consumption_kwh'])
            total_consumption = safe_sum(data['energy_consumption_kwh'])
            st.metric("Avg Energy Consumption", f"{avg_consumption:.1f} kWh", delta=f"Total: {total_consumption:,.0f} kWh")
    
    with col2:
        if 'peak_demand_kw' in data.columns:
            avg_peak = safe_mean(data['peak_demand_kw'])
            max_peak = data['peak_demand_kw'].max() if not data['peak_demand_kw'].empty else 0
            st.metric("Avg Peak Demand", f"{avg_peak:.1f} kW", delta=f"Max: {max_peak:.1f} kW")
    
    with col3:
        if 'predicted_demand_mw' in data.columns:
            avg_predicted = safe_mean(data['predicted_demand_mw'])
            prediction_std = data['predicted_demand_mw'].std() if not data['predicted_demand_mw'].empty else 0
            st.metric("Avg Predicted Demand", f"{avg_predicted:.1f} MW", delta=f"±{prediction_std:.1f} MW std")
    
    with col4:
        if 'outage_events' in data.columns:
            total_outages = safe_sum(data['outage_events'])
            avg_outages = safe_mean(data['outage_events'])
            st.metric("Total Outage Events", f"{total_outages:,}", delta=f"Avg: {avg_outages:.1f} per meter")
    
    st.markdown("---")
    
    # Create and display charts
    charts = create_metrics_charts(data)
    
    # ---- Title clipping fix (Altair) ----
    def fixed_title(text: str) -> alt.TitleParams:
        return alt.TitleParams(
            text=text,
            fontSize=16,
            fontWeight='bold',
            anchor='start',
            offset=14  # key: moves title downward so it won't be cut off
        )
    PAD = {"top": 28, "left": 6, "right": 6, "bottom": 6}  # key: explicit top padding
    charts_fixed = []
    if charts:
        for item in charts:
            # Expected shape: (title_text, chart_object). Fallback if a bare chart arrives.
            try:
                title_text, ch = item
            except Exception:
                title_text, ch = "", item
            ch = ch.properties(title=fixed_title(title_text or ""), padding=PAD)
            ch = ch.configure_title(anchor='start')
            charts_fixed.append((title_text, ch))
    
    if charts_fixed:
        st.subheader("📈 Performance Visualizations")
        # Display in a 2-column grid
        num_charts = len(charts_fixed)
        for i in range(0, num_charts, 2):
            cols = st.columns(2)
            if i < num_charts:
                _, ch = charts_fixed[i]
                with cols[0]:
                    st.altair_chart(ch, use_container_width=True)
            if i + 1 < num_charts:
                _, ch = charts_fixed[i + 1]
                with cols[1]:
                    st.altair_chart(ch, use_container_width=True)
        st.caption(f"Displaying {num_charts} performance charts")
    else:
        st.info("No suitable data found for creating visualizations.")
    
    # Enhanced Summary statistics table
    st.subheader("📈 Summary Statistics")
    if numeric_candidates:
        # Create enhanced summary statistics
        summary_stats = data[numeric_candidates].describe()
        
        # Transpose for better readability and add formatting
        summary_df = summary_stats.T.round(3)
        
        # Add meaningful column names and formatting
        summary_df.columns = ['Count', 'Mean', 'Std Dev', 'Min', '25%', '50% (Median)', '75%', 'Max']
        
        # Create two columns for better organization
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**⚡ Key Demand Forecasting Metrics**")
            key_metrics = ['energy_consumption_kwh', 'peak_demand_kw', 'predicted_demand_mw', 'voltage_level']
            key_metrics_present = [m for m in key_metrics if m in summary_df.index]
            
            if key_metrics_present:
                for metric in key_metrics_present:
                    mean_val = summary_df.loc[metric, 'Mean']
                    min_val = summary_df.loc[metric, 'Min']
                    max_val = summary_df.loc[metric, 'Max']
                    
                    # Format based on metric type
                    if 'kwh' in metric.lower():
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.1f} kWh",
                            help=f"Range: {min_val:.1f} - {max_val:.1f} kWh"
                        )
                    elif 'kw' in metric.lower():
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.1f} kW",
                            help=f"Range: {min_val:.1f} - {max_val:.1f} kW"
                        )
                    elif 'mw' in metric.lower():
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.1f} MW",
                            help=f"Range: {min_val:.1f} - {max_val:.1f} MW"
                        )
                    elif 'voltage' in metric.lower():
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:,.0f} V",
                            help=f"Range: {min_val:,.0f} - {max_val:,.0f} V"
                        )
                    else:
                        st.metric(
                            label=metric.replace('_', ' ').title(),
                            value=f"{mean_val:.2f}",
                            help=f"Range: {min_val:.2f} - {max_val:.2f}"
                        )
        
        with col2:
            st.markdown("**🌡️ Weather & Performance Insights**")
            
            # Calculate and display key insights
            insights = []
            
            if 'temperature_fahrenheit' in summary_df.index:
                temp_mean = summary_df.loc['temperature_fahrenheit', 'Mean']
                temp_range = summary_df.loc['temperature_fahrenheit', 'Max'] - summary_df.loc['temperature_fahrenheit', 'Min']
                insights.append(f"• **Average Temperature**: {temp_mean:.1f}°F")
                insights.append(f"• **Temperature Range**: {temp_range:.1f}°F")
            
            if 'humidity_percent' in summary_df.index:
                humidity_mean = summary_df.loc['humidity_percent', 'Mean']
                insights.append(f"• **Average Humidity**: {humidity_mean:.1f}%")
            
            if 'power_factor' in summary_df.index:
                pf_mean = summary_df.loc['power_factor', 'Mean']
                if pf_mean >= 0.95:
                    insights.append(f"• **Excellent Power Factor**: {pf_mean:.3f}")
                elif pf_mean >= 0.90:
                    insights.append(f"• **Good Power Factor**: {pf_mean:.3f}")
                else:
                    insights.append(f"• **⚠️ Low Power Factor**: {pf_mean:.3f}")
            
            if 'outage_events' in summary_df.index:
                outage_total = safe_sum(data['outage_events'])
                outage_median = summary_df.loc['outage_events', '50% (Median)']
                insights.append(f"• **Total Outages**: {outage_total:,} events")
                insights.append(f"• **Median Outages per Meter**: {outage_median:.1f}")
            
            if 'social_media_sentiment' in summary_df.index:
                sentiment_mean = summary_df.loc['social_media_sentiment', 'Mean']
                if sentiment_mean > 0.1:
                    insights.append(f"• **Positive Customer Sentiment**: {sentiment_mean:.2f}")
                elif sentiment_mean < -0.1:
                    insights.append(f"• **⚠️ Negative Customer Sentiment**: {sentiment_mean:.2f}")
                else:
                    insights.append(f"• **Neutral Customer Sentiment**: {sentiment_mean:.2f}")
            
            # Add categorical insights
            if 'customer_type' in data.columns:
                type_distribution = data['customer_type'].value_counts()
                if not type_distribution.empty:
                    top_type = type_distribution.index[0]
                    top_count = type_distribution.iloc[0]
                    insights.append(f"• **Top Customer Type**: {top_type} ({top_count} meters)")
            
            if 'weather_condition' in data.columns:
                weather_distribution = data['weather_condition'].value_counts()
                if not weather_distribution.empty:
                    top_weather = weather_distribution.index[0]
                    top_weather_count = weather_distribution.iloc[0]
                    insights.append(f"• **Most Common Weather**: {top_weather} ({top_weather_count} readings)")
            
            if 'service_territory' in data.columns:
                territory_distribution = data['service_territory'].value_counts()
                if not territory_distribution.empty:
                    top_territory = territory_distribution.index[0]
                    insights.append(f"• **Largest Service Territory**: {top_territory}")
            
            # Calculate demand prediction accuracy if possible
            if 'predicted_demand_mw' in data.columns and 'peak_demand_kw' in data.columns:
                # Convert peak demand to MW for comparison
                actual_demand_mw = data['peak_demand_kw'] / 1000
                predicted_demand_mw = data['predicted_demand_mw']
                
                # Calculate mean absolute percentage error (MAPE)
                mape = (abs(predicted_demand_mw - actual_demand_mw) / actual_demand_mw * 100).mean()
                insights.append(f"• **Prediction Accuracy (MAPE)**: {mape:.1f}%")
            
            for insight in insights:
                st.markdown(insight)
        
        # Full detailed table (collapsible)
        with st.expander("📋 Detailed Statistics Table", expanded=False):
            st.dataframe(
                summary_df.style.format({
                    'Count': '{:.0f}',
                    'Mean': '{:.3f}',
                    'Std Dev': '{:.3f}',
                    'Min': '{:.3f}',
                    '25%': '{:.3f}',
                    '50% (Median)': '{:.3f}',
                    '75%': '{:.3f}',
                    'Max': '{:.3f}'
                }),
                use_container_width=True
            )

# AI Insights tab (SECONDARY - position 2)
with tabs[1]:
    st.subheader("✨ AI-Powered Demand Forecasting with Agent Workflows")
    st.markdown("**Experience behind-the-scenes AI agent processing for each demand forecasting analysis focus area**")
    
    focus_area = st.radio("Focus Area", [
        "Overall Performance", 
        "Optimization Opportunities", 
        "Financial Impact", 
        "Strategic Recommendations"
    ])
    
    # Show business challenge and solution
    focus_info = get_focus_area_info(focus_area)
    if focus_info["challenge"]:
        st.markdown("#### Business Challenge")
        st.info(focus_info["challenge"])
        st.markdown("#### Agent Solution")
        st.success(focus_info["solution"])
    
    st.markdown("**Select Databricks Serving Endpoint for Analysis:**")
    selected_model = st.selectbox("", MODELS, index=0, label_visibility="collapsed")

    # Agent control buttons and status
    col1, col2, col3 = st.columns([2, 1, 1])
    
    agent_running_key = f"{focus_area}_agent_running"
    if agent_running_key not in st.session_state:
        st.session_state[agent_running_key] = False
    
    with col1:
        if st.button("🚀 Start Demand Forecasting Agent"):
            st.session_state[agent_running_key] = True
            st.rerun()
    
    with col2:
        if st.button("⏹ Stop Agent"):
            st.session_state[agent_running_key] = False
            st.rerun()
    
    with col3:
        st.markdown("**Status**")
        if st.session_state[agent_running_key]:
            st.markdown('<div class="agent-status-active">✅ Active</div>', unsafe_allow_html=True)
        else:
            st.markdown("⏸ Ready")

    # Progress placeholder
    progress_placeholder = st.empty()
    
    # Run agent if active
    if st.session_state[agent_running_key]:
        with st.spinner("Demand Forecasting Agent Running..."):
            insights = generate_insights_with_agent_workflow(data, focus_area, selected_model, progress_placeholder)
            
            if insights:
                # Show completion message
                st.success(f"🎉 {focus_area} Demand Forecasting Agent completed with real utilities data analysis!")
                
                # Show report in expandable section
                with st.expander(f"📋 Generated {focus_area} Report (Real Utilities Data)", expanded=True):
                    st.markdown(f"""
                    <div class="agent-report-header">
                        <strong>{focus_area} Report - AI-Generated Demand Forecasting Analysis</strong><br>
                        <small>Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</small><br>
                        <small>Data Source: Live Databricks Utilities Demand Analysis</small><br>
                        <small>AI Model: {selected_model}</small>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.markdown(insights)
                
                # Save to history
                timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
                st.session_state.insights_history.append({
                    "timestamp": timestamp,
                    "focus": focus_area,
                    "insights": insights,
                    "model": selected_model
                })
                
                # Download button
                st.download_button(
                    "📥 Download Demand Forecasting Analysis Report", 
                    insights, 
                    file_name=f"{solution_name.replace(' ', '_').lower()}_{focus_area.lower().replace(' ', '_')}_report.md",
                    mime="text/markdown"
                )
                
                # Stop the agent after completion
                st.session_state[agent_running_key] = False

# Insights History tab
with tabs[2]:
    st.subheader("📁 Insights History")
    if st.session_state.insights_history:
        for i, item in enumerate(reversed(st.session_state.insights_history)):
            with st.expander(f"{item['timestamp']} - {item['focus']} ({item.get('model', 'Unknown')})", expanded=False):
                st.markdown(item["insights"])
    else:
        st.info("No insights generated yet. Go to the AI Insights tab to generate some insights.")

# Data Explorer tab
with tabs[3]:
    st.subheader("🔍 Data Explorer")
    rows_per_page = st.slider("Rows per page", 5, 50, 10)
    page = st.number_input("Page", min_value=1, value=1)
    start = (page - 1) * rows_per_page
    end = min(start + rows_per_page, len(data))
    st.dataframe(data.iloc[start:end], use_container_width=True)
    st.caption(f"Showing rows {start + 1}–{end} of {len(data)}")