# Data Warehouse Project - Crash Analysis

This project involves creating a data warehouse for analyzing crash data. The data warehouse is designed using a star schema with dimension and fact tables to facilitate efficient analysis and reporting of crash statistics.

## Project Structure

- `analyze_data.py`: Script for exploratory data analysis and basic visualization
- `create_data_warehouse.py`: Script to create the data warehouse schema and ETL process
- `query_data_warehouse.py`: Script to query the data warehouse and generate analytical reports
- `main.py`: Main script to run the entire pipeline

## Data Model

The data warehouse uses a star schema with the following structure:

### Fact Table
- `fact_crash`: Contains the crash events and measures

### Dimension Tables
- `dim_date`: Date and time-related attributes
- `dim_location`: Geographic location attributes
- `dim_road`: Road-related attributes
- `dim_person`: Person-related attributes
- `dim_vehicle`: Vehicle-related attributes

## Setup and Installation

1. Ensure Python 3.7+ is installed
2. Install required packages:
   ```
   pip install pandas matplotlib seaborn
   ```
3. Place the dataset file `Project2_Dataset_Corrected.csv` in the project root directory

## Running the Project

To run the complete pipeline:

```
python main.py
```

This will:
1. Run exploratory data analysis
2. Create the data warehouse
3. Query the data warehouse and generate reports

## Output Files

- `visualizations/`: Contains exploratory data visualizations
- `crash_data_warehouse.db`: SQLite database containing the data warehouse
- `reports/`: Contains analytical reports and visualizations
- `reports/summary_report.txt`: Text summary of key findings

## Individual Script Execution

You can also run each script individually:

```
python analyze_data.py        # Run exploratory analysis only
python create_data_warehouse.py  # Create data warehouse only
python query_data_warehouse.py   # Query warehouse and generate reports only
```

## Project Dataset

The project uses the `Project2_Dataset_Corrected.csv` file, which contains crash data with the following attributes:

- ID: Unique identifier for each record
- Crash ID: Identifier for the crash event
- State: State where crash occurred
- Month/Year: Time of crash
- Dayweek: Day of the week
- Time: Time of crash
- Crash Type: Single or Multiple vehicles
- Number Fatalities: Number of fatalities
- Vehicle involvement (Bus, Heavy Rigid Truck, Articulated Truck)
- Speed Limit: Speed limit at crash location
- Road User: Type of road user (Driver, Passenger, Pedestrian, etc.)
- Gender: Gender of person involved
- Age: Age of person involved
- Remoteness Areas: Geographic classification
- Road Type: Type of road
- Christmas/Easter Period: Whether crash occurred during holiday periods
- And more geographical and temporal attributes