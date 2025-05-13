import pandas as pd
import sqlite3
import os

def load_data(file_path):
    """Load the crash dataset from CSV file"""
    return pd.read_csv(file_path)

def create_database(db_name='crash_data_warehouse.db'):
    """Create SQLite database for the data warehouse"""
    # If database exists, remove it to start fresh
    if os.path.exists(db_name):
        os.remove(db_name)
    
    # Connect to database
    conn = sqlite3.connect(db_name)
    print(f"Created database: {db_name}")
    return conn

def create_dimension_tables(conn):
    """Create dimension tables for the data warehouse"""
    cursor = conn.cursor()
    
    # Create Date dimension
    cursor.execute('''
    CREATE TABLE dim_date (
        date_id INTEGER PRIMARY KEY,
        year INTEGER,
        month INTEGER,
        month_name TEXT,
        day_of_week TEXT,
        is_weekend INTEGER,
        is_holiday INTEGER,
        time_of_day TEXT
    )
    ''')
    
    # Create Location dimension
    cursor.execute('''
    CREATE TABLE dim_location (
        location_id INTEGER PRIMARY KEY,
        state TEXT,
        remoteness_area TEXT,
        sa4_name TEXT,
        lga_name TEXT
    )
    ''')
    
    # Create Road dimension
    cursor.execute('''
    CREATE TABLE dim_road (
        road_id INTEGER PRIMARY KEY,
        road_type TEXT,
        speed_limit INTEGER
    )
    ''')
    
    # Create Person dimension
    cursor.execute('''
    CREATE TABLE dim_person (
        person_id INTEGER PRIMARY KEY,
        road_user_type TEXT,
        gender TEXT,
        age INTEGER,
        age_group TEXT
    )
    ''')
    
    # Create Vehicle dimension
    cursor.execute('''
    CREATE TABLE dim_vehicle (
        vehicle_id INTEGER PRIMARY KEY,
        bus_involvement TEXT,
        heavy_rigid_truck_involvement TEXT,
        articulated_truck_involvement TEXT
    )
    ''')
    
    print("Created dimension tables")
    conn.commit()

def create_fact_table(conn):
    """Create fact table for crashes"""
    cursor = conn.cursor()
    
    # Create Crash fact table
    cursor.execute('''
    CREATE TABLE fact_crash (
        crash_id INTEGER PRIMARY KEY,
        original_id TEXT,
        date_id INTEGER,
        location_id INTEGER,
        road_id INTEGER,
        person_id INTEGER,
        vehicle_id INTEGER,
        crash_type TEXT,
        number_fatalities INTEGER,
        christmas_period INTEGER,
        easter_period INTEGER,
        FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
        FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
        FOREIGN KEY (road_id) REFERENCES dim_road(road_id),
        FOREIGN KEY (person_id) REFERENCES dim_person(person_id),
        FOREIGN KEY (vehicle_id) REFERENCES dim_vehicle(vehicle_id)
    )
    ''')
    
    print("Created fact table")
    conn.commit()

def process_date_dimension(df, conn):
    """Process and insert data into date dimension"""
    # Map month numbers to names
    month_map = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    
    # Create unique combinations of date-related columns
    date_data = df[['Year', 'Month', 'Dayweek', 'Time of day']].drop_duplicates().reset_index(drop=True)
    
    # Add derived columns
    date_data['month_name'] = date_data['Month'].map(month_map)
    date_data['is_weekend'] = date_data['Dayweek'].apply(lambda x: 1 if x in ['Saturday', 'Sunday'] else 0)
    # Default holiday flag (would need more data to determine actual holidays)
    date_data['is_holiday'] = 0
    
    # Add unique date_id
    date_data['date_id'] = date_data.index + 1
    
    # Insert into database
    date_data_for_db = date_data.rename(columns={
        'Year': 'year',
        'Month': 'month',
        'Dayweek': 'day_of_week',
        'Time of day': 'time_of_day'
    })
    
    date_data_for_db[['date_id', 'year', 'month', 'month_name', 'day_of_week', 
                     'is_weekend', 'is_holiday', 'time_of_day']].to_sql(
        'dim_date', conn, if_exists='append', index=False
    )
    
    print(f"Processed {len(date_data)} rows for date dimension")
    
    # Create mapping for fact table
    date_mapping = date_data[['Year', 'Month', 'Dayweek', 'Time of day', 'date_id']]
    return date_mapping

def process_location_dimension(df, conn):
    """Process and insert data into location dimension"""
    # Create unique combinations of location-related columns
    location_data = df[['State', 'National Remoteness Areas', 'SA4 Name 2021', 
                        'National LGA Name 2024']].drop_duplicates().reset_index(drop=True)
    
    # Add unique location_id
    location_data['location_id'] = location_data.index + 1
    
    # Insert into database
    location_data_for_db = location_data.rename(columns={
        'State': 'state',
        'National Remoteness Areas': 'remoteness_area',
        'SA4 Name 2021': 'sa4_name',
        'National LGA Name 2024': 'lga_name'
    })
    
    location_data_for_db[['location_id', 'state', 'remoteness_area', 
                         'sa4_name', 'lga_name']].to_sql(
        'dim_location', conn, if_exists='append', index=False
    )
    
    print(f"Processed {len(location_data)} rows for location dimension")
    
    # Create mapping for fact table
    location_mapping = location_data[['State', 'National Remoteness Areas', 
                                     'SA4 Name 2021', 'National LGA Name 2024', 'location_id']]
    return location_mapping

def process_road_dimension(df, conn):
    """Process and insert data into road dimension"""
    # Create unique combinations of road-related columns
    road_data = df[['National Road Type', 'Speed Limit']].drop_duplicates().reset_index(drop=True)
    
    # Add unique road_id
    road_data['road_id'] = road_data.index + 1
    
    # Insert into database
    road_data_for_db = road_data.rename(columns={
        'National Road Type': 'road_type',
        'Speed Limit': 'speed_limit'
    })
    
    road_data_for_db[['road_id', 'road_type', 'speed_limit']].to_sql(
        'dim_road', conn, if_exists='append', index=False
    )
    
    print(f"Processed {len(road_data)} rows for road dimension")
    
    # Create mapping for fact table
    road_mapping = road_data[['National Road Type', 'Speed Limit', 'road_id']]
    return road_mapping

def process_person_dimension(df, conn):
    """Process and insert data into person dimension"""
    # Create unique combinations of person-related columns
    person_data = df[['Road User', 'Gender', 'Age', 'Age Group']].drop_duplicates().reset_index(drop=True)
    
    # Add unique person_id
    person_data['person_id'] = person_data.index + 1
    
    # Insert into database
    person_data_for_db = person_data.rename(columns={
        'Road User': 'road_user_type',
        'Gender': 'gender',
        'Age': 'age',
        'Age Group': 'age_group'
    })
    
    person_data_for_db[['person_id', 'road_user_type', 'gender', 
                       'age', 'age_group']].to_sql(
        'dim_person', conn, if_exists='append', index=False
    )
    
    print(f"Processed {len(person_data)} rows for person dimension")
    
    # Create mapping for fact table
    person_mapping = person_data[['Road User', 'Gender', 'Age', 'Age Group', 'person_id']]
    return person_mapping

def process_vehicle_dimension(df, conn):
    """Process and insert data into vehicle dimension"""
    # Create unique combinations of vehicle-related columns
    vehicle_data = df[['Bus Involvement', 'Heavy Rigid Truck Involvement', 
                      'Articulated Truck Involvement']].drop_duplicates().reset_index(drop=True)
    
    # Add unique vehicle_id
    vehicle_data['vehicle_id'] = vehicle_data.index + 1
    
    # Insert into database
    vehicle_data_for_db = vehicle_data.rename(columns={
        'Bus Involvement': 'bus_involvement',
        'Heavy Rigid Truck Involvement': 'heavy_rigid_truck_involvement',
        'Articulated Truck Involvement': 'articulated_truck_involvement'
    })
    
    vehicle_data_for_db[['vehicle_id', 'bus_involvement', 
                        'heavy_rigid_truck_involvement', 
                        'articulated_truck_involvement']].to_sql(
        'dim_vehicle', conn, if_exists='append', index=False
    )
    
    print(f"Processed {len(vehicle_data)} rows for vehicle dimension")
    
    # Create mapping for fact table
    vehicle_mapping = vehicle_data[['Bus Involvement', 'Heavy Rigid Truck Involvement', 
                                   'Articulated Truck Involvement', 'vehicle_id']]
    return vehicle_mapping

def process_fact_table(df, conn, dimension_mappings):
    """Process and insert data into fact table"""
    # Extract needed mappings
    date_mapping = dimension_mappings['date']
    location_mapping = dimension_mappings['location']
    road_mapping = dimension_mappings['road']
    person_mapping = dimension_mappings['person']
    vehicle_mapping = dimension_mappings['vehicle']
    
    # Create temporary working dataframe
    fact_data = df[['ID', 'Crash ID', 'Year', 'Month', 'Dayweek', 'Time of day',
                   'State', 'National Remoteness Areas', 'SA4 Name 2021', 'National LGA Name 2024',
                   'National Road Type', 'Speed Limit',
                   'Road User', 'Gender', 'Age', 'Age Group',
                   'Bus Involvement', 'Heavy Rigid Truck Involvement', 'Articulated Truck Involvement',
                   'Crash Type', 'Number Fatalities', 'Christmas Period', 'Easter Period']].copy()
    
    # Convert Yes/No to 1/0 for binary fields
    fact_data['Christmas Period'] = fact_data['Christmas Period'].map({'Yes': 1, 'No': 0})
    fact_data['Easter Period'] = fact_data['Easter Period'].map({'Yes': 1, 'No': 0})
    
    # Merge with dimension mappings to get foreign keys
    fact_data = pd.merge(
        fact_data,
        date_mapping,
        on=['Year', 'Month', 'Dayweek', 'Time of day'],
        how='left'
    )
    
    fact_data = pd.merge(
        fact_data,
        location_mapping,
        on=['State', 'National Remoteness Areas', 'SA4 Name 2021', 'National LGA Name 2024'],
        how='left'
    )
    
    fact_data = pd.merge(
        fact_data,
        road_mapping,
        on=['National Road Type', 'Speed Limit'],
        how='left'
    )
    
    fact_data = pd.merge(
        fact_data,
        person_mapping,
        on=['Road User', 'Gender', 'Age', 'Age Group'],
        how='left'
    )
    
    fact_data = pd.merge(
        fact_data,
        vehicle_mapping,
        on=['Bus Involvement', 'Heavy Rigid Truck Involvement', 'Articulated Truck Involvement'],
        how='left'
    )
    
    # Prepare for database
    fact_data_for_db = fact_data[['ID', 'Crash ID', 'date_id', 'location_id', 'road_id', 
                                 'person_id', 'vehicle_id', 'Crash Type', 
                                 'Number Fatalities', 'Christmas Period', 'Easter Period']]
    
    fact_data_for_db.rename(columns={
        'ID': 'crash_id',
        'Crash ID': 'original_id',
        'Crash Type': 'crash_type',
        'Number Fatalities': 'number_fatalities',
        'Christmas Period': 'christmas_period',
        'Easter Period': 'easter_period'
    }, inplace=True)
    
    # Insert into database
    fact_data_for_db.to_sql('fact_crash', conn, if_exists='append', index=False)
    
    print(f"Processed {len(fact_data)} rows for fact table")

def create_data_warehouse():
    """Main function to create the data warehouse"""
    # Load data
    print("Loading data...")
    df = load_data("Project2_Dataset_Corrected.csv")
    
    # Create database
    print("Creating database...")
    conn = create_database()
    
    # Create dimension and fact tables
    print("Creating tables...")
    create_dimension_tables(conn)
    create_fact_table(conn)
    
    # Process dimensions
    print("Processing dimensions...")
    date_mapping = process_date_dimension(df, conn)
    location_mapping = process_location_dimension(df, conn)
    road_mapping = process_road_dimension(df, conn)
    person_mapping = process_person_dimension(df, conn)
    vehicle_mapping = process_vehicle_dimension(df, conn)
    
    # Collect all dimension mappings
    dimension_mappings = {
        'date': date_mapping,
        'location': location_mapping,
        'road': road_mapping,
        'person': person_mapping,
        'vehicle': vehicle_mapping
    }
    
    # Process fact table
    print("Processing fact table...")
    process_fact_table(df, conn, dimension_mappings)
    
    # Close connection
    conn.close()
    print("Data warehouse creation complete!")

if __name__ == "__main__":
    create_data_warehouse() 