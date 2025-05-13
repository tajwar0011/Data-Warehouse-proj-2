import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Set the style for the plots
plt.style.use('ggplot')
sns.set(font_scale=1.2)

def connect_to_warehouse(db_name='crash_data_warehouse.db'):
    """Connect to the SQLite database"""
    if not os.path.exists(db_name):
        raise FileNotFoundError(f"Database '{db_name}' not found. Run create_data_warehouse.py first.")
    
    conn = sqlite3.connect(db_name)
    print(f"Connected to database: {db_name}")
    return conn

def create_output_dir():
    """Create directory for output reports"""
    if not os.path.exists('reports'):
        os.makedirs('reports')
    print("Created reports directory")

def fatalities_by_state(conn):
    """Query fatalities by state and create visualization"""
    query = """
    SELECT l.state, SUM(f.number_fatalities) as total_fatalities
    FROM fact_crash f
    JOIN dim_location l ON f.location_id = l.location_id
    GROUP BY l.state
    ORDER BY total_fatalities DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(x='state', y='total_fatalities', data=df)
    plt.title('Total Fatalities by State')
    plt.xlabel('State')
    plt.ylabel('Number of Fatalities')
    plt.tight_layout()
    plt.savefig('reports/fatalities_by_state.png')
    plt.close()
    
    print("Generated report: fatalities_by_state.png")
    return df

def fatalities_by_road_type(conn):
    """Query fatalities by road type and create visualization"""
    query = """
    SELECT r.road_type, SUM(f.number_fatalities) as total_fatalities
    FROM fact_crash f
    JOIN dim_road r ON f.road_id = r.road_id
    GROUP BY r.road_type
    ORDER BY total_fatalities DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    plt.figure(figsize=(14, 7))
    sns.barplot(x='road_type', y='total_fatalities', data=df)
    plt.title('Total Fatalities by Road Type')
    plt.xlabel('Road Type')
    plt.ylabel('Number of Fatalities')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('reports/fatalities_by_road_type.png')
    plt.close()
    
    print("Generated report: fatalities_by_road_type.png")
    return df

def fatalities_by_age_group(conn):
    """Query fatalities by age group and create visualization"""
    query = """
    SELECT p.age_group, SUM(f.number_fatalities) as total_fatalities
    FROM fact_crash f
    JOIN dim_person p ON f.person_id = p.person_id
    GROUP BY p.age_group
    ORDER BY 
        CASE 
            WHEN p.age_group = '0_to_16' THEN 1
            WHEN p.age_group = '17_to_25' THEN 2
            WHEN p.age_group = '26_to_39' THEN 3
            WHEN p.age_group = '40_to_64' THEN 4
            WHEN p.age_group = '65_to_74' THEN 5
            WHEN p.age_group = '75_or_older' THEN 6
            ELSE 7
        END
    """
    
    df = pd.read_sql_query(query, conn)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(x='age_group', y='total_fatalities', data=df)
    plt.title('Total Fatalities by Age Group')
    plt.xlabel('Age Group')
    plt.ylabel('Number of Fatalities')
    plt.tight_layout()
    plt.savefig('reports/fatalities_by_age_group.png')
    plt.close()
    
    print("Generated report: fatalities_by_age_group.png")
    return df

def fatalities_by_gender(conn):
    """Query fatalities by gender and create visualization"""
    query = """
    SELECT p.gender, SUM(f.number_fatalities) as total_fatalities
    FROM fact_crash f
    JOIN dim_person p ON f.person_id = p.person_id
    GROUP BY p.gender
    """
    
    df = pd.read_sql_query(query, conn)
    
    plt.figure(figsize=(10, 6))
    sns.barplot(x='gender', y='total_fatalities', data=df)
    plt.title('Total Fatalities by Gender')
    plt.xlabel('Gender')
    plt.ylabel('Number of Fatalities')
    plt.tight_layout()
    plt.savefig('reports/fatalities_by_gender.png')
    plt.close()
    
    print("Generated report: fatalities_by_gender.png")
    return df

def fatalities_by_time_of_day(conn):
    """Query fatalities by time of day and create visualization"""
    query = """
    SELECT d.time_of_day, SUM(f.number_fatalities) as total_fatalities
    FROM fact_crash f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.time_of_day
    """
    
    df = pd.read_sql_query(query, conn)
    
    plt.figure(figsize=(10, 6))
    sns.barplot(x='time_of_day', y='total_fatalities', data=df)
    plt.title('Total Fatalities by Time of Day')
    plt.xlabel('Time of Day')
    plt.ylabel('Number of Fatalities')
    plt.tight_layout()
    plt.savefig('reports/fatalities_by_time_of_day.png')
    plt.close()
    
    print("Generated report: fatalities_by_time_of_day.png")
    return df

def fatalities_by_day_of_week(conn):
    """Query fatalities by day of week and create visualization"""
    query = """
    SELECT d.day_of_week, SUM(f.number_fatalities) as total_fatalities
    FROM fact_crash f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.day_of_week
    ORDER BY 
        CASE d.day_of_week
            WHEN 'Monday' THEN 1
            WHEN 'Tuesday' THEN 2
            WHEN 'Wednesday' THEN 3
            WHEN 'Thursday' THEN 4
            WHEN 'Friday' THEN 5
            WHEN 'Saturday' THEN 6
            WHEN 'Sunday' THEN 7
        END
    """
    
    df = pd.read_sql_query(query, conn)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(x='day_of_week', y='total_fatalities', data=df)
    plt.title('Total Fatalities by Day of Week')
    plt.xlabel('Day of Week')
    plt.ylabel('Number of Fatalities')
    plt.tight_layout()
    plt.savefig('reports/fatalities_by_day_of_week.png')
    plt.close()
    
    print("Generated report: fatalities_by_day_of_week.png")
    return df

def fatalities_by_speed_limit(conn):
    """Query fatalities by speed limit and create visualization"""
    query = """
    SELECT r.speed_limit, SUM(f.number_fatalities) as total_fatalities
    FROM fact_crash f
    JOIN dim_road r ON f.road_id = r.road_id
    GROUP BY r.speed_limit
    ORDER BY r.speed_limit
    """
    
    df = pd.read_sql_query(query, conn)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(x='speed_limit', y='total_fatalities', data=df)
    plt.title('Total Fatalities by Speed Limit')
    plt.xlabel('Speed Limit')
    plt.ylabel('Number of Fatalities')
    plt.tight_layout()
    plt.savefig('reports/fatalities_by_speed_limit.png')
    plt.close()
    
    print("Generated report: fatalities_by_speed_limit.png")
    return df

def fatalities_by_road_user(conn):
    """Query fatalities by road user type and create visualization"""
    query = """
    SELECT p.road_user_type, SUM(f.number_fatalities) as total_fatalities
    FROM fact_crash f
    JOIN dim_person p ON f.person_id = p.person_id
    GROUP BY p.road_user_type
    ORDER BY total_fatalities DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    plt.figure(figsize=(12, 6))
    sns.barplot(x='road_user_type', y='total_fatalities', data=df)
    plt.title('Total Fatalities by Road User Type')
    plt.xlabel('Road User Type')
    plt.ylabel('Number of Fatalities')
    plt.tight_layout()
    plt.savefig('reports/fatalities_by_road_user.png')
    plt.close()
    
    print("Generated report: fatalities_by_road_user.png")
    return df

def fatalities_by_remoteness(conn):
    """Query fatalities by remoteness area and create visualization"""
    query = """
    SELECT l.remoteness_area, SUM(f.number_fatalities) as total_fatalities
    FROM fact_crash f
    JOIN dim_location l ON f.location_id = l.location_id
    GROUP BY l.remoteness_area
    ORDER BY total_fatalities DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    plt.figure(figsize=(14, 7))
    sns.barplot(x='remoteness_area', y='total_fatalities', data=df)
    plt.title('Total Fatalities by Remoteness Area')
    plt.xlabel('Remoteness Area')
    plt.ylabel('Number of Fatalities')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('reports/fatalities_by_remoteness.png')
    plt.close()
    
    print("Generated report: fatalities_by_remoteness.png")
    return df

def generate_summary_report(results):
    """Generate a comprehensive summary report"""
    with open('reports/summary_report.txt', 'w') as f:
        f.write("=== CRASH DATA WAREHOUSE SUMMARY REPORT ===\n\n")
        
        # Total fatalities
        total_fatalities = sum(df['total_fatalities'].sum() for df in results.values())
        f.write(f"Total fatalities: {total_fatalities}\n\n")
        
        # Top states by fatalities
        f.write("--- Top States by Fatalities ---\n")
        for i, row in results['by_state'].head(3).iterrows():
            f.write(f"{row['state']}: {row['total_fatalities']} fatalities\n")
        f.write("\n")
        
        # Top road types by fatalities
        f.write("--- Top Road Types by Fatalities ---\n")
        for i, row in results['by_road_type'].head(3).iterrows():
            f.write(f"{row['road_type']}: {row['total_fatalities']} fatalities\n")
        f.write("\n")
        
        # Fatalities by age group
        f.write("--- Fatalities by Age Group ---\n")
        for i, row in results['by_age_group'].iterrows():
            f.write(f"{row['age_group']}: {row['total_fatalities']} fatalities\n")
        f.write("\n")
        
        # Fatalities by gender
        f.write("--- Fatalities by Gender ---\n")
        for i, row in results['by_gender'].iterrows():
            f.write(f"{row['gender']}: {row['total_fatalities']} fatalities\n")
        f.write("\n")
        
        # Fatalities by time of day
        f.write("--- Fatalities by Time of Day ---\n")
        for i, row in results['by_time_of_day'].iterrows():
            f.write(f"{row['time_of_day']}: {row['total_fatalities']} fatalities\n")
        f.write("\n")
        
        # Fatalities by day of week
        f.write("--- Fatalities by Day of Week ---\n")
        for i, row in results['by_day_of_week'].iterrows():
            f.write(f"{row['day_of_week']}: {row['total_fatalities']} fatalities\n")
        f.write("\n")
        
        # Fatalities by speed limit (top 3)
        f.write("--- Top Speed Limits by Fatalities ---\n")
        top_speed_limits = results['by_speed_limit'].sort_values('total_fatalities', ascending=False).head(3)
        for i, row in top_speed_limits.iterrows():
            f.write(f"{row['speed_limit']} km/h: {row['total_fatalities']} fatalities\n")
        f.write("\n")
        
        # Fatalities by road user type
        f.write("--- Fatalities by Road User Type ---\n")
        for i, row in results['by_road_user'].iterrows():
            f.write(f"{row['road_user_type']}: {row['total_fatalities']} fatalities\n")
        f.write("\n")
        
        # Fatalities by remoteness area
        f.write("--- Fatalities by Remoteness Area ---\n")
        for i, row in results['by_remoteness'].iterrows():
            f.write(f"{row['remoteness_area']}: {row['total_fatalities']} fatalities\n")
        f.write("\n")
        
        f.write("=== END OF REPORT ===\n")
    
    print("Generated summary report: summary_report.txt")

def analyze_data_warehouse():
    """Main function to query and analyze data warehouse"""
    # Connect to the database
    conn = connect_to_warehouse()
    
    # Create output directory
    create_output_dir()
    
    # Execute queries and create visualizations
    results = {
        'by_state': fatalities_by_state(conn),
        'by_road_type': fatalities_by_road_type(conn),
        'by_age_group': fatalities_by_age_group(conn),
        'by_gender': fatalities_by_gender(conn),
        'by_time_of_day': fatalities_by_time_of_day(conn),
        'by_day_of_week': fatalities_by_day_of_week(conn),
        'by_speed_limit': fatalities_by_speed_limit(conn),
        'by_road_user': fatalities_by_road_user(conn),
        'by_remoteness': fatalities_by_remoteness(conn)
    }
    
    # Generate summary report
    generate_summary_report(results)
    
    # Close connection
    conn.close()
    print("Analysis complete!")

if __name__ == "__main__":
    analyze_data_warehouse() 