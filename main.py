import os
import sys
import time

def print_header(message):
    """Print a formatted header"""
    print("\n" + "=" * 80)
    print(f" {message}")
    print("=" * 80 + "\n")

def check_dependencies():
    """Check if required Python packages are installed"""
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
        import seaborn as sns
        import sqlite3
        print("All required dependencies are installed.")
        return True
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("\nPlease install required packages using:")
        print("pip install pandas matplotlib seaborn")
        return False

def run_exploratory_analysis():
    """Run exploratory data analysis"""
    print_header("STEP 1: Running Exploratory Data Analysis")
    import analyze_data
    analyze_data.main()

def create_data_warehouse():
    """Create the data warehouse"""
    print_header("STEP 2: Creating Data Warehouse")
    import create_data_warehouse
    create_data_warehouse.create_data_warehouse()

def query_data_warehouse():
    """Query the data warehouse and generate reports"""
    print_header("STEP 3: Generating Reports from Data Warehouse")
    import query_data_warehouse
    query_data_warehouse.analyze_data_warehouse()

def main():
    """Main function to run the entire pipeline"""
    print_header("CRASH DATA WAREHOUSE PROJECT")
    
    # Check if required packages are installed
    if not check_dependencies():
        sys.exit(1)
    
    # Check if dataset exists
    if not os.path.exists("Project2_Dataset_Corrected.csv"):
        print("Error: Dataset file 'Project2_Dataset_Corrected.csv' not found.")
        sys.exit(1)
    
    start_time = time.time()
    
    # Run exploratory analysis
    run_exploratory_analysis()
    
    # Create data warehouse
    create_data_warehouse()
    
    # Query data warehouse and generate reports
    query_data_warehouse()
    
    # Print completion message
    elapsed_time = time.time() - start_time
    print_header(f"PIPELINE COMPLETED in {elapsed_time:.2f} seconds")
    print("All tasks completed successfully!")
    print("\nOutput files:")
    print("- Exploratory visualizations: ./visualizations/")
    print("- Data warehouse: ./crash_data_warehouse.db")
    print("- Analysis reports: ./reports/")
    print("\nTo view the summary report, open: ./reports/summary_report.txt")

if __name__ == "__main__":
    main() 