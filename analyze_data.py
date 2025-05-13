import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Set the style for the plots
plt.style.use('ggplot')
sns.set(font_scale=1.2)

# Load the dataset
def load_data(file_path):
    """Load the crash dataset from CSV file"""
    return pd.read_csv(file_path)

# Explore basic statistics and information
def explore_data(df):
    """Basic exploration of the dataset"""
    print(f"Dataset shape: {df.shape}")
    print("\nColumn names:")
    for col in df.columns:
        print(f"- {col}")
    
    print("\nData types:")
    print(df.dtypes)
    
    print("\nMissing values:")
    print(df.isnull().sum())
    
    print("\nBasic statistics:")
    print(df.describe())
    
    return df

# Create visualizations
def create_visualizations(df):
    """Create basic visualizations of the dataset"""
    # Create output directory
    if not os.path.exists('visualizations'):
        os.makedirs('visualizations')
    
    # 1. Crashes by State
    plt.figure(figsize=(12, 6))
    state_counts = df['State'].value_counts()
    sns.barplot(x=state_counts.index, y=state_counts.values)
    plt.title('Number of Crashes by State')
    plt.xlabel('State')
    plt.ylabel('Count')
    plt.savefig('visualizations/crashes_by_state.png')
    plt.close()
    
    # 2. Crashes by Year and Month
    plt.figure(figsize=(14, 7))
    df_grouped = df.groupby(['Year', 'Month']).size().reset_index(name='count')
    pivot_table = df_grouped.pivot(index='Month', columns='Year', values='count')
    sns.heatmap(pivot_table, annot=True, cmap='YlOrRd', fmt='d')
    plt.title('Crashes by Year and Month')
    plt.savefig('visualizations/crashes_by_year_month.png')
    plt.close()
    
    # 3. Distribution of Crashes by Time of Day
    plt.figure(figsize=(10, 6))
    time_counts = df['Time of day'].value_counts()
    sns.barplot(x=time_counts.index, y=time_counts.values)
    plt.title('Distribution of Crashes by Time of Day')
    plt.xlabel('Time of Day')
    plt.ylabel('Count')
    plt.savefig('visualizations/crashes_by_time.png')
    plt.close()
    
    # 4. Distribution of Age Groups
    plt.figure(figsize=(12, 6))
    age_counts = df['Age Group'].value_counts().sort_index()
    sns.barplot(x=age_counts.index, y=age_counts.values)
    plt.title('Distribution of Crashes by Age Group')
    plt.xlabel('Age Group')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('visualizations/crashes_by_age.png')
    plt.close()
    
    # 5. Road User Type Distribution
    plt.figure(figsize=(12, 6))
    user_counts = df['Road User'].value_counts()
    sns.barplot(x=user_counts.index, y=user_counts.values)
    plt.title('Distribution of Road User Types in Crashes')
    plt.xlabel('Road User Type')
    plt.ylabel('Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('visualizations/crashes_by_road_user.png')
    plt.close()
    
    # 6. Gender Distribution
    plt.figure(figsize=(8, 6))
    gender_counts = df['Gender'].value_counts()
    plt.pie(gender_counts, labels=gender_counts.index, autopct='%1.1f%%', startangle=90)
    plt.title('Gender Distribution in Crashes')
    plt.savefig('visualizations/crashes_by_gender.png')
    plt.close()

    # 7. Speed Limit Analysis
    plt.figure(figsize=(12, 6))
    speed_counts = df['Speed Limit'].value_counts().sort_index()
    sns.barplot(x=speed_counts.index, y=speed_counts.values)
    plt.title('Crashes by Speed Limit')
    plt.xlabel('Speed Limit')
    plt.ylabel('Count')
    plt.savefig('visualizations/crashes_by_speed_limit.png')
    plt.close()
    
    print("Visualizations created and saved to 'visualizations' folder")

# Analyze crash factors
def analyze_crash_factors(df):
    """Analyze factors associated with crashes"""
    # Create correlation matrix for numeric columns
    numeric_df = df.select_dtypes(include=['int64', 'float64'])
    if not numeric_df.empty:
        plt.figure(figsize=(10, 8))
        corr_matrix = numeric_df.corr()
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', linewidths=0.5)
        plt.title('Correlation Matrix of Numeric Features')
        plt.tight_layout()
        plt.savefig('visualizations/correlation_matrix.png')
        plt.close()
    
    # Analyze crash types
    crash_type_counts = df['Crash Type'].value_counts()
    print("\nCrash Type Distribution:")
    print(crash_type_counts)
    
    # Analyze remoteness areas
    remoteness_counts = df['National Remoteness Areas'].value_counts()
    print("\nRemoteness Areas Distribution:")
    print(remoteness_counts)
    
    # Analyze road types
    road_type_counts = df['National Road Type'].value_counts()
    print("\nRoad Type Distribution:")
    print(road_type_counts)
    
    # Truck involvement analysis
    truck_involvement = df['Articulated Truck Involvement'].value_counts()
    print("\nArticulated Truck Involvement:")
    print(truck_involvement)
    
    return None

def main():
    """Main function to run the analysis"""
    data_file = "Project2_Dataset_Corrected.csv"
    
    # Load and explore data
    df = load_data(data_file)
    explore_data(df)
    
    # Create visualizations
    create_visualizations(df)
    
    # Analyze crash factors
    analyze_crash_factors(df)
    
    print("\nAnalysis complete!")

if __name__ == "__main__":
    main() 