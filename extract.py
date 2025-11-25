import pandas as pd
from database.connection import get_connection

EPISODE_CSV = "data/bob_ross_paintings.csv"
COLOR_CSV = "data/elements-by-episode.csv"


def extract_episodes():
    """
    Extracts episode data from a CSV file and returns it as a DataFrame.
    """
    try:
        df = pd.read_csv(EPISODE_CSV)
        print("Episode data loaded successfully.")
        print(df.head()) # Display first few rows for verification
        return df
    except Exception as e:
        print(f"Error reading episode CSV: {e}")
        return None
    
def extract_colors():
    """
    Reads the paint color CSV and returns a pandas DataFrame.
    """
    try:
        df = pd.read_csv(COLOR_CSV)
        print("Color data loaded successfully.")
        print(df.head()) # Display first few rows for verification
        return df
    except Exception as e:
        print(f"Error reading color CSV: {e}")
        return None
    
def main():
    #Connect to the database
    conn = get_connection()
    if not conn:
        print("Cannot connect to the database. Exiting...")
        return
    
    # Extract data
    episodes_df = extract_episodes()
    colors_df = extract_colors()


    conn.close()
    print("Database connection closed.")

if __name__ == "__main__":
    main()
