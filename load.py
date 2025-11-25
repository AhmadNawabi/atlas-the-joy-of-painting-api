import pandas as pd
from database.connection import get_connection

# CSV file paths
EPISODE_CSV = "data/bob_ross_paintings.csv"
COLOR_CSV = "data/elements-by-episode.csv"

def create_tables(conn):
    """
    Create tables if they don't exist
    """
    with conn.cursor() as cur:
        # Episodes table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS episodes (
            id SERIAL PRIMARY KEY,
            episode_code VARCHAR(10),
            title VARCHAR(255),
            season INT,
            episode_number INT,
            month INT,
            year INT
        );
        """)

        # Colors table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS colors (
            id SERIAL PRIMARY KEY,
            episode_id INT REFERENCES episodes(id),
            color_name VARCHAR(100),
            used BOOLEAN
        );
        """)

        conn.commit()
        print("Tables created successfully.")

def load_episodes(conn, episodes_df):
    """
    Insert episode data into the episodes table
    """
    with conn.cursor() as cur:
        for _, row in episodes_df.iterrows():
            episode_code = row.get('painting_index', None)
            title = row.get('painting_title', '').strip('"')

            # Safe conversion to season/episode_number
            try:
                idx = str(episode_code)
                season = int(idx[0]) if idx else None
                episode_number = int(idx[1:]) if len(idx) > 1 else None
            except (ValueError, TypeError):
                season = None
                episode_number = None

            month = row.get('month', None)
            year = row.get('year', None)

            cur.execute("""
            INSERT INTO episodes (episode_code, title, season, episode_number, month, year)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
            """, (episode_code, title, season, episode_number, month, year))
            
            episode_id = cur.fetchone()['id']

            # Load colors for this episode
            color_columns = [col for col in episodes_df.columns if col not in ['painting_index', 'painting_title', 'month', 'year', 'img_src']]
            for color in color_columns:
                used = bool(row[color])
                cur.execute("""
                INSERT INTO colors (episode_id, color_name, used)
                VALUES (%s, %s, %s)
                """, (episode_id, color, used))

        conn.commit()
        print("Episodes and colors loaded successfully.")

def main():
    # Load CSVs
    episodes_df = pd.read_csv(EPISODE_CSV)
    print("Episode data loaded successfully.")

    colors_df = pd.read_csv(COLOR_CSV)
    print("Color data loaded successfully.")

    # Connect to database
    conn = get_connection()
    if not conn:
        print("Failed to connect to database. Exiting.")
        return

    # Create tables and load data
    create_tables(conn)
    load_episodes(conn, episodes_df)

    # Close connection
    conn.close()
    print("Database connection closed.")

if __name__ == "__main__":
    main()
