import pandas as pd
import re
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus
import os

# Database connection configuration
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'joy_of_painting')

# Create connection string
password = quote_plus(DB_PASSWORD)
connection_string = f"postgresql://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)
session = Session()

def extract_episode_dates():
    """Extract episode data from the dates CSV file."""
    with open('data/The Joy Of Painting - Episode Dates.csv', 'r') as f:
        lines = f.readlines()
    
    episodes = []
    
    for line in lines:

        match = re.match(r'"([^"]+)"\s+\(([^)]+)\)', line.strip())
        if match:
            title = match.group(1)
            date_str = match.group(2)
            
            # Parse date
            try:
                air_date = datetime.strptime(date_str, "%B %d, %Y")
            except ValueError:
                air_date = None
                
            episodes.append({
                'title': title.strip(),
                'air_date': air_date
            })
    
    return pd.DataFrame(episodes)

def extract_color_data():
    """Extract color data from the colors CSV file."""
    df = pd.read_csv('data/The Joy Of Painiting - Colors Used.csv', on_bad_lines='skip')
    
    # Clean column names
    df.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('\r', '').replace('\n', '') for col in df.columns]
    df.columns = [col.replace('(', '').replace(')', '').replace('/', '_') for col in df.columns]
    
    return df

def extract_subject_data():
    """Extract subject matter data from the subject CSV file."""
    df = pd.read_csv('data/The Joy Of Painiting - Subject Matter.csv', on_bad_lines='skip')
    
    # Clean column names
    df.columns = [col.strip().replace(' ', '_').replace('-', '_') for col in df.columns]
    
    return df

def transform_data(episodes_df, colors_df, subjects_df):
    """Transform data to match database schema."""
    # Add air_date to colors_df
    for idx, row in colors_df.iterrows():
        title = row['painting_title']
        match = episodes_df[episodes_df['title'] == title.strip()]
        if len(match) > 0:
            colors_df.at[idx, 'air_date'] = match.iloc[0]['air_date']
    
    # Extract season and episode numbers from EPISODE column in subjects_df
    subjects_df['season_number'] = subjects_df['EPISODE'].str.extract(r'S(\d+)E\d+').astype(int)
    subjects_df['episode_number'] = subjects_df['EPISODE'].str.extract(r'S\d+E(\d+)').astype(int)
    
    return colors_df, subjects_df

def clean_column_name(name):
    """Clean column names to match database field names."""
    return name.replace(' ', '_').replace('-', '_').replace('\r', '').replace('\n', '').lower()

def load_data_to_db(episodes_df, colors_df, subjects_df):
    """Load transformed data into the database."""
    
    # First, populate the Color table
    color_columns = [col for col in colors_df.columns 
                     if col not in ['painting_index', 'img_src', 'painting_title', 'season', 'episode', 'num_colors', 'youtube_src', 'colors', 'color_hex', 'air_date']]
    
    color_names = []
    color_hexes = []
    
    # Extract unique colors from the colors list column
    for idx, row in colors_df.iterrows():
        if 'colors' in colors_df.columns and pd.notna(row['colors']):
            try:
                # Clean up the colors string
                colors_str = row['colors'].replace('\r', '').replace('\n', '')
                color_list = eval(colors_str)
                hex_list = eval(row['color_hex'])
                for color, hex_code in zip(color_list, hex_list):
                    clean_color = color.strip("[]'\" ").strip()
                    if clean_color and clean_color not in color_names:
                        color_names.append(clean_color)
                        color_hexes.append(hex_code.strip("[]'\" ").strip())
            except:
                continue
    
    # Insert colors
    color_ids = {}
    for i, (color_name, hex_code) in enumerate(zip(color_names, color_hexes)):
        clean_name = color_name.strip()
        clean_hex = hex_code.strip()
        
        # Insert or get color ID
        query = text("""
            INSERT INTO Color (name, hex_code) 
            VALUES (:name, :hex_code) 
            ON CONFLICT (name) DO UPDATE SET hex_code = :hex_code 
            RETURNING id
        """)
        try:
            result = session.execute(query, {"name": clean_name, "hex_code": clean_hex})
            color_id = result.fetchone()[0]
            color_ids[clean_name] = color_id
        except Exception as e:
            print(f"Error inserting color {clean_name}: {e}")
    
    session.commit()
    
    # Insert episodes
    episode_ids = {}
    for idx, row in colors_df.iterrows():
        title = row['painting_title'].strip()
        season_num = int(row['season'])
        episode_num = int(row['episode'])
        air_date = row['air_date'] if 'air_date' in row else None
        youtube_url = row['youtube_src'] if 'youtube_src' in row else None
        image_url = row['img_src'] if 'img_src' in row else None
        
        # Insert episode
        query = text("""
            INSERT INTO Episode (title, season_number, episode_number, air_date, youtube_url, image_url)
            VALUES (:title, :season_number, :episode_number, :air_date, :youtube_url, :image_url)
            ON CONFLICT (season_number, episode_number) DO UPDATE SET
                title = :title,
                air_date = COALESCE(:air_date, air_date),
                youtube_url = COALESCE(:youtube_url, youtube_url),
                image_url = COALESCE(:image_url, image_url)
            RETURNING id
        """)
        try:
            result = session.execute(query, {
                "title": title,
                "season_number": season_num,
                "episode_number": episode_num,
                "air_date": air_date,
                "youtube_url": youtube_url,
                "image_url": image_url
            })
            episode_id = result.fetchone()[0]
            episode_ids[(season_num, episode_num)] = episode_id
        except Exception as e:
            print(f"Error inserting episode {title} (S{season_num}E{episode_num}): {e}")
    
    session.commit()
    
    # Insert episode colors
    for idx, row in colors_df.iterrows():
        season_num = int(row['season'])
        episode_num = int(row['episode'])
        
        if (season_num, episode_num) not in episode_ids:
            continue
            
        episode_id = episode_ids[(season_num, episode_num)]
        
        # Get colors used in this episode
        for color_name, is_used in row.items():
            if color_name in color_columns and isinstance(is_used, int) and is_used == 1:
                # Find the matching color name
                clean_name = color_name.replace('_', ' ')
                matching_colors = [name for name in color_ids.keys() if clean_name.lower() in name.lower()]
                if matching_colors:
                    color_id = color_ids[matching_colors[0]]
                    # Insert into EpisodeColor
                    query = text("""
                        INSERT INTO EpisodeColor (episode_id, color_id, is_used)
                        VALUES (:episode_id, :color_id, true)
                        ON CONFLICT (episode_id, color_id) DO NOTHING
                    """)
                    session.execute(query, {"episode_id": episode_id, "color_id": color_id})
    
    session.commit()
    
    # Insert subject matter data
    subject_columns = [col for col in subjects_df.columns if col not in ['EPISODE', 'TITLE', 'season_number', 'episode_number']]
    
    # First, populate SubjectMatter table
    subject_ids = {}
    for subject_name in subject_columns:
        clean_name = subject_name.replace('_', ' ').title()
        query = text("""
            INSERT INTO SubjectMatter (name)
            VALUES (:name)
            ON CONFLICT (name) DO NOTHING
            RETURNING id
        """)
        try:
            result = session.execute(query, {"name": clean_name})
            if result.rowcount > 0:
                subject_id = result.fetchone()[0]
                subject_ids[clean_name] = subject_id
            else:
                # Get the ID if it already existed
                query = text("SELECT id FROM SubjectMatter WHERE name = :name")
                result = session.execute(query, {"name": clean_name})
                if result.rowcount > 0:
                    subject_id = result.fetchone()[0]
                    subject_ids[clean_name] = subject_id
        except Exception as e:
            print(f"Error inserting subject {clean_name}: {e}")
    
    session.commit()
    
    # Insert episode subjects
    for idx, row in subjects_df.iterrows():
        season_num = row['season_number']
        episode_num = row['episode_number']
        
        # Find episode ID
        if (season_num, episode_num) in episode_ids:
            episode_id = episode_ids[(season_num, episode_num)]
        else:
            continue
            
        # Insert subjects for this episode
        for subject_name in subject_columns:
            if subject_name in row and row[subject_name] == 1:
                clean_name = subject_name.replace('_', ' ').title()
                if clean_name in subject_ids:
                    subject_id = subject_ids[clean_name]
                    query = text("""
                        INSERT INTO EpisodeSubject (episode_id, subject_id, is_featured)
                        VALUES (:episode_id, :subject_id, true)
                        ON CONFLICT (episode_id, subject_id) DO NOTHING
                    """)
                    session.execute(query, {"episode_id": episode_id, "subject_id": subject_id})
    
    session.commit()
    session.close()

def main():
    print("Starting ETL process...")
    
    # Extract data
    print("Extracting data from CSV files...")
    episodes_df = extract_episode_dates()
    colors_df = extract_color_data()
    subjects_df = extract_subject_data()
    
    # Transform data
    print("Transforming data...")
    colors_df, subjects_df = transform_data(episodes_df, colors_df, subjects_df)
    
    # Load data to database
    print("Loading data to database...")
    load_data_to_db(episodes_df, colors_df, subjects_df)
    
    print("ETL process completed successfully!")

if __name__ == "__main__":
    main()
