import os
import csv
import re
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus

# -- File paths --
DATA_FOLDER = "../data"  # relative path from ETL folder
COLORS_FILE = os.path.join(DATA_FOLDER, "colors_used.csv")
EPISODES_FILE = os.path.join(DATA_FOLDER, "episodes_dates.csv")
SUBJECTS_FILE = os.path.join(DATA_FOLDER, "subject_matter.csv")

# -- Database connection --
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'password')
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME', 'joy_of_painting')

password = quote_plus(DB_PASSWORD)
connection_string = f"postgresql://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)

# -- Insert Colors --
def insert_colors():
    session = Session()
    inserted = 0

    with open(COLORS_FILE, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            name = row['colors'].strip().replace("[", "").replace("]", "").replace("'", "")
            hex_codes = row['color_hex'].strip().replace("[", "").replace("]", "").replace("'", "").split(",")
            color_names = [c.strip() for c in name.split(",")]
            color_hexes = [h.strip() for h in hex_codes]

            for cname, chex in zip(color_names, color_hexes):
                insert_stmt = text("""
                    INSERT INTO Color (name, hex_code)
                    VALUES (:name, :hex_code)
                    ON CONFLICT (name) DO NOTHING
                """)
                session.execute(insert_stmt, {"name": cname, "hex_code": chex})
                inserted += 1

    session.commit()
    session.close()
    print(f"Inserted {inserted} colors.")

# -- Insert Episodes --
def insert_episodes():
    session = Session()
    inserted = 0
    episode_ids = []

    with open(EPISODES_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Extract title and date using regex
            title_match = re.match(r'"?(.*?)"?\s*\((.*?)\)', line)
            if not title_match:
                print(f"Skipping line (cannot parse title/date): {line}")
                continue

            title = title_match.group(1).strip()
            date_str = title_match.group(2).strip()

            try:
                air_date = datetime.strptime(date_str, "%B %d, %Y").date()
            except ValueError:
                print(f"Skipping line (invalid date format): {line}")
                continue

            # Assign season and episode numbers sequentially
            season_number = 1
            episode_number = inserted + 1

            insert_stmt = text("""
                INSERT INTO Episode (title, season_number, episode_number, air_date)
                VALUES (:title, :season_number, :episode_number, :air_date)
                ON CONFLICT (season_number, episode_number) DO NOTHING
                RETURNING id
            """)
            result = session.execute(insert_stmt, {
                "title": title,
                "season_number": season_number,
                "episode_number": episode_number,
                "air_date": air_date
            }).fetchone()

            if result:
                episode_ids.append(result[0])
            else:
                # fetch existing ID if already exists
                result = session.execute(text("""
                    SELECT id FROM Episode WHERE season_number = :season AND episode_number = :episode
                """), {"season": season_number, "episode": episode_number}).fetchone()
                if result:
                    episode_ids.append(result[0])

            inserted += 1

    session.commit()
    session.close()
    print(f"Inserted {inserted} episodes.")
    return episode_ids

# -- Insert Subjects --
def insert_subjects():
    session = Session()
    inserted = 0

    with open(SUBJECTS_FILE, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            for col_name, value in row.items():
                if col_name in ['EPISODE', 'TITLE']:
                    continue
                if value.strip() == "1":
                    subject_name = col_name.replace("_", " ").title()
                    # Insert subject if not exists
                    session.execute(text("""
                        INSERT INTO SubjectMatter (name)
                        VALUES (:name)
                        ON CONFLICT (name) DO NOTHING
                    """), {"name": subject_name})
                    inserted += 1

    session.commit()
    session.close()
    print(f"Inserted {inserted} subjects.")

# -- Link Episodes to Colors --
def link_episodes_colors():
    session = Session()
    inserted = 0

    with open(COLORS_FILE, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            season = int(row['season'])
            episode = int(row['episode'])
            # Fetch episode_id
            result = session.execute(text("""
                SELECT id FROM Episode
                WHERE season_number = :season AND episode_number = :episode
            """), {"season": season, "episode": episode}).fetchone()
            if not result:
                continue
            episode_id = result[0]

            color_names = row['colors'].strip().replace("[","").replace("]","").replace("'", "").split(",")
            color_names = [c.strip() for c in color_names]

            for cname in color_names:
                color_id = session.execute(text("SELECT id FROM Color WHERE name = :name"), {"name": cname}).fetchone()
                if color_id:
                    session.execute(text("""
                        INSERT INTO EpisodeColor (episode_id, color_id)
                        VALUES (:episode_id, :color_id)
                        ON CONFLICT (episode_id, color_id) DO NOTHING
                    """), {"episode_id": episode_id, "color_id": color_id[0]})
                    inserted += 1

    session.commit()
    session.close()
    print(f"Linked {inserted} episode-color relations.")

# -- Link Episodes to Subjects --
def link_episodes_subjects():
    session = Session()
    inserted = 0

    with open(SUBJECTS_FILE, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            ep_code = row['EPISODE']
            title = row['TITLE'].strip('"')
            # Fetch episode_id
            result = session.execute(text("""
                SELECT id FROM Episode
                WHERE title ILIKE :title
            """), {"title": title}).fetchone()
            if not result:
                continue
            episode_id = result[0]

            for col_name, value in row.items():
                if col_name in ['EPISODE', 'TITLE']:
                    continue
                if value.strip() == "1":
                    subject_name = col_name.replace("_", " ").title()
                    subject_id = session.execute(text("""
                        SELECT id FROM SubjectMatter WHERE name = :name
                    """), {"name": subject_name}).fetchone()
                    if subject_id:
                        session.execute(text("""
                            INSERT INTO EpisodeSubject (episode_id, subject_id)
                            VALUES (:episode_id, :subject_id)
                            ON CONFLICT (episode_id, subject_id) DO NOTHING
                        """), {"episode_id": episode_id, "subject_id": subject_id[0]})
                        inserted += 1

    session.commit()
    session.close()
    print(f"Linked {inserted} episode-subject relations.")

# ---------- Run all ----------
if __name__ == "__main__":
    insert_colors()
    insert_episodes()
    insert_subjects()
    link_episodes_colors()
    link_episodes_subjects()
    print("Database seeding completed successfully!")
