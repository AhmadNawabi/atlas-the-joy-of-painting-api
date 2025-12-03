from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus
import os

app = Flask(__name__)

# Database configuration
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'password')
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME', 'joy_of_painting')

# Create connection string
password = quote_plus(DB_PASSWORD)
connection_string = f"postgresql://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)

def get_episodes_by_filters(filters, filter_type="AND"):
    """
    Get episodes based on filters.
    filter_type can be "AND" (all filters must match) or "OR" (any filter can match)
    """
    session = Session()

    query = """
    SELECT e.id, e.title, e.season_number, e.episode_number, e.air_date, e.youtube_url, e.image_url,
           ARRAY_AGG(DISTINCT c.name) FILTER (WHERE c.name IS NOT NULL) as colors,
           ARRAY_AGG(DISTINCT s.name) FILTER (WHERE s.name IS NOT NULL) as subjects
    FROM Episode e
    LEFT JOIN EpisodeColor ec ON e.id = ec.episode_id
    LEFT JOIN Color c ON ec.color_id = c.id
    LEFT JOIN EpisodeSubject es ON e.id = es.episode_id
    LEFT JOIN SubjectMatter s ON es.subject_id = s.id
    """

    where_clauses = []
    params = {}

    # Filter by months
    if filters.get("months"):
        month_conditions = []
        for i, month in enumerate(filters["months"]):
            param_name = f"month_{i}"
            month_conditions.append(f"EXTRACT(MONTH FROM e.air_date) = :{param_name}")
            params[param_name] = int(month)
        where_clauses.append(f"({' OR '.join(month_conditions)})")

    # Filter by colors
    if filters.get("colors"):
        color_conditions = []
        for i, color in enumerate(filters["colors"]):
            param_name = f"color_{i}"
            color_conditions.append(f"c.name ILIKE :{param_name}")
            params[param_name] = f"%{color}%"

        if filter_type == "AND":
            # Ensure all colors are present using COUNT in HAVING
            having_clause = f"COUNT(DISTINCT CASE WHEN {' OR '.join(color_conditions)} THEN c.name END) = {len(filters['colors'])}"
        else:
            # OR: any color matches
            where_clauses.append(f"({' OR '.join(color_conditions)})")
            having_clause = None

    # Filter by subjects
    if filters.get("subjects"):
        subject_conditions = []
        for i, subject in enumerate(filters["subjects"]):
            param_name = f"subject_{i}"
            subject_conditions.append(f"s.name ILIKE :{param_name}")
            params[param_name] = f"%{subject}%"

        if filter_type == "AND":
            # Ensure all subjects are present using COUNT in HAVING
            subject_having = f"COUNT(DISTINCT CASE WHEN {' OR '.join(subject_conditions)} THEN s.name END) = {len(filters['subjects'])}"
            if 'having_clause' in locals() and having_clause:
                having_clause += f" AND {subject_having}"
            else:
                having_clause = subject_having
        else:
            # OR: any subject matches
            where_clauses.append(f"({' OR '.join(subject_conditions)})")

    # Build WHERE
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += " GROUP BY e.id"

    # Add HAVING if AND filters
    if filter_type == "AND" and 'having_clause' in locals() and having_clause:
        query += " HAVING " + having_clause

    query += " ORDER BY e.season_number, e.episode_number"

    result = session.execute(text(query), params)

    episodes = []
    for row in result:
        episodes.append({
            "id": row[0],
            "title": row[1],
            "season": row[2],
            "episode": row[3],
            "air_date": row[4].strftime("%Y-%m-%d") if row[4] else None,
            "youtube_url": row[5],
            "image_url": row[6],
            "colors": [c for c in row[7] if c] if row[7] else [],
            "subjects": [s for s in row[8] if s] if row[8] else []
        })

    session.close()
    return episodes

# --- ROUTES ---
@app.route('/')
def index():
    return "Joy of Painting API is running."

@app.route('/api/episodes', methods=['GET', 'POST'])
def get_episodes():
    if request.method == 'GET':
        months = request.args.getlist('month')
        colors = request.args.getlist('color')
        subjects = request.args.getlist('subject')
        filter_type = request.args.get('filter_type', 'AND')

        filters = {}
        if months:
            filters['months'] = months
        if colors:
            filters['colors'] = colors
        if subjects:
            filters['subjects'] = subjects

    elif request.method == 'POST':
        data = request.json
        filters = data.get('filters', {})
        filter_type = data.get('filter_type', 'AND')

    episodes = get_episodes_by_filters(filters, filter_type.upper())
    return jsonify(episodes)

@app.route('/api/colors', methods=['GET'])
def get_colors():
    session = Session()
    result = session.execute(text("SELECT id, name, hex_code FROM Color ORDER BY name"))
    colors = [{'id': row[0], 'name': row[1], 'hex_code': row[2]} for row in result]
    session.close()
    return jsonify(colors)

@app.route('/api/subjects', methods=['GET'])
def get_subjects():
    session = Session()
    result = session.execute(text("SELECT id, name FROM SubjectMatter ORDER BY name"))
    subjects = [{'id': row[0], 'name': row[1]} for row in result]
    session.close()
    return jsonify(subjects)

@app.route('/api/months', methods=['GET'])
def get_months():
    months = [
        {'id': 1, 'name': 'January'},
        {'id': 2, 'name': 'February'},
        {'id': 3, 'name': 'March'},
        {'id': 4, 'name': 'April'},
        {'id': 5, 'name': 'May'},
        {'id': 6, 'name': 'June'},
        {'id': 7, 'name': 'July'},
        {'id': 8, 'name': 'August'},
        {'id': 9, 'name': 'September'},
        {'id': 10, 'name': 'October'},
        {'id': 11, 'name': 'November'},
        {'id': 12, 'name': 'December'}
    ]
    return jsonify(months)

@app.route('/api/episodes/<int:season>/<int:episode>', methods=['GET'])
def get_episode_details(season, episode):
    session = Session()
    query = text("""
    SELECT e.id, e.title, e.season_number, e.episode_number, e.air_date, e.youtube_url, e.image_url,
           ARRAY_AGG(DISTINCT c.name) FILTER (WHERE c.name IS NOT NULL) as colors,
           ARRAY_AGG(DISTINCT s.name) FILTER (WHERE s.name IS NOT NULL) as subjects
    FROM Episode e
    LEFT JOIN EpisodeColor ec ON e.id = ec.episode_id
    LEFT JOIN Color c ON ec.color_id = c.id
    LEFT JOIN EpisodeSubject es ON e.id = es.episode_id
    LEFT JOIN SubjectMatter s ON es.subject_id = s.id
    WHERE e.season_number = :season AND e.episode_number = :episode
    GROUP BY e.id, e.title, e.season_number, e.episode_number, e.air_date, e.youtube_url, e.image_url
    """)
    result = session.execute(query, {"season": season, "episode": episode}).fetchone()
    if not result:
        session.close()
        return jsonify({"error": "Episode not found"}), 404
    episode_data = {
        'id': result[0],
        'title': result[1],
        'season': result[2],
        'episode': result[3],
        'air_date': result[4].strftime('%Y-%m-%d') if result[4] else None,
        'youtube_url': result[5],
        'image_url': result[6],
        'colors': [color.strip() for color in result[7] if color] if result[7] else [],
        'subjects': [subject.strip() for subject in result[8] if subject] if result[8] else []
    }
    session.close()
    return jsonify(episode_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
