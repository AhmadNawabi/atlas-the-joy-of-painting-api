-- Drop tables if they already exist
DROP TABLE IF EXISTS episode_colors;
DROP TABLE IF EXISTS episode_subjects;
DROP TABLE IF EXISTS episodes;
DROP TABLE IF EXISTS colors;
DROP TABLE IF EXISTS subjects;


-- -------------------------
-- Table: Episodes
-- -------------------------
CREATE TABLE episodes (
    id SERIAL PRIMARY KEY,
    episode_code VARCHAR(20) UNIQUE NOT NULL,
    title VARCHAR(255) NOT NULL,
    season INTEGER,
    month VARCHAR(20),
    broadcast_date DATE
);

-- -------------------------
-- Table: Colors
-- -------------------------
CREATE TABLE colors (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    hex_code VARCHAR(7) NOT NULL
);

-- -------------------------
-- Junction table: episode_subjects
-- -------------------------
CREATE TABLE episode_subjects (
    episode_id INTEGER REFERENCES episodes(id) ON DELETE CASCADE,
    subject_id INTEGER REFERENCES subjects(id) ON DELETE CASCADE,
    PRIMARY KEY (episode_id, subject_id)
);

-- -------------------------
-- Junction table: episode_colors
-- -------------------------
CREATE TABLE episode_colors (
    episode_id INTEGER REFERENCES episodes(id) ON DELETE CASCADE,
    color_id INTEGER REFERENCES colors(id) ON DELETE CASCADE,
    PRIMARY KEY (episode_id, color_id)
);

