-- Create database
CREATE DATABASE joy_of_painting;
\c joy_of_painting

-- Create Episode table
CREATE TABLE Episode (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    season_number INTEGER NOT NULL,
    episode_number INTEGER NOT NULL,
    air_date DATE NOT NULL,
    youtube_url TEXT,
    image_url TEXT,
    UNIQUE(season_number, episode_number)
);

-- Create Color table
CREATE TABLE Color (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    hex_code VARCHAR(7) NOT NULL
);

-- Create SubjectMatter table
CREATE TABLE SubjectMatter (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- Create EpisodeColor junction table
CREATE TABLE EpisodeColor (
    episode_id INTEGER NOT NULL,
    color_id INTEGER NOT NULL,
    is_used BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (episode_id, color_id),
    FOREIGN KEY (episode_id) REFERENCES Episode(id) ON DELETE CASCADE,
    FOREIGN KEY (color_id) REFERENCES Color(id) ON DELETE CASCADE
);

-- Create EpisodeSubject junction table
CREATE TABLE EpisodeSubject (
    episode_id INTEGER NOT NULL,
    subject_id INTEGER NOT NULL,
    is_featured BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (episode_id, subject_id),
    FOREIGN KEY (episode_id) REFERENCES Episode(id) ON DELETE CASCADE,
    FOREIGN KEY (subject_id) REFERENCES SubjectMatter(id) ON DELETE CASCADE
);
