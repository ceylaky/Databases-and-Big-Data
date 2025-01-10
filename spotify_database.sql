-- Drop the database if it already exists
DROP DATABASE IF EXISTS spotify_project;

-- Create the database
CREATE DATABASE spotify_project;

-- Use the database
USE spotify_project;

-- Create the Artist Table
CREATE TABLE Artist (
    artist_ID INT AUTO_INCREMENT PRIMARY KEY, -- Primary key, auto-incremented
    artist_name VARCHAR(255) NOT NULL UNIQUE  -- Artist name, must be unique
);

-- Create the Track Table
CREATE TABLE Track (
    track_ID INT AUTO_INCREMENT PRIMARY KEY,  -- Primary key, auto-incremented
    track_name VARCHAR(255) NOT NULL,         -- Track name
    artist_count INT,                         -- Number of artists for the track
    release_date DATE                         -- Release date of the track
);

-- Create the Artist_Track Table (Intermediate table for many-to-many relationship)
CREATE TABLE Artist_Track (
    artist_ID INT NOT NULL,                   -- Foreign key referencing Artist
    track_ID INT NOT NULL,                    -- Foreign key referencing Track
    PRIMARY KEY (artist_ID, track_ID),        -- Composite primary key for uniqueness
    FOREIGN KEY (artist_ID) REFERENCES Artist(artist_ID) ON DELETE CASCADE,
    FOREIGN KEY (track_ID) REFERENCES Track(track_ID) ON DELETE CASCADE
);

-- Create the StreamingMetrics Table
CREATE TABLE StreamingMetrics (
    track_ID INT NOT NULL,                    -- Foreign key referencing Track
    streams BIGINT,                           -- Total streams
    in_spotify_playlists BIGINT,              -- Number of Spotify playlists
    in_spotify_charts BIGINT,                 -- Spotify chart appearances
    in_deezer_playlists BIGINT,               -- Number of Deezer playlists
    in_deezer_charts BIGINT,                  -- Deezer chart appearances
    in_shazam_charts BIGINT,                  -- Shazam chart appearances
    in_apple_charts BIGINT,                   -- Apple Music chart appearances
    in_apple_playlists BIGINT,                -- Number of Apple Music playlists
    PRIMARY KEY (track_ID),                   -- Primary key inherited from Track
    FOREIGN KEY (track_ID) REFERENCES Track(track_ID) ON DELETE CASCADE
);

-- Create the MusicalAttributes Table
CREATE TABLE MusicalAttributes (
    track_ID INT NOT NULL,                   
    bpm BIGINT,                               
    `key` VARCHAR(50),                        
    mode VARCHAR(50),                        
    danceability BIGINT,                      
    energy BIGINT,                            
    valence BIGINT,                           
    liveness BIGINT,                          
    acousticness BIGINT,                     
    instrumentalness BIGINT,                  
    speechiness BIGINT,                       
    PRIMARY KEY (track_ID),                   
    FOREIGN KEY (track_ID) REFERENCES Track(track_ID) ON DELETE CASCADE
);
