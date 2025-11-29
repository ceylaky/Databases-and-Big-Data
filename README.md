# Databases-and-Big-Data

# Spotify Top-Streamed Songs Database Project

**Authors:** Assima Amangeldina, Ceyla Kaya, Sabina Nurseitova, Ziyi Dong
This notebook summarizes the database project based on the presentation.

## Project Overview

A full pipeline project including:
- ER Diagram Design
- MySQL Schema Creation
- Python Data Cleaning & Loading
- Interactive Query Interface
- SQL Query Development

## Dataset Description

Dataset contains track information, streaming metrics (Spotify, Apple, Deezer, Shazam), and musical attributes such as BPM, danceability, energy, acousticness.

## Database Design

ER Diagram includes entities (Artist, Track, MusicalAttributes, StreamingMetrics) with many-to-many and one-to-one relationships.

## MySQL Database Creation

Tables were created with foreign keys and normalized structure.

## Data Loading (Python ETL)

Python ETL cleaned data, removed nulls, and loaded everything into MySQL using cached inserts.

## Query Interface (Python GUI)

Tkinter UI: dropdown for queries, run button, and Treeview to display SQL results.

## Implemented SQL Queries

Queries include Top Songs by Streams, Top Artists, Most Collaborated, Minimum Speechiness, Danceability & Energy, Popularity Score, Acoustic Tracks, etc.

## Project Summary

ER model, SQL schema, ETL load, and GUI query tool completed with analytical results.

## Technologies Used

MySQL, Python (Tkinter, Pandas, MySQL Connector), CSV dataset.

## How to Run

1. Run SQL schema.
2. Run `insert_data.py`.
3. Run `app.py` to open the query interface.
