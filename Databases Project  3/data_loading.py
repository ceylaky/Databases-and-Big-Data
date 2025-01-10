import pandas as pd
import mysql.connector as mysql

def clean_numeric(value):
    # cleaning the numeric columns by removing commas and converting them to integers
    if isinstance(value, str):
        value = value.replace(',', '')  #removing commas
    try:
        return int(value)
    except ValueError:
        return 0

def clean_data(file_path):
    # cleaning the data from the CSV file
    data = pd.read_csv(file_path, index_col=False, delimiter=",", encoding="UTF-8")

    # removing rows with critical missing values(track_name and artist_name)
    data.dropna(subset=["track_name", "artist(s)_name"], inplace=True)

    # list of numeric columns to clean
    numeric_columns = [
        "streams", "in_spotify_playlists", "in_spotify_charts",
        "in_deezer_playlists", "in_deezer_charts", "in_shazam_charts",
        "in_apple_charts", "in_apple_playlists", "bpm", "artist_count",
        "danceability_%", "energy_%", "valence_%", "liveness_%", "acousticness_%",
        "instrumentalness_%", "speechiness_%"
    ]

    # only filters the existing columns in the dataset, helping to avoid errors if some columns are missing
    numeric_columns = [col for col in numeric_columns if col in data.columns]
    # applying the clean_numeric funtion to each numeric column 
    for col in numeric_columns:
        data[col] = data[col].apply(clean_numeric)

    data.fillna("", inplace=True)  # handling missing values
    return data

# function to load the data from CVS file into the created database in MySQL
def dataloading(user: str, password: str, cleaned_data: pd.DataFrame):
    # load the cleaned data into the MySQL database
    db = mysql.connect(host="localhost", user=user, passwd=password, database="spotify_project") #connectin to MySQL database
    curs = db.cursor() # creating a cursor object to interact with the database (allows for executing SQL queries)
    
    # cache for artist and track IDs for accessing the data fast and avoiding the queryig the database multiple times 
    artists_cache = {}  #initializing an empty dictionary to cache artist IDs (with artist name)
    tracks_cache = {}  #initializing an empty dictionary to cache track IDs (with track name)
    errors = []  #collecting errors that occurred during the insert process

    # insert artists into the Artist table
    for _, row in cleaned_data.iterrows():
        artist_names = row["artist(s)_name"].split(", ") #splitting artist names by commas (if there are multiple artists)
        for artist_name in artist_names:
            artist_name = artist_name.strip() #cleaning up any leading/trailing spaces

            # only insert artist name into the Artist table if it's not already cached
            if artist_name not in artists_cache:
                try:
                    # inserting artist name into the Artist table, avoiding duplicates
                    curs.execute("""
                        INSERT INTO Artist (artist_name)
                        VALUES (%s)
                        ON DUPLICATE KEY UPDATE artist_name=VALUES(artist_name)
                    """, (artist_name,))
                    db.commit() #commits the transaction to the database

                    #retrieving the artist_ID for the newly inserted artist 
                    curs.execute("SELECT artist_ID FROM Artist WHERE artist_name = %s", (artist_name,))
                    artist_id = curs.fetchone()
                    if artist_id:
                        artists_cache[artist_name] = artist_id[0] #cache the artist name
                        curs.fetchall()  #clearing any remaining results
                except mysql.Error as err:
                    errors.append(f"Error inserting artist '{artist_name}': {err}") #catches any errors during the insert process and adds them to the errors list

    # insert tracks and Artist_Track relationships

    for _, row in cleaned_data.iterrows(): #_ index of the current row
        track_name = row["track_name"]
        artist_names = row["artist(s)_name"].split(", ")
        artist_count = row["artist_count"]
        release_date = None #initialize the variable to None
        if row.get('released_year') and row.get('released_month') and row.get('released_day'): #checks if all the components of the release date (year, month and day) are present/non-empty
            release_date = f"{int(row['released_year'])}-{int(row['released_month']):02d}-{int(row['released_day']):02d}" #if true, formats the release date as a string in the format YYYY-MM-DD ("02d" formats as two digit string)

        # insert track into Track table
        if track_name not in tracks_cache:
            try:
                #if track_name is not in track_cache dictionary, inserts it to Track table
                curs.execute("""
                    INSERT INTO Track (track_name, artist_count, release_date)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE artist_count=VALUES(artist_count) 
                """, (track_name, artist_count, release_date)) #"DUPLICATE KEY UPDATE" ensures the existing record is updated rather than re-inserted 
                db.commit()
                # Retrieve track_ID
                curs.execute("SELECT track_ID FROM Track WHERE track_name = %s", (track_name,)) #retrives the track_id of the track that has been just inserted/updated
                track_id = curs.fetchone() #fetches the result of the previous query
                if track_id: #ensures if valid track_id was retrived from the query
                    tracks_cache[track_name] = track_id[0] #adds the retrived track_id to the track_cache dictionary with track_name as the key
                    curs.fetchall()  #clears any remaining results; even if "fetchall" retrieves only one result, we used it to be avoid lingering data in the cursor 
            except mysql.Error as err:
                errors.append(f"Error inserting track '{track_name}': {err}")

        # insert into Artist_Track
        track_id = tracks_cache.get(track_name) #gets the track_id 
        if track_id: #ensures that track has been successfully inserted into the track table
            for artist_name in artist_names: #iterates through artist names associated with current track
                artist_id = artists_cache.get(artist_name) #retrives the artist_id from the artist_cache dictionary 
                if artist_id: #ensures that artist has been successfully inserted into the Artist table  
                    try:
                        #inserts a track into Artist_Track table linking the artist_id with track_id 
                        curs.execute("""
                            INSERT INTO Artist_Track (artist_ID, track_ID)
                            VALUES (%s, %s)
                            ON DUPLICATE KEY UPDATE artist_ID=VALUES(artist_ID), track_ID=VALUES(track_ID)
                        """, (artist_id, track_id))
                        db.commit()
                        curs.fetchall()  #clearing any remaining results
                    except mysql.Error as err:
                        errors.append(f"Error inserting Artist_Track for '{artist_name}' and '{track_name}': {err}")

    #insert streaming metrics into StreamingMetrics table
    for _, row in cleaned_data.iterrows():
        track_name = row["track_name"]
        track_id = tracks_cache.get(track_name)

        if track_id: #ensures that track exists in the database 
            streaming_data = (
                track_id, #foreign key
                #row.get is used to safely retrieve values from the row; if column is missing, defaults to 0
                row.get("streams", 0), 
                row.get("in_spotify_playlists", 0),
                row.get("in_spotify_charts", 0),
                row.get("in_deezer_playlists", 0),
                row.get("in_deezer_charts", 0),
                row.get("in_shazam_charts", 0),
                row.get("in_apple_charts", 0),
                row.get("in_apple_playlists", 0),
            )
            try:
                curs.execute("""
                    INSERT INTO StreamingMetrics (
                        track_ID, streams, in_spotify_playlists, in_spotify_charts,
                        in_deezer_playlists, in_deezer_charts, in_shazam_charts,
                        in_apple_charts, in_apple_playlists
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE streams=VALUES(streams)
                """, streaming_data) #if a record with the same track_id already exists, it updates the "streams" column with the new value from streaming data; other columns can be added to the UPDATE clause if needed
                db.commit()
                curs.fetchall()  #clearing any remaining results
            except mysql.Error as err:
                errors.append(f"Error inserting streaming metrics for track '{track_name}': {err}")

    # Insert musical attributes into MusicalAttributes table
    for _, row in cleaned_data.iterrows():
        track_name = row["track_name"]
        track_id = tracks_cache.get(track_name)

        if track_id: #ensure that track exists in the database 
            musical_data = (
                track_id, #foreign key
                row.get("bpm", 0),
                row.get("key", ""),
                row.get("mode", ""),
                row.get("danceability_%", 0),
                row.get("energy_%", 0),
                row.get("valence_%", 0),
                row.get("liveness_%", 0),
                row.get("acousticness_%", 0),
                row.get("instrumentalness_%", 0),
                row.get("speechiness_%", 0),
            )
            try:
                curs.execute("""
                    INSERT INTO MusicalAttributes (
                        track_ID, bpm, `key`, mode, danceability,
                        energy, valence, liveness, acousticness,
                        instrumentalness, speechiness
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE bpm=VALUES(bpm) 
                """, musical_data) #if a record with the same track_id already exists, it updates the "bpm" column with the new value from musical data; other columns can be added to the UPDATE clause if needed
                db.commit()
                curs.fetchall()  #clearing any remaining results
            except mysql.Error as err:
                errors.append(f"Error inserting musical attributes for track '{track_name}': {err}")

    db.commit()
    curs.close()
    db.close()

    print("Data loaded successfully.")
    if errors:
        print("Some errors occurred:")
        for error in errors:
            print(error)

# Example usage
file_path = "/Users/mac/Desktop/DATABASES and BIG DATA/project/Spotify Most Streamed Songs 2.csv"
cleaned_data = clean_data(file_path)
dataloading(user="root", password="Mechta101_", cleaned_data=cleaned_data)