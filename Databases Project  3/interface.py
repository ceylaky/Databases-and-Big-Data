import pymysql
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox

# Database connection configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Dzy20021030',
    'database': 'spotify_project',
    'port': 3306
}

# Queries for the Spotify database
queries = {
    '-- Top Artist by Stream Count': '''
        SELECT 
            a.artist_name,
            SUM(sm.streams) AS total_streams
        FROM 
            Artist a
        JOIN 
            Artist_Track at ON a.artist_id = at.artist_id
        JOIN 
            Track t ON at.track_id = t.track_id
        JOIN 
            StreamingMetrics sm ON t.track_id = sm.track_id
        GROUP BY 
            a.artist_name
        ORDER BY 
            total_streams DESC
        LIMIT 10;
    ''',
    '-- Top Songs by Stream Count': '''
        SELECT t.track_name, sm.streams
        FROM Track t
        JOIN StreamingMetrics sm ON t.track_id = sm.track_id
        ORDER BY sm.streams DESC
        LIMIT 10;
    ''',
    '-- Most Collaborated Artists': '''
        SELECT 
            a.artist_name AS artist_name,
            COUNT(DISTINCT at.track_ID) AS total_tracks,
            COUNT(DISTINCT other_at.artist_ID) AS artists_collaborated
        FROM 
            Artist a
        JOIN 
            Artist_Track at ON a.artist_ID = at.artist_ID
        JOIN 
            Artist_Track other_at ON at.track_ID = other_at.track_ID AND a.artist_ID != other_at.artist_ID
        GROUP BY 
            a.artist_name
        ORDER BY 
            artists_collaborated DESC
        LIMIT 10;
    ''',
    '-- Artists Appearing Most in Playlists': '''
        SELECT 
            a.artist_name AS artist_name,
            (
                COALESCE(SUM(sm.in_spotify_playlists), 0) + 
                COALESCE(SUM(sm.in_deezer_playlists), 0) + 
                COALESCE(SUM(sm.in_apple_playlists), 0)
            ) AS playlists_count
        FROM 
            Artist a
        JOIN 
            Artist_Track at ON a.artist_id = at.artist_id
        JOIN 
            Track t ON at.track_id = t.track_id
        JOIN 
            StreamingMetrics sm ON t.track_id = sm.track_id
        GROUP BY 
            a.artist_name
        ORDER BY 
            playlists_count DESC
        LIMIT 10;
    ''',
    '-- Minimum Speechiness': '''
        SELECT t.track_name, ma.speechiness
        FROM Track t
        JOIN MusicalAttributes ma ON t.track_id = ma.track_id
        WHERE ma.speechiness = (SELECT MIN(speechiness) FROM MusicalAttributes);
    ''',
    '-- Artists Appearing Most in Charts': '''
        SELECT 
            a.artist_name,
            (
                COALESCE(SUM(sm.in_spotify_charts), 0) + 
                COALESCE(SUM(sm.in_apple_charts), 0) + 
                COALESCE(SUM(sm.in_deezer_charts), 0) + 
                COALESCE(SUM(sm.in_shazam_charts), 0)
            ) AS charts_count
        FROM 
            Artist a
        JOIN 
            Artist_Track at ON a.artist_id = at.artist_id
        JOIN 
            Track t ON at.track_id = t.track_id
        JOIN 
            StreamingMetrics sm ON t.track_id = sm.track_id
        GROUP BY 
            a.artist_name
        ORDER BY 
            charts_count DESC
        LIMIT 10;
    ''',
    '-- Popularity Score of Artists (Normalized)': '''
        WITH ArtistPopularityScores AS (
        SELECT 
            a.artist_name,
            (
            COALESCE(SUM(sm.in_spotify_playlists), 0) + 
            COALESCE(SUM(sm.in_spotify_charts), 0) + 
            COALESCE(SUM(sm.in_apple_playlists), 0) + 
            COALESCE(SUM(sm.in_apple_charts), 0) + 
            COALESCE(SUM(sm.in_deezer_playlists), 0) + 
            COALESCE(SUM(sm.in_deezer_charts), 0) + 
            COALESCE(SUM(sm.in_shazam_charts), 0)
            ) AS raw_popularity_score
        FROM 
            Artist a
        JOIN 
            Artist_Track at ON a.artist_ID = at.artist_ID
        JOIN 
            Track t ON at.track_ID = t.track_ID
        JOIN 
            StreamingMetrics sm ON t.track_ID = sm.track_ID
        GROUP BY 
            a.artist_name
        ),
        TotalArtistPopularity AS (
        SELECT 
        SUM(raw_popularity_score) AS total_popularity
        FROM ArtistPopularityScores
        )
        SELECT 
           aps.artist_name,
           aps.raw_popularity_score,
           aps.raw_popularity_score / tap.total_popularity AS normalized_popularity_score
        FROM ArtistPopularityScores aps, TotalArtistPopularity tap
        ORDER BY normalized_popularity_score DESC
        LIMIT 10;
    ''',
    '-- Average Danceability and Energy': '''
        SELECT 
            AVG(danceability) AS avg_danceability,
            AVG(energy) AS avg_energy
        FROM MusicalAttributes;
    ''',
    '-- Most Streamed Track per Year': '''
        SELECT 
            YEAR(t.release_date) AS release_year, 
            t.track_name, 
            GROUP_CONCAT(DISTINCT a.artist_name ORDER BY a.artist_name ASC SEPARATOR ', ') AS artists,
            s.streams
        FROM 
            Track t
        JOIN 
            StreamingMetrics s ON t.track_ID = s.track_ID
        JOIN 
            Artist_Track at ON t.track_ID = at.track_ID
        JOIN 
            Artist a ON at.artist_ID = a.artist_ID
        WHERE 
            s.streams = (
                SELECT 
                    MAX(s2.streams)
                FROM 
                    StreamingMetrics s2
                JOIN 
                    Track t2 ON s2.track_ID = t2.track_ID
                WHERE 
                    YEAR(t2.release_date) = YEAR(t.release_date)
            )
        GROUP BY 
            YEAR(t.release_date), t.track_name, s.streams
        ORDER BY 
            release_year;
    ''',
    '-- Top Acoustic Tracks by Acousticness and Streams': '''
        SELECT 
            t.track_name, 
            SUM(sm.streams) AS total_streams,
            ma.acousticness,
            GROUP_CONCAT(DISTINCT a.artist_name ORDER BY a.artist_name ASC SEPARATOR ', ') AS artists
        FROM 
            Track t
        JOIN 
            StreamingMetrics sm ON t.track_ID = sm.track_ID
        JOIN 
            MusicalAttributes ma ON t.track_ID = ma.track_ID
        JOIN 
            Artist_Track at ON t.track_ID = at.track_ID
        JOIN 
            Artist a ON at.artist_ID = a.artist_ID
        WHERE 
            ma.acousticness > 0 -- Filtering for tracks with some level of acousticness
        GROUP BY 
            t.track_ID
        ORDER BY 
            acousticness DESC, total_streams DESC
        LIMIT 10;
    '''
}

# Execute a query and return the result as a DataFrame
def execute_query(query):
    """
    Executes the given SQL query and retrieves the results in a pandas DataFrame.

    Args:
        query (str): The SQL query to execute.

    Returns:
        DataFrame: A pandas DataFrame containing the query results.
    """
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        
        # Execute the query and fetch all the rows
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Extract column names for the DataFrame
        columns = [desc[0] for desc in cursor.description]
        
        # Return the results as a pandas DataFrame
        return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        # Show an error message if the query fails
        messagebox.showerror("Error", f"Failed to execute query: {e}")
        return None
    finally:
        if conn:
            conn.close()

# Run the selected query and display the results in the UI
def run_query():
    """
    Handles the execution of the query selected by the user 
    and displays the results in the Treeview widget.
    """
    # Get the selected query from the dropdown
    selected_query = query_combobox.get()
    if not selected_query:
        messagebox.showwarning("Warning", "Please select a query first.")
        return
    
    # Fetch the query from the dictionary and execute it
    query = queries[selected_query]
    result = execute_query(query)
    
    if result is not None:
        # Clear any previous results in the Treeview
        for item in tree.get_children():
            tree.delete(item)
        
        # Set up the Treeview columns to match the query results
        tree["columns"] = list(result.columns)
        tree["show"] = "headings"
        for col in result.columns:
            tree.heading(col, text=col)  # Set column header text
            tree.column(col, width=150, anchor="center")  # Adjust column width
        
        # Insert the rows into the Treeview
        for _, row in result.iterrows():
            tree.insert("", "end", values=list(row))

# Set up the UI layout
def setup_ui(root):
    """
    Sets up the graphical interface for the application, including labels, buttons, and the Treeview.

    Args:
        root (Tk): The root Tkinter window.
    """
    # Add a title label at the top of the window
    tk.Label(root, text="Spotify Query Interface", font=("Helvetica", 16)).pack(pady=10)

    # Add a label and dropdown menu for selecting queries
    tk.Label(root, text="Select a Query:").pack(pady=5)
    global query_combobox
    query_combobox = ttk.Combobox(root, values=list(queries.keys()), state="readonly", width=60)
    query_combobox.pack(pady=5)

    # Add a button to run the selected query
    tk.Button(root, text="Run Query", command=run_query).pack(pady=10)

    # Add a frame to hold the Treeview widget
    tree_frame = tk.Frame(root)
    tree_frame.pack(fill="both", expand=True, padx=10, pady=10)

    # Add the Treeview to display query results
    global tree
    tree = ttk.Treeview(tree_frame)
    tree.pack(fill="both", expand=True)

# Main application logic
if __name__ == "__main__":
    """
    Entry point for the application. Sets up the main window and initializes the interface.
    """
    app = tk.Tk()
    app.title("Spotify Database Query Interface")
    app.geometry("900x700")
    setup_ui(app)
    app.mainloop()
