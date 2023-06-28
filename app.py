import pandas as pd
import sqlite3
from flask import Flask, jsonify, request

app = Flask(__name__)
# In-memory SQLite database
db = sqlite3.connect(':memory:', check_same_thread=False)

# Create and populate movies and ratings tables from movies.csv and ratings.csv


def create_tables():
    movies_df = pd.read_csv('movies.csv')
    ratings_df = pd.read_csv('ratings.csv')

    movies_df.to_sql('movies', db, if_exists='replace', index=False)
    ratings_df.to_sql('ratings', db, if_exists='replace', index=False)


create_tables()

# GET route to retrieve the top 10 movies with the longest runTime


@app.route('/api/v1/longest-duration-movies', methods=['GET'])
def get_longest_duration_movies():
    query = '''
        SELECT tconst, primaryTitle, runtimeMinutes, genres
        FROM movies
        ORDER BY runtimeMinutes DESC
        LIMIT 10
    '''
    result = pd.read_sql_query(query, db)
    return jsonify(result.to_dict(orient='records'))

# POST route to save a new movie to the database


@app.route('/api/v1/new-movie', methods=['POST'])
def save_new_movie():
    data = request.get_json()
    if not data or 'tconst' not in data or 'titleType' not in data or 'primaryTitle' not in data \
            or 'runtimeMinutes' not in data or 'genres' not in data:
        return jsonify({'error': 'Invalid request body'}), 400

    df = pd.DataFrame(data, index=[0])
    df.to_sql('movies', db, if_exists='append', index=False)
    return jsonify({'message': 'success'})

# GET route to retrieve top-rated movies with averageRating > 6.0


@app.route('/api/v1/top-rated-movies', methods=['GET'])
def get_top_rated_movies():
    query = '''
        SELECT m.tconst, m.primaryTitle, m.genres, r.averageRating
        FROM movies AS m
        JOIN ratings AS r ON m.tconst = r.tconst
        WHERE r.averageRating > 6.0
        ORDER BY r.averageRating
    '''
    result = pd.read_sql_query(query, db)
    return jsonify(result.to_dict(orient='records'))

# GET route to retrieve genre-wise movies with numVotes subtotals


@app.route('/api/v1/genre-movies-with-subtotals', methods=['GET'])
def get_genre_movies_with_subtotals():
    query = '''
        SELECT m.genres, m.primaryTitle, SUM(r.numVotes) AS numVotes
        FROM movies AS m
        JOIN ratings AS r ON m.tconst = r.tconst
        GROUP BY m.genres, m.primaryTitle
        ORDER BY m.genres
    '''
    result = pd.read_sql_query(query, db)
    return jsonify(result.to_dict(orient='records'))


# Update runtimeMinutes using SQL queries
update_runtime_minutes_query = '''
    UPDATE movies
    SET runtimeMinutes =
        CASE
            WHEN genres LIKE '%Documentary%' THEN runtimeMinutes + 15
            WHEN genres LIKE '%Animation%' THEN runtimeMinutes + 30
            ELSE runtimeMinutes + 45
        END
'''

db.execute(update_runtime_minutes_query)
db.commit()

# Start the server
if __name__ == '__main__':
    app.run(port=3001)
