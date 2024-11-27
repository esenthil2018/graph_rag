from fastapi import FastAPI, HTTPException
from neo4j import GraphDatabase
from typing import List, Dict, Optional
from pydantic import BaseModel
from openai import OpenAI
import os
from dotenv import load_dotenv
import traceback
import json

load_dotenv()

app = FastAPI(title="Movie Graph Explorer")

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# Neo4j setup
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'password')
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

class MovieResponse(BaseModel):
    title: str
    genre: str
    description: str
    director: str
    actors: List[str]

def execute_search_query(session, cypher_query: str, params: Dict) -> List[Dict]:
    """Execute a Cypher query and return results"""
    try:
        print(f"Executing query: {cypher_query} with params: {params}")  # Debug print
        results = session.run(cypher_query, params).data()
        print(f"Query results: {results}")  # Debug print
        return results
    except Exception as e:
        print(f"Query execution error: {str(e)}")
        raise

def generate_cypher_with_llm(query: str) -> dict:
    """Use LLM to generate Cypher query from natural language"""
    system_prompt = """You are a Neo4j Cypher query generator for a movie database with the following schema and data:

    Database Schema & Statistics:
    - Movies (15 total): title, genre, description, movie_id
    - Directors (13 total): name
    - Actors (44 total): name
    - Relationships: [:ACTED_IN] (45), [:DIRECTED] (15)

    Available Genres: Sci-Fi, Action, Crime, Drama, Adventure, Romance, Thriller, Fantasy

    Example Movie Structure:
    {
        "title": "The Matrix",
        "genre": "Sci-Fi",
        "description": "A computer programmer discovers...",
        "director": "Wachowski Sisters",
        "actors": ["Keanu Reeves", "Laurence Fishburne", "Carrie-Anne Moss"]
    }

    Common Query Patterns:

    1. Finding similar movies:
    ```cypher
    MATCH (m:Movie {title: $title})
    MATCH (similar:Movie)
    WHERE similar.genre = m.genre AND m <> similar
    OPTIONAL MATCH (d:Director)-[:DIRECTED]->(similar)
    OPTIONAL MATCH (a:Actor)-[:ACTED_IN]->(similar)
    WITH similar, d.name as director, collect(DISTINCT a.name) as actors
    RETURN similar.title as title,
           similar.genre as genre,
           similar.description as description,
           director,
           actors
    LIMIT 5
    ```

    2. Finding movies by actor:
    ```cypher
    MATCH (a:Actor)
    WHERE toLower(a.name) CONTAINS toLower($actor_name)
    MATCH (m:Movie)<-[:ACTED_IN]-(a)
    OPTIONAL MATCH (d:Director)-[:DIRECTED]->(m)
    OPTIONAL MATCH (coActor:Actor)-[:ACTED_IN]->(m)
    WITH m, d.name as director, collect(DISTINCT coActor.name) as actors
    RETURN m.title as title,
           m.genre as genre,
           m.description as description,
           director,
           actors
    LIMIT 5
    ```

    3. Finding movies by director:
    ```cypher
    MATCH (d:Director)
    WHERE toLower(d.name) CONTAINS toLower($director_name)
    MATCH (m:Movie)<-[:DIRECTED]-(d)
    OPTIONAL MATCH (a:Actor)-[:ACTED_IN]->(m)
    WITH m, d.name as director, collect(DISTINCT a.name) as actors
    RETURN m.title as title,
           m.genre as genre,
           m.description as description,
           director,
           actors
    LIMIT 5
    ```

    4. Finding movies by genre:
    ```cypher
    MATCH (m:Movie)
    WHERE toLower(m.genre) = toLower($genre)
    OPTIONAL MATCH (d:Director)-[:DIRECTED]->(m)
    OPTIONAL MATCH (a:Actor)-[:ACTED_IN]->(m)
    WITH m, d.name as director, collect(DISTINCT a.name) as actors
    RETURN m.title as title,
           m.genre as genre,
           m.description as description,
           director,
           actors
    LIMIT 5
    ```

    Always return JSON with:
    1. "cypher": The Cypher query string
    2. "params": Parameter dictionary
    3. "explanation": Plain English explanation of what the query does

    Requirements:
    1. Use OPTIONAL MATCH for relationships to handle missing data
    2. Always use case-insensitive matching (toLower())
    3. Always collect actor names using collect(DISTINCT a.name)
    4. Always return all fields: title, genre, description, director, actors
    5. Always limit results to 5
    6. Use parameters for user inputs
    """

    try:
        # Parse special prefixes
        if query.startswith(('actor:', 'director:', 'genre:')):
            prefix, value = query.split(':', 1)
            value = value.strip()
            
            # Clean up the search value by removing common question patterns
            value = value.lower()
            value = value.replace('what are the ', '')
            value = value.replace('list ', '')
            value = value.replace('show me ', '')
            value = value.replace('find ', '')
            value = value.replace(' movies', '')
            value = value.replace('?', '')
            value = value.strip()
            
            if prefix == 'actor':
                return {
                    "cypher": """
                    MATCH (a:Actor)
                    WHERE toLower(a.name) CONTAINS toLower($value)
                    MATCH (m:Movie)<-[:ACTED_IN]-(a)
                    OPTIONAL MATCH (d:Director)-[:DIRECTED]->(m)
                    OPTIONAL MATCH (coActor:Actor)-[:ACTED_IN]->(m)
                    WITH m, d.name as director, collect(DISTINCT coActor.name) as actors
                    RETURN m.title as title,
                           m.genre as genre,
                           m.description as description,
                           director,
                           actors
                    LIMIT 5
                    """,
                    "params": {"value": value},
                    "explanation": f"Finding movies featuring actor matching '{value}'"
                }
            elif prefix == 'director':
                return {
                    "cypher": """
                    MATCH (d:Director)
                    WHERE toLower(d.name) CONTAINS toLower($value)
                    MATCH (m:Movie)<-[:DIRECTED]-(d)
                    OPTIONAL MATCH (a:Actor)-[:ACTED_IN]->(m)
                    WITH m, d.name as director, collect(DISTINCT a.name) as actors
                    RETURN m.title as title,
                           m.genre as genre,
                           m.description as description,
                           director,
                           actors
                    LIMIT 5
                    """,
                    "params": {"value": value},
                    "explanation": f"Finding movies directed by '{value}'"
                }
            elif prefix == 'genre':
                return {
                    "cypher": """
                    MATCH (m:Movie)
                    WHERE toLower(m.genre) = toLower($value)
                    OPTIONAL MATCH (d:Director)-[:DIRECTED]->(m)
                    OPTIONAL MATCH (a:Actor)-[:ACTED_IN]->(m)
                    WITH m, d.name as director, collect(DISTINCT a.name) as actors
                    RETURN m.title as title,
                           m.genre as genre,
                           m.description as description,
                           director,
                           actors
                    LIMIT 5
                    """,
                    "params": {"value": value},
                    "explanation": f"Finding movies in the {value} genre"
                }

        # For natural language queries, use OpenAI
        messages = [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": f"Generate a Cypher query to answer: {query}. Return response in JSON format including cypher, params, and explanation."
            }
        ]

        response = client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=messages,
            response_format={"type": "json_object"},
            temperature=0.1
        )
        
        result = json.loads(response.choices[0].message.content)
        print(f"Generated Cypher: {result}")  # Debug print
        return result
        
    except Exception as e:
        print(f"LLM generation error: {str(e)}")
        # Fallback to simple search
        return {
            "cypher": """
            MATCH (m:Movie)
            OPTIONAL MATCH (d:Director)-[:DIRECTED]->(m)
            OPTIONAL MATCH (a:Actor)-[:ACTED_IN]->(m)
            WHERE toLower(m.title) CONTAINS toLower($query)
               OR toLower(m.description) CONTAINS toLower($query)
            WITH m, d.name as director, collect(DISTINCT a.name) as actors
            RETURN m.title as title,
                   m.genre as genre,
                   m.description as description,
                   director,
                   actors
            LIMIT 5
            """,
            "params": {"query": query},
            "explanation": "Fallback to general search due to error"
        }
@app.get("/movies/search/{query}")
async def search_movies(query: str) -> Dict:
    """Search movies using LLM-generated Cypher queries"""
    try:
        print(f"Received search query: {query}")  # Debug print
        
        with neo4j_driver.session() as session:
            # Generate Cypher query using LLM
            generated = generate_cypher_with_llm(query)
            
            # Execute the generated query
            results = execute_search_query(session, generated["cypher"], generated["params"])
            
            # Handle empty results
            if not results:
                return {
                    "message": "No movies found matching your criteria",
                    "interpretation": generated["explanation"],
                    "results": [],
                    "debug": {
                        "cypher": generated["cypher"],
                        "params": generated["params"]
                    }
                }
            
            return {
                "message": f"Found {len(results)} movies",
                "interpretation": generated["explanation"],
                "results": results,
                "debug": {
                    "cypher": generated["cypher"],
                    "params": generated["params"]
                }
            }

    except Exception as e:
        print(f"Search error: {str(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/database-stats")
async def get_database_stats():
    """Get statistics about the database"""
    try:
        with neo4j_driver.session() as session:
            movie_count = session.run("MATCH (m:Movie) RETURN count(m) as count").single()["count"]
            actor_count = session.run("MATCH (a:Actor) RETURN count(a) as count").single()["count"]
            director_count = session.run("MATCH (d:Director) RETURN count(d) as count").single()["count"]
            
            # Get sample data for verification
            sample_movies = session.run("""
                MATCH (m:Movie)
                OPTIONAL MATCH (d:Director)-[:DIRECTED]->(m)
                OPTIONAL MATCH (a:Actor)-[:ACTED_IN]->(m)
                WITH m, d, collect(a.name) as actors
                RETURN m.title as title, d.name as director, actors
                LIMIT 3
            """).data()
            
            return {
                "movies": movie_count,
                "actors": actor_count,
                "directors": director_count,
                "sample_data": sample_movies
            }
    except Exception as e:
        print(f"Error getting stats: {str(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# Add a health check endpoint
@app.get("/health")
async def health_check():
    """Check if the service is running and can connect to Neo4j"""
    try:
        with neo4j_driver.session() as session:
            # Try a simple query
            result = session.run("RETURN 1 as n").single()
            return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")