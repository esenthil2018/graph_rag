import pandas as pd
import numpy as np
from neo4j import GraphDatabase
import chromadb
import traceback
from tqdm import tqdm
import os
from typing import List, Dict, Any

class DataProcessor:
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        """Initialize the data processor with database connections"""
        self.neo4j_driver = GraphDatabase.driver(
            neo4j_uri, auth=(neo4j_user, neo4j_password)
        )
        
        # Initialize ChromaDB
        self.chroma_client = chromadb.Client()
        try:
            self.collection = self.chroma_client.create_collection(name="movie_descriptions")
            print("Created new ChromaDB collection")
        except:
            self.collection = self.chroma_client.get_collection(name="movie_descriptions")
            print("Using existing ChromaDB collection")

    def parse_csv(self, filepath: str) -> pd.DataFrame:
        """Parse and validate the movies CSV file"""
        try:
            print("\nStep 1: Loading and validating CSV...")
            
            # Read CSV
            df = pd.read_csv(filepath)
            
            # Ensure required columns exist
            required_columns = ['movie_id', 'title', 'director', 'genre', 'actors', 'description']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Clean and validate data
            df['movie_id'] = df['movie_id'].astype(str)
            df['title'] = df['title'].fillna('')
            df['director'] = df['director'].fillna('')
            df['genre'] = df['genre'].fillna('')
            df['description'] = df['description'].fillna('')
            
            # Convert pipe-separated actors to lists
            df['actors'] = df['actors'].apply(lambda x: [] if pd.isna(x) else [a.strip() for a in str(x).split('|')])
            
            # Print sample data for verification
            print("\nSample data after processing:")
            print(df[['movie_id', 'title', 'actors']].head())
            print(f"\nLoaded {len(df)} records from CSV")
            
            return df
            
        except Exception as e:
            print(f"Error reading CSV file: {str(e)}")
            print("\nDetailed error information:")
            traceback.print_exc()
            raise

    def load_to_neo4j(self, df: pd.DataFrame):
        """Load data into Neo4j"""
        try:
            with self.neo4j_driver.session() as session:
                print("\nStep 2: Loading to Neo4j...")
                
                # Clear existing data
                session.run("MATCH (n) DETACH DELETE n")
                
                # Create indexes
                print("\nCreating indexes...")
                session.run("CREATE INDEX movie_id IF NOT EXISTS FOR (m:Movie) ON (m.movie_id)")
                session.run("CREATE INDEX actor_name IF NOT EXISTS FOR (a:Actor) ON (a.name)")
                session.run("CREATE INDEX director_name IF NOT EXISTS FOR (d:Director) ON (d.name)")
                
                # Process each movie
                for _, row in df.iterrows():
                    # Create Movie node
                    movie_query = """
                    CREATE (m:Movie {
                        movie_id: $movie_id,
                        title: $title,
                        genre: $genre,
                        description: $description
                    })
                    """
                    session.run(movie_query, {
                        'movie_id': row['movie_id'],
                        'title': row['title'],
                        'genre': row['genre'],
                        'description': row['description']
                    })
                    
                    # Create Director node and relationship
                    director_query = """
                    MERGE (d:Director {name: $director})
                    WITH d
                    MATCH (m:Movie {movie_id: $movie_id})
                    CREATE (d)-[:DIRECTED]->(m)
                    """
                    session.run(director_query, {
                        'director': row['director'],
                        'movie_id': row['movie_id']
                    })
                    
                    # Create Actor nodes and relationships
                    for actor in row['actors']:
                        actor_query = """
                        MERGE (a:Actor {name: $actor})
                        WITH a
                        MATCH (m:Movie {movie_id: $movie_id})
                        CREATE (a)-[:ACTED_IN]->(m)
                        """
                        session.run(actor_query, {
                            'actor': actor,
                            'movie_id': row['movie_id']
                        })
                
                # Verify data loading
                movie_count = session.run("MATCH (m:Movie) RETURN count(m) as count").single()["count"]
                actor_count = session.run("MATCH (a:Actor) RETURN count(a) as count").single()["count"]
                director_count = session.run("MATCH (d:Director) RETURN count(d) as count").single()["count"]
                
                print(f"\nSuccessfully loaded:")
                print(f"- {movie_count} movies")
                print(f"- {actor_count} actors")
                print(f"- {director_count} directors")
                
                # Print sample data
                print("\nSample data in Neo4j:")
                sample = session.run("""
                    MATCH (m:Movie)<-[:ACTED_IN]-(a:Actor)
                    RETURN m.title as movie, collect(a.name) as actors
                    LIMIT 3
                """).data()
                print(sample)
                
        except Exception as e:
            print(f"Error loading to Neo4j: {str(e)}")
            print("\nDetailed error information:")
            traceback.print_exc()
            raise

    def load_to_chroma(self, df: pd.DataFrame):
        """Load movie descriptions into ChromaDB"""
        try:
            print("\nStep 3: Loading to ChromaDB...")
            
            # Prepare data for ChromaDB
            print("Preparing data for ChromaDB...")
            
            # Process records with progress bar
            for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing records"):
                try:
                    # Create document with movie information
                    document = f"Movie: {row['title']}\nGenre: {row['genre']}\nDescription: {row['description']}"
                    
                    # Add to ChromaDB
                    self.collection.add(
                        documents=[document],
                        metadatas=[{
                            'movie_id': row['movie_id'],
                            'title': row['title'],
                            'genre': row['genre']
                        }],
                        ids=[f"movie_{row['movie_id']}"]
                    )
                except Exception as e:
                    print(f"Error processing record {row['movie_id']}: {str(e)}")
                    
        except Exception as e:
            print(f"Error loading to ChromaDB: {str(e)}")
            print("\nDetailed error information:")
            traceback.print_exc()
            raise

    def verify_data(self):
        """Verify data in both databases"""
        try:
            print("\nStep 4: Verifying data...")
            
            print("\nVerifying processed data...")
            
            # Neo4j statistics
            with self.neo4j_driver.session() as session:
                print("\nNeo4j Statistics:")
                
                # Count nodes
                movie_count = session.run("MATCH (m:Movie) RETURN count(m) as count").single()["count"]
                print(f"Total movies: {movie_count}")
                
                # Count relationships
                rel_stats = session.run("""
                    MATCH (a:Actor)-[r:ACTED_IN]->(m:Movie)
                    RETURN count(r) as total_rels,
                           count(distinct a) as unique_actors,
                           count(distinct m) as unique_movies
                """).single()
                
                print("\nRelationship Statistics:")
                print(f"Total ACTED_IN relationships: {rel_stats['total_rels']}")
                print(f"Unique actors: {rel_stats['unique_actors']}")
                print(f"Unique movies: {rel_stats['unique_movies']}")
                
                # Sample relationships
                print("\nSample Relationships (with properties):")
                sample_rels = session.run("""
                    MATCH (a:Actor)-[r:ACTED_IN]->(m:Movie)
                    RETURN a.name as actor, m.title as movie
                    LIMIT 5
                """).data()
                print(sample_rels)
            
            # ChromaDB statistics
            print("\nChromaDB Statistics:")
            print(f"Total documents: {self.collection.count()}")
            
        except Exception as e:
            print(f"Error verifying data: {str(e)}")
            print("\nDetailed error information:")
            traceback.print_exc()
            raise

    def close(self):
        """Close database connections"""
        self.neo4j_driver.close()

def main():
    # Initialize processor
    processor = DataProcessor(
        neo4j_uri="bolt://localhost:7687",
        neo4j_user="neo4j",
        neo4j_password="Akhil_2002"
    )
    
    try:
        # Process data
        df = processor.parse_csv('app/data/movies_data.csv')
        processor.load_to_neo4j(df)
        processor.load_to_chroma(df)
        processor.verify_data()
        
    except Exception as e:
        print(f"Error in main process: {str(e)}")
        traceback.print_exc()
        
    finally:
        processor.close()

if __name__ == "__main__":
    main()