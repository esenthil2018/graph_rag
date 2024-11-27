import streamlit as st
import requests
from typing import Dict, List
import json

# Page config
st.set_page_config(
    page_title="Movie Graph Explorer",
    page_icon="🎬",
    layout="wide"
)

# API endpoint
API_URL = "http://localhost:8000"

def get_genre_emoji(genre: str) -> str:
    """Return emoji based on movie genre"""
    genre_emojis = {
        'Action': '💥',
        'Adventure': '🗺️',
        'Comedy': '😄',
        'Drama': '🎭',
        'Horror': '👻',
        'Sci-Fi': '🚀',
        'Thriller': '😱',
        'Crime': '🕵️',
        'Romance': '❤️',
        'Fantasy': '🧙‍♂️'
    }
    return genre_emojis.get(genre, '🎬')

def search_movies(query: str, search_type: str = None) -> Dict:
    """Search movies via API with proper prefix based on search type"""
    try:
        # Clean the query
        query = query.strip()
        
        # Format query based on search type
        if search_type:
            # Extract just the name/term from natural language queries
            clean_query = query.lower()
            clean_query = clean_query.replace('what are the ', '')
            clean_query = clean_query.replace('list ', '')
            clean_query = clean_query.replace('show me ', '')
            clean_query = clean_query.replace('find ', '')
            clean_query = clean_query.replace(' movies', '')
            clean_query = clean_query.replace('?', '')
            clean_query = clean_query.strip()
            
            formatted_query = f"{search_type}:{clean_query}"
        else:
            formatted_query = query
            
        # URL encode the query
        formatted_query = requests.utils.quote(formatted_query)
        
        print(f"Making API call to: {API_URL}/movies/search/{formatted_query}")
        response = requests.get(f"{API_URL}/movies/search/{formatted_query}")
        
        if response.status_code != 200:
            st.error(f"API Error: {response.status_code}")
            print(f"API Error Response: {response.text}")
            return {"message": "Error", "results": []}
            
        result = response.json()
        print(f"API Response: {json.dumps(result, indent=2)}")
        return result
    except Exception as e:
        print(f"Search error: {str(e)}")
        st.error(f"Error connecting to API: {str(e)}")
        return {"message": "Error", "results": []}

def check_api_health():
    """Check if the API is healthy"""
    try:
        response = requests.get(f"{API_URL}/health")
        return response.status_code == 200
    except:
        return False

# Check API health
api_healthy = check_api_health()
if not api_healthy:
    st.error("⚠️ Cannot connect to the Movie API. Please make sure the API server is running.")
    st.stop()

# Sidebar navigation
with st.sidebar:
    st.title("🎯 Navigation")
    st.write("Choose your adventure:")
    
    # Navigation options
    nav_option = st.selectbox(
        "Search by:",
        options=[
            "🔍 Search Movies",
            "👥 Search by Actor",
            "🎬 Search by Director",
            "🎪 Search by Genre"
        ],
        key="nav_select"
    )

# Main content
st.title("🎬 Movie Graph Explorer 🎥")
st.write("Welcome to the Movie Graph Explorer! Discover movies, actors, and directors in an interactive way! Let's make movie exploration fun!🎉")

# Get search type based on navigation
search_type = None
placeholder_text = ""

if "Search Movies" in nav_option:
    st.header("🔍 Search Movies")
    placeholder_text = "Try: 'What movies are similar to The Matrix?'"
    search_type = None
    
elif "Search by Actor" in nav_option:
    st.header("👥 Search by Actor")
    search_type = "actor"
    placeholder_text = "Try: 'Keanu Reeves'"
    
elif "Search by Director" in nav_option:
    st.header("🎬 Search by Director")
    search_type = "director"
    placeholder_text = "Try: 'Christopher Nolan'"
    
else:  # Genre search
    st.header("🎪 Search by Genre")
    search_type = "genre"
    genres = [
        "Action", "Adventure", "Comedy", "Drama", 
        "Horror", "Sci-Fi", "Thriller", "Crime", 
        "Romance", "Fantasy"
    ]
    
    genre = st.selectbox(
        "Select genre",
        options=genres
    )
    
    if genre:
        search_query = genre
        if st.button(f"Search {genre} Movies"):
            with st.spinner("🎬 Searching for movies..."):
                results = search_movies(genre, search_type)
                st.session_state.search_results = results
    else:
        search_query = None

# Show text input for non-genre searches
if "Genre" not in nav_option:
    col1, col2 = st.columns([3, 1])
    with col1:
        search_query = st.text_input(
            "Enter your search term",
            placeholder=placeholder_text,
            key="search_input"
        ).strip()
    
    with col2:
        search_button = st.button("🔍 Search", key="search_button")

    # Process search if query exists and button is clicked
    if search_query and search_button:
        with st.spinner("🎬 Searching for movies..."):
            results = search_movies(search_query, search_type)
            st.session_state.search_results = results

# Display results if they exist
if hasattr(st.session_state, 'search_results'):
    results = st.session_state.search_results
    
    # Show interpretation if available
    if results.get("interpretation"):
        st.info(f"🔍 {results['interpretation']}")
    
    # Show debug information in expander
    with st.expander("🔧 Debug Information", expanded=False):
        st.write("Search Configuration:")
        st.write({
            "Search Type": search_type,
            "Query": search_query,
            "API URL": API_URL
        })
        if results.get("debug"):
            st.write("Query Debug:")
            st.json(results["debug"])
    
    # Display results
    if results.get("results"):
        for movie in results["results"]:
            with st.container():
                st.markdown(f"""
                <div style="padding: 20px; border-radius: 10px; background-color: #f0f2f6; margin-bottom: 20px;">
                    <h3>{get_genre_emoji(movie.get('genre', ''))} {movie.get('title', 'Unknown Title')}</h3>
                    <p>🎪 Genre: {movie.get('genre', 'Unknown Genre')}</p>
                    <p>🎭 Director: {movie.get('director', 'Unknown Director')}</p>
                    <p>👥 Actors: {', '.join(movie.get('actors', ['Unknown']))}</p>
                    <p>📖 {movie.get('description', 'No description available')}</p>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.warning("😢 No movies found matching your criteria.")

# Footer
st.markdown("---")
st.markdown("Made with ❤️ and 🎬 by Your Friendly Movie Explorer")

# Add some space at the bottom
st.markdown("<br><br>", unsafe_allow_html=True)