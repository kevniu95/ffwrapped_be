import requests
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# FastAPI backend URL
BASE_URL = "http://localhost:8000"

# Cache for API responses to minimize backend calls
response_cache = {}


# Method to fetch data
def fetch_lineup_data(endpoint, team_id):
    """Fetches lineup data for a given team with caching"""
    cache_key = f"{endpoint}_{team_id}"

    if cache_key in response_cache:
        logger.info(f"Using cached data for {endpoint}, team {team_id}")
        return response_cache[cache_key]

    logger.info(f"Fetching data from {endpoint} for team {team_id}")
    url = f"{BASE_URL}/{endpoint}?teamId={team_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        response_cache[cache_key] = data
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {endpoint}: {e}")
        return {}
