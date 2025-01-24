# filepath: /Users/kniu91/Documents/projects/ffwrapped_be/ffwrapped_be/config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    pfref_base = os.getenv('PFREF_BASE')
    
    railway_db_url = os.getenv('RAILWAY_DB_URL')
    railway_db_user = os.getenv('RAILWAY_DB_USER')
    railway_db_password = os.getenv('RAILWAY_DB_PASSWORD')

config = Config()