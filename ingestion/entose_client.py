import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Data paths
DATA_PATH = "data/raw/"

# Belgium country code
COUNTRY_CODE = "BE"


def load_prices_from_csv(file_path: str) -> pd.DataFrame:
    """
    Load day-ahead electricity prices from a CSV file.
    
    Args:
        file_path: Path to the CSV file from ENTSO-E File Library
    
    Returns:
        DataFrame with cleaned price data
    """
    logger.info(f"Loading prices from {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows from {file_path}")
        return df
    
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    
    except Exception as e:
        logger.error(f"Error loading prices: {e}")
        raise


def load_generation_from_csv(file_path: str) -> pd.DataFrame:
    """
    Load electricity generation by source from a CSV file.
    
    Args:
        file_path: Path to the CSV file from ENTSO-E File Library
    
    Returns:
        DataFrame with cleaned generation data
    """
    logger.info(f"Loading generation data from {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows from {file_path}")
        return df
    
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    
    except Exception as e:
        logger.error(f"Error loading generation data: {e}")
        raise

def load_load_from_csv(file_path: str) -> pd.DataFrame:
    """
    Load total electricity load (demand) from a CSV file.
    
    Args:
        file_path: Path to the CSV file from ENTSO-E File Library
    
    Returns:
        DataFrame with cleaned load data
    """
    logger.info(f"Loading load data from {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows from {file_path}")
        return df
    
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    
    except Exception as e:
        logger.error(f"Error loading load data: {e}")
        raise