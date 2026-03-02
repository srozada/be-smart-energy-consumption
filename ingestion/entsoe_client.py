import os
import pandas as pd
from entsoe import EntsoePandasClient
from datetime import datetime
import logging
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Belgium country code
COUNTRY_CODE = "BE"


def get_client() -> EntsoePandasClient:
    """
    Initialize and return the ENTSO-E API client.
    
    Returns:
        EntsoePandasClient instance
    """
    api_key = os.getenv("ENTSOE_API_KEY")
    
    if not api_key:
        raise ValueError("ENTSOE_API_KEY not found in environment variables")
    
    logger.info("ENTSO-E client initialized successfully")
    return EntsoePandasClient(api_key=api_key)


def fetch_day_ahead_prices(start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch day-ahead electricity prices for Belgium from ENTSO-E API.
    
    Args:
        start: Start datetime
        end: End datetime
    
    Returns:
        DataFrame with hourly day-ahead prices
    """
    logger.info(f"Fetching day-ahead prices from {start} to {end}")
    
    try:
        client = get_client()
        
        prices = client.query_day_ahead_prices(
            COUNTRY_CODE,
            start=pd.Timestamp(start, tz='UTC'),
            end=pd.Timestamp(end, tz='UTC')
        )
        
        df = prices.reset_index()
        df.columns = ['timestamp', 'price_eur']
        df['currency'] = 'EUR'
        
        logger.info(f"Fetched {len(df)} price records")
        return df
    
    except Exception as e:
        logger.error(f"Error fetching day-ahead prices: {e}")
        raise


def fetch_generation(start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch actual generation per production type for Belgium from ENTSO-E API.
    
    Args:
        start: Start datetime
        end: End datetime
    
    Returns:
        DataFrame with hourly generation by energy source
    """
    logger.info(f"Fetching generation data from {start} to {end}")
    
    try:
        client = get_client()
        
        generation = client.query_generation(
            COUNTRY_CODE,
            start=pd.Timestamp(start, tz='UTC'),
            end=pd.Timestamp(end, tz='UTC'),
            psr_type=None
        )

        # Aplanar MultiIndex si existe
        if isinstance(generation.columns, pd.MultiIndex):
            generation.columns = [f"{a}_{b}" if b else a for a, b in generation.columns]

        df = generation.reset_index()
        timestamp_col = df.columns[0]

        df = df.melt(
            id_vars=timestamp_col,
            var_name='energy_type',
            value_name='quantity_mw'
        )
        df.columns = ['timestamp', 'energy_type', 'quantity_mw']
        df = df.dropna(subset=['quantity_mw'])
            
        logger.info(f"Fetched {len(df)} generation records")
        return df
    
    except Exception as e:
        logger.error(f"Error fetching generation data: {e}")
        raise


def fetch_load(start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch actual total load for Belgium from ENTSO-E API.
    
    Args:
        start: Start datetime
        end: End datetime
    
    Returns:
        DataFrame with hourly total load
    """
    logger.info(f"Fetching load data from {start} to {end}")
    
    try:
        client = get_client()
        
        load = client.query_load(
            COUNTRY_CODE,
            start=pd.Timestamp(start, tz='UTC'),
            end=pd.Timestamp(end, tz='UTC')
        )
        
        df = load.reset_index()
        df.columns = ['timestamp', 'load_mw']
        df = df.dropna(subset=['load_mw'])
        
        logger.info(f"Fetched {len(df)} load records")
        return df
    
    except Exception as e:
        logger.error(f"Error fetching load data: {e}")
        raise


def insert_dataframe(df: pd.DataFrame, table_name: str, connection_string: str) -> None:
    """
    Insert a DataFrame into a PostgreSQL table using psycopg2 directly.
    """
    logger.info(f"Inserting {len(df)} rows into {table_name}")
    
    try:
        import psycopg2
        from psycopg2.extras import execute_values
        
        # Parse connection string
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        db = os.getenv("POSTGRES_DB")
        host = os.getenv("POSTGRES_HOST", "postgres")
        port = os.getenv("POSTGRES_PORT", "5432")
        
        conn = psycopg2.connect(
            host=host, port=port, dbname=db, user=user, password=password
        )
        
        columns = ', '.join(df.columns)
        values = [tuple(row) for row in df.itertuples(index=False)]
        query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
        
        with conn.cursor() as cur:
            execute_values(cur, query, values)
        conn.commit()
        conn.close()
        
        logger.info(f"Successfully inserted {len(df)} rows into {table_name}")
    
    except Exception as e:
        logger.error(f"Error inserting data into {table_name}: {e}")
        raise


def get_db_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    
    if not all([user, password, db]):
        raise ValueError("Missing PostgreSQL credentials in environment variables")
    
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    logger.info(f"Connecting to PostgreSQL at {host}:{port}/{db}")
    
    return create_engine(connection_string)



def run_ingestion(start: datetime, end: datetime) -> None:
    logger.info(f"Starting ingestion pipeline for {start} to {end}")
    
    prices_df = fetch_day_ahead_prices(start, end)
    insert_dataframe(prices_df, 'raw_prices', None)
    
    generation_df = fetch_generation(start, end)
    insert_dataframe(generation_df, 'raw_generation', None)
    
    load_df = fetch_load(start, end)
    insert_dataframe(load_df, 'raw_load', None)
    
    logger.info("Ingestion pipeline completed successfully")



if __name__ == "__main__":
    from datetime import timedelta
    
    end = datetime.utcnow()
    start = end - timedelta(days=7)
    
    run_ingestion(start, end)