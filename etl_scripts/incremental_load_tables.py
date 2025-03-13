import pandas as pd
import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Define Source Database Connection Strings
SQL_SERVER_CONN_STR = (
    f"mssql+pyodbc://{os.getenv('SQL_SERVER_USER')}:{os.getenv('SQL_SERVER_PASSWORD')}@"
    f"{os.getenv('SQL_SERVER_HOST')}/{os.getenv('SQL_SERVER_DB')}?driver=ODBC+Driver+17+for+SQL+Server"
)

# Define Staging Environment Connection Strings
POSTGRES_CONN_STR = (
    f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}"
)

# Configure logging
log_dir = os.path.join(os.getcwd(), "log")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, "data_migration.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file, mode="a", encoding="utf-8"), logging.StreamHandler()]
)

# Create sqlalchemy engines
sql_server_engine = create_engine(SQL_SERVER_CONN_STR, echo=False)
postgres_engine = create_engine(POSTGRES_CONN_STR, echo=False)

# Create a session for transactions
Session = sessionmaker(bind=postgres_engine)
session = Session()

# Get the last ingestion time from PostgreSQL
def get_last_ingestion_time():
    """Fetch the last ingestion timestamp from the staging table."""
    try:
        query = "SELECT max(created_at::timestamp) as last_ingested_at FROM stg_customers;"
        result = pd.read_sql(query, postgres_engine)
        
        if not result.empty and result.iloc[0]["last_ingested_at"] is not None:
            return result.iloc[0]["last_ingested_at"]
        else:
            return None  # No previous ingestion, so load all data
    except Exception as e:
        logging.error(f"Error fetching last ingestion time: {e}")
        raise


# Define Incremental Extract Function
def extract_incremental_data(last_ingested_at):
    """Fetch only records that have been added or updated since the last ingestion."""
    try:
        logging.info("Extracting incremental data from SQL Server...")

        # Use last_ingested_at for filtering
        query = f"""
            SELECT * 
            FROM efz_customers
            WHERE created_at > '{last_ingested_at}';
        """
        
        df = pd.read_sql(query, sql_server_engine)
        
        logging.info(f"Extracted {len(df)} records from SQL Server.")
        return df
    
    except Exception as e:
        logging.error(f"Incremental data extraction failed: {e}")
        raise

# Define Load Function (No change needed from the full load)
def load_to_staging(df):
    """Loading extracted data to PostgreSQL."""
    try:
        logging.info("Loading incremental data records to staging PostgreSQL...")

        # Load data to PostgreSQL using append mode
        df.to_sql("stg_customers", postgres_engine, if_exists="append", index=False)

        logging.info("Data loaded into PostgreSQL.")
    except Exception as e:
        logging.error(f"Data loading failed: {e}")
        raise


# Update the last ingestion time in PostgreSQL
def update_last_ingestion_time(last_ingested_at):
    """Update the last ingestion time in the staging table after each load."""
    try:
        query = text("""
            INSERT INTO ingestion_incremental_log (table_name, last_ingested_at, last_updated_at)
            VALUES ('efz_customers', :last_ingested_at, now())
            ON CONFLICT (table_name) 
            DO UPDATE SET last_ingested_at = :last_ingested_at, last_updated_at = now();
        """)
        session.execute(query, {"last_ingested_at": last_ingested_at})
        session.commit()
    except Exception as e:
        logging.error(f"Error updating last ingestion time: {e}")
        session.rollback()
        raise


# Run the Incremental Extract & Load pipeline
def main():
    """Main function to transfer incremental data from source DB to staging."""
    try:
        logging.info("Starting incremental data transfer process...")
        start_time = datetime.now()

        # Step 1: Get the last ingestion time
        last_ingested_at = get_last_ingestion_time()
        if last_ingested_at:
            logging.info(f"Last ingestion time: {last_ingested_at}")
        else:
            logging.info("No previous ingestion found. Will perform full extraction.")

        # Step 2: Extract the incremental data
        df = extract_incremental_data(last_ingested_at)

        # Step 3: Load to Staging
        load_to_staging(df)

        # Step 4: Always update the last ingestion time in the ingestion log table
        if not df.empty:
            # Get the latest timestamp from the newly loaded data (from 'created_at' or 'updated_at')
            last_ingested_at = df['created_at'].max()  # Adjust if using 'updated_at' instead
        else:
            # If no data is extracted, set the `last_ingested_at` to current timestamp
            last_ingested_at = last_ingested_at

        logging.info(f"Updating last ingestion time to: {last_ingested_at}")
        update_last_ingestion_time(last_ingested_at)

        end_time = datetime.now()
        logging.info(f"Incremental data transfer completed successfully in {end_time - start_time}.")

    except Exception as e:
        logging.error(f"Incremental data transfer failed: {e}")

if __name__ == "__main__":
    main()
