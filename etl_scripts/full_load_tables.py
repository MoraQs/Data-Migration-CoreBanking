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
    f"mssql+pyodbc://{os.getenv('SQL_SERVER_USER')}:{os.getenv('SQL_SERVER_PASSWORD')}"
    f"@{os.getenv('SQL_SERVER_HOST')}/{os.getenv('SQL_SERVER_DB')}?driver=ODBC+Driver+17+for+SQL+Server"
)

# Define Staging Environment Connection Strings
POSTGRES_CONN_STR = (
    f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}"
)

# Configure logging
log_dir = os.path.join(os.getcwd(), "log")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, "data_migration.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Create sqlalchemy engines
sql_server_engine = create_engine(SQL_SERVER_CONN_STR, echo=False)
postgres_engine = create_engine(POSTGRES_CONN_STR, echo=False)

# Create a session for transactions
Session = sessionmaker(bind=postgres_engine)
session = Session()

# Define Extract Function
def extract_data():
    """Fetch data from source DB (SQL Server)."""
    try:
        logging.info("Extracting data from SQL Server...")

        query = "SELECT * FROM efz_customers;"
        df = pd.read_sql(query, sql_server_engine)
        
        logging.info(f"Extracted {len(df)} records from SQL Server.")
        return df
    
    except Exception as e:
        logging.error(f"Data extraction failed: {e}")
        raise

# Run and display extracted data
df_extracted = extract_data()


# Loading transformed data into the Analytics Database (PostgreSQL)
def load_to_staging():
    """Loading ingested data to staging environment (PostgreSQL)."""
    try:
        logging.info("Loading extracted data to PostgreSQL...")

        df_extracted.to_sql("stg_customers", postgres_engine, if_exists="replace", index=False)

        logging.info("Data loaded into PostgreSQL.")

    except Exception as e:
        logging.error(f"Data loading failed: {e}")
        raise


# Run the Extract & Load to staging pipeline
def main():
    """Main function to transfer data from source DB to staging."""
    try:
        logging.info("Starting data transfer process...")
        start_time = datetime.now()

        # Extract
        df = extract_data()
        logging.info(f"Extracted {len(df)} records from Source DB")

        # Load
        load_to_staging()

        end_time = datetime.now()
        logging.info(f"Data transfer completed successfully in {end_time - start_time}.")

    except Exception as e:
        logging.error(f"Data transfer failed: {e}")

if __name__ == "__main__":
    main()