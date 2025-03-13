import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import sessionmaker
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Define PostgreSQL Connection String (Source and Destination)
POSTGRES_CONN_STR = (
    f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB')}"
)

POSTGRES_DEST_CONN_STR = (
    f"postgresql+psycopg://{os.getenv('POSTGRES_USER_DEST')}:{os.getenv('POSTGRES_PASSWORD_DEST')}"
    f"@{os.getenv('POSTGRES_HOST_DEST', 'localhost')}:{os.getenv('POSTGRES_PORT_DEST', '5432')}/{os.getenv('POSTGRES_DB_DEST')}"
)

# Create SQLAlchemy Engine
source_engine = create_engine(POSTGRES_CONN_STR)
destination_engine = create_engine(POSTGRES_DEST_CONN_STR)

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

# ✅ Create a session for transactions
Session = sessionmaker(bind=source_engine)
session = Session()


# Load Field Mapping
def load_mapping():
    """Load field mappings from the migration_mapping_document."""
    mapping_file = "mapping_doc/migration_mapping_doc.xlsx"
    df_mapping = pd.read_excel(mapping_file, sheet_name="Customer Ind-Corporate")

    # Extract mappings: Source → Destination, Default Values
    field_map = {k: v for k, v in zip(df_mapping["Source Field"], df_mapping["Destination Field"]) if pd.notna(k)}
    default_values = {k: v for k, v in zip(df_mapping["Destination Field"], df_mapping["Default Value"]) if pd.notna(k)}

    return field_map, default_values


# Extract data from staging DB
def extract_staging_data():
    """Fetch data from staging DB."""
    tables = ["stg_customers", "customer_uuids"]
    dataframes = {}

    with source_engine.connect() as conn:
        for table in tables:
            query = f"SELECT * FROM {table};"
            df = pd.read_sql(query, conn)
            dataframes[table] = df
    
    return dataframes

df_extracted = extract_staging_data()
stg_customers = df_extracted["stg_customers"]
customer_uuids = df_extracted["customer_uuids"]


# Transform Data
def transform_data(stg_customers_df, customer_uuids_df):
    """Join extracted data with pre-generated UUID mapping and apply transformations."""

    df_all = pd.merge(stg_customers_df, customer_uuids_df, on='customer_code', how='left')

    # Load Field Mapping
    field_map, default_values = load_mapping()

    # Apply Field Mappings
    df_all.rename(columns=field_map, inplace=True)

    # Define required columns from mapping
    all_ind_columns = set(field_map.values()).union(set(default_values.keys()))


    # Ensure all required columns exist in the DataFrame for individual and corporate
    for column in all_ind_columns:
        if column not in df_all.columns:
            df_all[column] = default_values.get(column, "")


    # Filter columns based on "Destination Field" in the mapping document
    df_all = df_all[list(all_ind_columns)]


    # Convert datetime fields to pandas datetime
    datetime_columns = ['createdAt', 'updatedAt']
    for col in datetime_columns:
        if col in df_all.columns:
            df_all[col] = pd.to_datetime(df_all[col], errors='coerce')  # Convert to datetime


    # Drop index to prevent misalignment
    df_all.reset_index(drop=True, inplace=True)

    print(df_all.columns)

    return df_all

df = transform_data(stg_customers, customer_uuids)



# Load Data into PostgreSQL
def load_data(df):
    """Load transformed data into `customer table` in PostgreSQL."""

    try:
        logging.info("📥 Loading data into Destination...")
        
        # Fetch valid columns from the destination table
        with destination_engine.connect() as conn:
            query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'customer'
            """)
            columns = conn.execute(query).fetchall()
            valid_columns = [col[0] for col in columns]  # List of valid columns in destination table

            # Ensure only valid columns are inserted
            df = df[[col for col in df.columns if col in valid_columns]]

            # Ensure missing columns in the DataFrame are filled with default values (e.g. None or specific defaults)
            for col in valid_columns:
                if col not in df.columns:
                    df[col] = None  # Default value, can be a specific value like "Unknown"


            # Delete Existing Records Before Insert
            with destination_engine.begin() as conn:
                conn.execute(
                    text("""TRUNCATE TABLE customer;""")
                )

            # Define PostgreSQL column type mappings (to handle UUID)
            dtype_map = {
                "customerId": UUID,
                "tenantId": UUID,
                "approverId": UUID,
                "initiatorId": UUID,
                "branchId": UUID
            }

            # Bulk Insert Data using pandas to_sql method
            df.to_sql(
                "customer",
                destination_engine,
                if_exists="append",
                index=False,
                dtype=dtype_map,  # Apply proper type mapping
                method="multi",   # Faster bulk insert
                chunksize=1000
            )

        logging.info("✅ Data successfully inserted into customer.")

    except Exception as e:
        logging.error(f"❌ An error occurred: {e}")
        raise e
    

    # Run the ETL Pipeline
def main():
    """Main function to transfer data from SQL Server to PostgreSQL."""
    try:
        logging.info("Starting data transfer process...")
        start_time = datetime.now()

        # Extract
        df_extracted = extract_staging_data()
        stg_customers_df = df_extracted["stg_customers"]
        customer_uuids_df = df_extracted["customer_uuids"]

        logging.info(f"Extracted {len(stg_customers_df)} records from stg_customers_df")
        logging.info(f"Extracted {len(customer_uuids_df)} records from customer_uuids_df")

        # Transform
        transformed_df = transform_data(stg_customers_df, customer_uuids_df)

        # Load
        load_data(transformed_df)

        end_time = datetime.now()
        logging.info(f"Start Datetime: {start_time}")
        logging.info(f"End Datetime: {end_time}")
        logging.info(f"✅ Data transfer completed successfully in {end_time - start_time}.")
        logging.info(f"Total records inserted: {len(transformed_df)}")

    except Exception as e:
        logging.error(f"❌ Data transfer failed: {e}")

if __name__ == "__main__":
    main()