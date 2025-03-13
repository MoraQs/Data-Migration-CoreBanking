import pandas as pd
import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import UUID

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

# Create a session for transactions
Session = sessionmaker(bind=source_engine)
session = Session()


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


# Load mapping document
def load_mapping():
    """Load mapping document and return relevant dictionaries."""

    # Mapping Document Path
    mapping_file = "mapping_doc/migration_mapping_doc.xlsx"

    # Load Individual and Corporate mappings
    df_ind_mapping = pd.read_excel(mapping_file, sheet_name="Customer Profile Individual")
    df_corp_mapping = pd.read_excel(mapping_file, sheet_name="Customer Profile Corporate")

    # Load JSON Field sheets
    df_json_ind = pd.read_excel(mapping_file, sheet_name="JSON Field Individual")
    df_json_corp = pd.read_excel(mapping_file, sheet_name="JSON Field Corporate")

    # Convert mappings to dictionaries
    ind_map = {k:v for k, v in zip(df_ind_mapping["Source Field"], df_ind_mapping["Destination Field"]) if pd.notna(k)}
    corp_map = {k:v for k, v in zip(df_corp_mapping["Source Field"], df_corp_mapping["Destination Field"]) if pd.notna(k)}

    # Default Values
    ind_defaults = {k: v for k, v in zip(df_ind_mapping["Destination Field"], df_ind_mapping["Default Value"]) if pd.notna(k)}
    corp_defaults = {k: v for k, v in zip(df_corp_mapping["Destination Field"], df_corp_mapping["Default Value"]) if pd.notna(k)}

    # Extract JSON Fields in the **exact order** from the document
    json_ind_fields = df_json_ind["Destination Field"].dropna().tolist()  #  Preserves ordinal order in Excel Mapping Document
    json_corp_fields = df_json_corp["Destination Field"].dropna().tolist()  #  Preserves ordinal order in Excel Mapping Document

    return ind_map, corp_map, ind_defaults, corp_defaults, json_ind_fields, json_corp_fields

# Load mapping document
ind_map, corp_map, ind_defaults, corp_defaults, json_ind_fields, json_corp_fields = load_mapping()


def build_json(row, json_fields, defaults):
    """Constructs the JSON structure for `customerProfileData`, ensuring:

    - Date fields are converted to string format (YYYY-MM-DD).
    - Missing values (NaT, None) are replaced with an empty string "".
    """
    structured_json = {} # Use a dictionary to maintain order

    for field in json_fields:  # Maintain order from Excel
        value = row.get(field, defaults.get(field, ""))

        # Convert date, and Timestamp to string format
        if isinstance(value, (pd.Timestamp)):
            value = value.strftime('%Y-%m-%d')  # Correct datetime handling

        # Handle missing values properly
        if pd.isna(value):
            value = ""

        # Ensure JSON contains only serializable data
        if not isinstance(value, (str, int, float, bool, list, dict, type(None))):
            value = str(value)

        structured_json[field] = value  # Assign value to JSON field

    return structured_json


import pandas as pd

# Transform Data
def transform_data(stg_customers_df, customer_uuids_df):
    """Join extracted data with pre-generated UUID mapping and apply transformations."""

    df = pd.merge(stg_customers_df, customer_uuids_df, on='customer_code', how='left')

    # Split DataFrame into Individual & Corporate
    df_ind = df[df["customer_type"] == "Individual"].copy()
    df_corp = df[df["customer_type"] == "SME"].copy()

    # Apply Field Mappings
    df_ind.rename(columns=ind_map, inplace=True)
    df_corp.rename(columns=corp_map, inplace=True)

    # Define required columns from mapping
    all_ind_columns = set(ind_map.values()).union(set(ind_defaults.keys()))
    all_corp_columns = set(corp_map.values()).union(set(corp_defaults.keys()))

    # Ensure all required columns exist in the DataFrame for individual and corporate
    for column in all_ind_columns:
        if column not in df_ind.columns:
            df_ind[column] = ind_defaults.get(column, "")

    for column in all_corp_columns:
        if column not in df_corp.columns:
            df_corp[column] = corp_defaults.get(column, "")

    # Filter columns based on "Destination Field" in the mapping document
    df_ind = df_ind[list(all_ind_columns)]
    df_corp = df_corp[list(all_corp_columns)]
  

    # Apply JSON transformation to both dataframes (Individual & Corporate)
    df_ind["customerProfileData"] = df_ind.apply(lambda x: build_json(x, json_ind_fields, ind_defaults), axis=1)
    df_corp["customerProfileData"] = df_corp.apply(lambda x: build_json(x, json_corp_fields, corp_defaults), axis=1)

    # Consolidate Individual & Corporate DataFrames into a single DataFrame
    df_final = pd.concat([df_ind, df_corp], ignore_index=True)

    # Convert datetime fields to pandas datetime
    datetime_columns = ['createdAt', 'updatedAt']
    for col in datetime_columns:
        if col in df_final.columns:
            df_final[col] = pd.to_datetime(df_final[col], errors='coerce')  # Convert to datetime


    # Convert integer fields to string where required (PostgreSQL expects text for certain columns)
    int_to_str_columns = ["customerNumber", "bvn"]
    for col in int_to_str_columns:
        if col in df_final.columns:
            df_final[col] = df_final[col].astype(str)

    # Drop index to prevent misalignment
    df_final.reset_index(drop=True, inplace=True)

    return df_final


# Load data into PostgreSQL using SQLAlchemy connection
def load_data(df_final):
    """Load transformed data into PostgreSQL using SQLAlchemy."""
    try:
        logging.info("📥 Loading data into Destination...")
        
        # Fetch valid columns from the destination table
        with destination_engine.connect() as conn:
            query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'customer_profile'
            """)
            columns = conn.execute(query).fetchall()
            valid_columns = [col[0] for col in columns]  # List of valid columns in destination table

            # Ensure only valid columns are inserted
            df_final = df_final[[col for col in df_final.columns if col in valid_columns]]

            # Ensure missing columns in the DataFrame are filled with default values
            for col in valid_columns:
                if col not in df_final.columns:
                    df_final[col] = None  # Default value (can be set to a specific value like "Unknown")

            # Define PostgreSQL column type mappings (to handle UUID and JSONB)
            dtype_map = {
                "customerId": UUID,
                "customerProfileId": UUID,
                "customerProfileData": JSONB
            }

            # Delete Existing Records Before Inserting new ones in order not to violate unique constraints
            with destination_engine.begin() as conn:
                conn.execute(
                    text("""TRUNCATE TABLE customer_profile;""")
                )
            
            # Bulk Insert Data using pandas to_sql method
            df_final.to_sql(
                "customer_profile",
                destination_engine,
                if_exists="append",
                index=False,
                dtype=dtype_map,  # Apply proper type mapping
                method="multi",   # Faster bulk insert
                chunksize=1000
            )

        logging.info("✅ Data successfully inserted into customer_profile.")

    except Exception as e:
        logging.error(f"❌ An error occurred: {e}")
        raise e
    

# Run the ETL Pipeline
def main():
    """Main function to transfer data to PostgreSQL."""
    try:
        logging.info("Starting data transfer process...")
        start_time = datetime.now()

        # Extract
        df_extracted = extract_staging_data()
        stg_customers_df = df_extracted["stg_customers"]
        customer_uuids_df = df_extracted["customer_uuids"]

        logging.info(f"📌 Extracted {len(stg_customers_df)} records from stg_customers_df")
        logging.info(f"📌 Extracted {len(customer_uuids_df)} records from customer_uuids_df")

        # Transform
        # Pass both dataframes to transform_data function
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