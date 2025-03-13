# Data Migration (Core Banking Appplication specifics): Customer Data

## Project Overview

This project focuses on the migration of customer data from a source database (`efz_customer`) to a destination database with two primary destination tables:

1. `customer`
2. `customer_profile`

The migration process includes extracting data from the source, transforming it according to predefined mappings and business rules, and loading it into the destination tables. The destination schema is designed to match the target system’s structure, and includes transformation logic to ensure the data integrity and accuracy during the migration process.

## Migration Workflow

### 1. **Full Load from Source to Staging**

The first step in the migration process involves performing a **full load** of the source data into the staging area. This is done using the script `full_load_tables.py`, which extracts all the data from the source (`efz_customer`) table and loads it into the staging tables.

```bash
python full_load_tables.py
```

### 2. **Create UUIDs for Each Customer Code**

Before merging the customer data with UUID mappings, we need to generate UUIDs for each unique `customer_code`. The following SQL steps are performed to handle this:

```sql
-- Create UUIDs for each customer_code
create table customer_uuids (
    customer_code int primary key,
    "customerId" UUID default gen_random_uuid(),
    "customerProfileId" UUID default gen_random_uuid()
);

-- Insert customer UUIDs from the staging table and ensure that UUID is only generated for new customer_codes
insert into customer_uuids (customer_code)
select customer_code
from stg_customers
where customer_code not in (select customer_code from customer_uuids);
```

### 3. **Incremental Load Script**

For subsequent data migrations, an incremental load script (incremental_load_tables.py) is used. This script leverages the watermark column(s) (e.g., `created_at`) in the source table to only extract and migrate new or modified records since the last migration.

This method improves performance by only migrating the incremental changes instead of performing a full load each time. It also ensures that the data load is more efficient, particularly for mock migration exercises and during the `cutover window`, when time is critical.

```bash
python incremental_load_tables.py
```

### 4. **Transform Data**

Transformation is applied in the following steps:

- **Mapping Source to Destination**: The fields in the source (`efz_customer`) are mapped to their corresponding destination fields in the `customer` and `customer_profile` tables.
- **Default Value Assignment**: For missing or null values, default values are applied according to the predefined mapping document.
- **Data Type Conversion**: Certain fields (e.g., date fields) are converted to the appropriate data types.
- **Field Renaming**: Column names are renamed as per the mapping document to match the destination field names.

During transformation, the `customer_profile` table's `customerProfileData` column is populated with serialized data in the `jsonb` format. This column consolidates multiple individual KYC-related columns (such as `address`, `phone`, `email`, etc.) into a single `jsonb` field, ensuring efficient storage and easier querying for complex KYC information.

### 5. **Load Data into Destination Tables**

- **`customer` Table**: Contains primary customer details.
- **`customer_profile` Table**: Contains additional customer profile information, including the `customerProfileData` field in `jsonb` format that consolidates multiple KYC-related columns.

The transformed data is inserted into these destination tables, and in case of any conflicts (e.g., duplicate records), appropriate handling is implemented.

## Project Structure

The project is organized into the following key sections:

### 1. **Extracting Data**

Data is extracted in its raw format from the `efz_customer` table and loaded into the staging environment (postgreSQL). 

### 2. **UUID Generation and Insertion**

UUIDs are generated and inserted into the `customer_uuids` table before merging the data.

### 3. **Transformation Logic**

The transformation logic involves:

- Merging customer data with UUID mappings
- Renaming columns according to the mapping document
- Applying default values where necessary
- Ensuring proper data types (e.g., converting string fields to integers, handling date fields)
- Populating the `customerProfileData` column in `customer_profile` as a serialized `jsonb` field, consolidating multiple KYC-related columns.

### 4. **Loading Data into Destination**

The transformed data is loaded into the following tables:

- **`customer`**: Stores essential customer data.
- **`customer_profile`**: Stores detailed customer profile data, including the `customerProfileData` column in `jsonb` format.

### 4. **Logging and Error Handling**

- Detailed logging is provided for each step of the migration process.
- Any errors encountered during the migration are captured and logged for further investigation.

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/MoraQs/data_migration.git
   cd data_migration
   ```

2. **Install Dependencies**: Ensure you have the necessary dependencies by running:

3. **Setup Environmental Variables**: The project uses environment variables for database connections. Create a `.env` file and define the following variables:

```bash
SOURCE_HOST=your_host
SOURCE_USER=your_user
SOURCE_PASSWORD=your_password
SOURCE_DB=your_database

STAGING_HOST=your_host
STAGING_USER=your_user
STAGING_PASSWORD=your_password
STAGING_DB=your_destination_database

DESTINATION_HOST=your_host
DESTINATION_USER=your_user
DESTINATION_PASSWORD=your_password
DESTINATION_DB=your_destination_database
```

## Usage

1. **Run the Full Load Migration**: The initial data migration (full load) can be performed using the following command:

```bash
python full_load_tables.py
```

2. **Run the Incremental Load Migration**: For subsequent migrations (incremental), use the following command:

```bash
python incremental_load_tables.py
```

3. **Run the Transformation and Load Process**: After the data is extracted and UUIDs are generated, transform and load the data using:

```bash
python main.py
```

## Data Flow

1. **Source Table**: `efz_customer`

- Contains the customer data to be migrated.

Example columns:

- `customer_code`
- `customer_name`
- `customer_type`
- `email`
- `phone`

2. **Destination Tables**:

- `customer`:

    -  This table stores basic customer information like customerIds, types, and statuses.

- `customer_profile`:

    -  This table stores detailed customer profile data like contact information, addresses, and other business details. It also contains a `customerProfileData` column, which is a serialized `jsonb` field combining various KYC-related information.

## Notes

- **Column Mappings**: All source columns are mapped to their corresponding destination fields as defined in the mapping document.
- **UUID Handling**: Customer IDs are transformed into UUID format during the migration.
- **Data Integrity**: The migration process ensures data integrity by checking for missing values and applying default values as necessary.
- `jsonb` **Transformation**: The `customer_profile` table’s `customerProfileData` column is populated with a serialized `jsonb` structure, consolidating KYC data for more efficient storage and querying.
