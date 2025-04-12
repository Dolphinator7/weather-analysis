import os
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize BigQuery Client
service_account_key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = bigquery.Client.from_service_account_json(service_account_key)

print("✅ BigQuery client successfully initialized!")

def preprocess_column_names(df):
    """
    Preprocess column names to conform to BigQuery's schema rules:
    - Replace invalid characters with underscores.
    - Ensure names are alphanumeric and start with a letter or underscore.
    """
    df.columns = [
        col.strip().replace(" ", "_").replace("(", "").replace(")", "").replace("%", "_").replace("-", "_").replace("°", "_C")
        for col in df.columns
    ]
    print(f"✅ Updated column names: {df.columns}")
    return df

def upload_to_bigquery(csv_file, dataset_id, table_name):
    # Correct dataset ID (replace hyphens with underscores) - Optional depending on your actual dataset_id
    dataset_id = dataset_id.replace("-", "_")

    # Define the full table reference
    table_ref = f"{client.project}.{dataset_id}.{table_name}"

    # Load the CSV file into a Pandas DataFrame
    df = pd.read_csv(csv_file)

    # Preprocess column names to BigQuery-compliant format
    df = preprocess_column_names(df)

    # Define dynamic schema based on DataFrame columns
    schema = []
    for column_name, column_dtype in zip(df.columns, df.dtypes):
        if column_dtype == 'int64':
            field_type = 'INTEGER'
        elif column_dtype == 'float64':
            field_type = 'FLOAT'
        else:
            field_type = 'STRING'
        schema.append(bigquery.SchemaField(column_name, field_type))

    # Check if the dataset exists, create if it doesn't
    try:
        client.get_dataset(f"{client.project}.{dataset_id}")
        print(f"✅ Dataset {dataset_id} already exists.")
    except NotFound:
        print(f"❌ Dataset {dataset_id} not found. Creating it...")
        dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
        dataset.location = "US"  # Specify location
        client.create_dataset(dataset)
        print(f"✅ Dataset {dataset_id} created.")

    # Check if the table exists, create if it doesn't
    try:
        client.get_table(table_ref)
        print(f"✅ Table {table_ref} already exists.")
    except NotFound:
        print(f"❌ Table {table_ref} not found. Creating it...")
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"✅ Table {table_ref} created.")

    # Upload the DataFrame to BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Overwrite table on each run
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the upload to complete
    print(f"✅ Data successfully uploaded to BigQuery table: {table_ref}")


if __name__ == "__main__":
    # Example Usage
    csv_file = "weather_data.csv"  # Your CSV file path
    dataset_id = "rock-bonus-452311-h8"  # Your original BigQuery dataset ID
    table_name = "weather_table"  # Desired table name

    upload_to_bigquery(csv_file, dataset_id, table_name)
