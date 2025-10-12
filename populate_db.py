import pandas as pd
import sqlite3

# Define file paths
DB_PATH = 'walmart.db'
SPREADSHEET_0_PATH = 'data/spreadsheet_0.csv'
SPREADSHEET_1_PATH = 'data/spreadsheet_1.csv'
SPREADSHEET_2_PATH = 'data/spreadsheet_2.csv'

def populate_database():
    """
    Reads data from CSV files, processes it, and populates the SQLite database.
    """
    try:
        # Establish a connection to the SQLite database
        conn = sqlite3.connect(DB_PATH)
        print("Successfully connected to the database.")

        # --- Part 1: Process and insert data from spreadsheet_0.csv ---
        print("Processing spreadsheet_0...")
        df0 = pd.read_csv(SPREADSHEET_0_PATH)
        # Directly insert this data into a table. If the table exists, replace it.
        # This is a simple, self-contained dataset.
        df0.to_sql('shipping_data_0', conn, if_exists='replace', index=False)
        print("Data from spreadsheet_0 inserted successfully.")

        # --- Part 2: Process and combine data from spreadsheets 1 and 2 ---
        print("Processing spreadsheet_1 and spreadsheet_2...")
        df1 = pd.read_csv(SPREADSHEET_1_PATH)
        df2 = pd.read_csv(SPREADSHEET_2_PATH)

        # Merge the two dataframes on 'shipment_id' to combine product data with location data
        merged_df = pd.merge(df1, df2, on='shipment_id')
        print("Spreadsheets 1 and 2 merged successfully.")

        # --- Part 3: Insert combined data into normalized tables ---

        # 1. Populate the 'Shipment' table
        # Create a DataFrame with unique shipments to avoid duplicates
        shipments_df = merged_df[['shipment_id', 'origin', 'destination', 'shipment_date']].drop_duplicates()
        
        # Rename columns to match the database schema
        shipments_df.rename(columns={
            'origin': 'origin_location',
            'destination': 'destination_location'
        }, inplace=True)
        
        # Insert data into the Shipment table
        shipments_df.to_sql('Shipment', conn, if_exists='append', index=False)
        print("Unique shipments inserted into 'Shipment' table.")

        # 2. Populate the 'ShipmentProduct' junction table
        # This table links products to shipments with a specific quantity
        shipment_products_df = merged_df[['shipment_id', 'product', 'quantity']]
        
        # Insert data into the ShipmentProduct table
        shipment_products_df.to_sql('ShipmentProduct', conn, if_exists='append', index=False)
        print("Product data inserted into 'ShipmentProduct' table.")

        print("\nDatabase has been successfully populated!")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except FileNotFoundError as e:
        print(f"File not found. Make sure the script is in the root directory of the repo. Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Ensure the database connection is closed
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    populate_database()
