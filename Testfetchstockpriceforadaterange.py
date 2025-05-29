import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

# Database connection configuration
DATABASE_URI = 'postgresql://postgres:Sup%4072164@localhost:5432/Taxanomy'

# Flag to control whether to save Excel file
SAVE_TO_EXCEL = True  

# Function to validate and add missing columns
def validate_columns(final_data, required_columns):
    missing_columns = [col for col in required_columns if col not in final_data.columns]
    if missing_columns:
        print(f"Adding missing columns: {missing_columns}")
        for col in missing_columns:
            final_data[col] = None  
    return final_data

def fetch_stock_data(symbol, start_date, end_date):
    try:
        # Add '.NS' suffix for NSE-listed stocks, you can customize it further
        symbol_with_suffix = symbol + '.NS'
        # Fetch historical data from Yahoo Finance using yf.download() (direct approach)

        data = yf.download(symbol_with_suffix, start=start_date, end=end_date)
        if isinstance(data.columns,pd.MultiIndex):
            data.columns = [col[0] for col in data.columns]

        # Check if data is not empty
        if not data.empty:
            # Reset index and rename the 'Date' column to match your expected structure
            data.reset_index(inplace=True)
            data.rename(columns={"Date": "Date", "Open": "Open", "High": "High", "Low": "Low", "Close": "Close", "Volume": "Volume", "Dividends": "Dividends", "Stock Splits": "Stock Split"}, inplace=True)

            data['Symbol'] = symbol  

            return data
        else:
            print(f"No data found for symbol {symbol_with_suffix} between {start_date} and {end_date}")
            return pd.DataFrame()

    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return pd.DataFrame()

# Function to save the final data to the output table in PostgreSQL, checking for duplicates before appending
def check_and_append_data(final_data, table_name, engine):
    try:
        final_data.reset_index(drop = True, inplace = True)
        
        # Load existing data from the database to check for duplicates
        existing_data = pd.read_sql_table(table_name, engine)
        # Assuming 'Date' and 'Symbol' uniquely identify each row
        # Merge the new data with existing data on 'Date' and 'Symbol' to find the new rows
        existing_data.reset_index(drop= True, inplace=True)
        
        merged_data = pd.merge(final_data, existing_data[['Date', 'Symbol']], on=['Date', 'Symbol'], how='left', indicator=True)

       
        # Filter rows where indicator is 'left_only' (these rows are not in existing_data)
        new_data = merged_data[merged_data['_merge'] == 'left_only'].drop(columns=['_merge'])

        if not new_data.empty:
            print(f"Appending {len(new_data)} new rows to the database.")
            new_data.to_sql(table_name, engine, index=False, if_exists='append')
        else:
            print("No new data to append.")

        # Optionally save to Excel
        if SAVE_TO_EXCEL:
            excel_file_path = r"output.xlsx"  # You can modify this path as needed
            final_data.to_excel(excel_file_path, index=False)
            print(f"Data successfully saved to Excel file '{excel_file_path}'.")

    except Exception as e:
        print(f"Error checking and appending data to PostgreSQL: {e}")

# Function to load symbols, company codes, and date ranges from an Excel file into a DataFrame
def load_symbols(file_path):
    try:
        symbols_df = pd.read_excel(file_path)
        if 'Symbol' not in symbols_df.columns or 'Security Code' not in symbols_df.columns or 'Start Date' not in symbols_df.columns or 'End Date' not in symbols_df.columns:
            print(f"Error: Missing required columns ('Symbol', 'Security Code', 'Start Date', 'End Date') in {file_path}.")
            return pd.DataFrame()

        # Rename 'Security Code' to 'Company Code'
        symbols_df.rename(columns={'Security Code': 'Company Code'}, inplace=True)

        # Convert the 'Start Date' and 'End Date' to datetime format
        symbols_df['Start Date'] = pd.to_datetime(symbols_df['Start Date'], format='%d/%m/%Y')
        symbols_df['End Date'] = pd.to_datetime(symbols_df['End Date'], format='%d/%m/%Y')

        return symbols_df[['Company Code', 'Symbol', 'Start Date', 'End Date']]  # Return only required columns
    except Exception as e:
        print(f"Error loading symbols from {file_path}: {e}")
        return pd.DataFrame()

# Main function to fetch data for multiple symbols and save to PostgreSQL
def main():
    # Path to the Excel file containing the symbols and company codes
    symbols_file_path = r'C:\Users\mayan\Downloads\OneDrive_2_5-27-2025\Sample_daterange (2) (1).xlsx'

    # Define your table name here
    table_name = 'test'  # <-- Define the table name here

    # Create SQLAlchemy engine
    engine = create_engine(DATABASE_URI)

    # Load symbols from Excel file
    symbols_df = load_symbols(symbols_file_path)
    
    if symbols_df.empty:
        print("No stock symbols found to process.")
        return

    final_data = pd.DataFrame()

    # Iterate over each row using iterrows(), which gives both index and row data
    for index, row in symbols_df.iterrows():
        company_code = row['Company Code']
        symbol = row['Symbol']
        start_date = row['Start Date']
        end_date = row['End Date']
        
        print(f"Processing symbol: {symbol} (Company Code: {company_code}) from {start_date} to {end_date}")

        # Fetch historical stock data from Yahoo Finance
        stock_data = fetch_stock_data(symbol, start_date, end_date)

        if not stock_data.empty:
            # Add Company Code to the fetched data
            stock_data['Company Code'] = company_code

            # Concatenate the data into the final DataFrame
            final_data = pd.concat([final_data, stock_data], ignore_index=True)

    if not final_data.empty:
        # Check for duplicates and append the final data to the PostgreSQL table
        check_and_append_data(final_data, table_name, engine)  # <-- Use the defined table name here
        print("Data processing completed successfully!")
    else:
        print("No data was processed.")

if __name__ == "__main__":
    main()