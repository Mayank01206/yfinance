## Code for date range with option to save the excel file or not 
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine

# Database connection configuration
DATABASE_URI = 'postgresql://postgres:Sup%4072164@localhost:5432/Taxanomy'
engine = create_engine(DATABASE_URI)

# Flag to control whether to save Excel file
SAVE_TO_EXCEL = True  # Change this to False if you don't want to save the Excel file

# Function to validate and add missing columns
def validate_columns(final_data, required_columns):
    missing_columns = [col for col in required_columns if col not in final_data.columns]
    if missing_columns:
        print(f"Adding missing columns: {missing_columns}")
        for col in missing_columns:
            final_data[col] = None  # Add missing columns with default None values
    return final_data

# Function to append data to PostgreSQL database
def save_to_postgres(final_data, table_name):
    try:
        # Validate columns before saving
        required_columns = ["Company Code", "Symbol", "Date", "Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Split"]
        final_data = validate_columns(final_data, required_columns)

        # Reorder columns to match your required final output
        final_data = final_data[['Company Code', 'Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Split']]

        # Save data to PostgreSQL using SQLAlchemy
        print(f"Attempting to save data to table '{table_name}'...")
        final_data.to_sql(table_name, engine, index=False, if_exists='append', method='multi')
        print(f"Data successfully saved to table '{table_name}'.")

        # Optionally save the data to an Excel file
        if SAVE_TO_EXCEL:
            excel_file_path = "output_data.xlsx"  # You can change this path as needed
            final_data.to_excel(excel_file_path, index=False)
            print(f"Data successfully saved to Excel file '{excel_file_path}'.")

    except Exception as e:
        print(f"Error saving data to PostgreSQL: {e}")

# Function to fetch historical data using yfinance
def fetch_historical_data(symbol, start_date, end_date, company_details):
    try:
        full_symbol = f"{symbol}.NS"  # Assuming NSE (National Stock Exchange of India) suffix
        ticker = yf.Ticker(full_symbol)
        
        # Fetch historical data for the specific date range
        data = ticker.history(start=start_date, end=end_date)
        
        if not data.empty:
            data.index = data.index.tz_localize(None)  # Removing timezone info
            data.reset_index(inplace=True)
            data.rename(columns={"Date": "Date"}, inplace=True) 
            
            company_info = company_details[company_details['Symbol'] == symbol]
            
            if not company_info.empty:
                security_code = company_info.iloc[0]['Security Code']
                
                data.insert(0, 'Company Code', security_code)
                data.insert(1, 'Symbol', symbol)
                
                # Save data to PostgreSQL and optionally to Excel
                save_to_postgres(data, 'test')  # Assuming 'test' is your target table in PostgreSQL
                
                return data
            else:
                print(f"No company information found for {full_symbol}")
                return pd.DataFrame()
        else:
            print(f"No data available for {full_symbol} within the specified date range ({start_date} to {end_date})")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return pd.DataFrame()

# Main script execution
if __name__ == "__main__":
    combined_file_path = r"D:\\yfinance\\historical_dateeeee.xlsx"  # Path for the output Excel file
    file_path = r"D:\yfinance\Sample_daterange (3).xlsx"  # Path for the input Excel file with symbols and date ranges
    
    company_details = pd.read_excel(file_path, parse_dates=['Start Date', 'End Date'])  # Reading the input file
    
    if company_details.empty:
        print("No company details found. Exiting...")
    else:
        all_data = []
        
        for index, row in company_details.iterrows():
            SYMBOL = row['Symbol']
            START_DATE = row['Start Date']
            END_DATE = row['End Date']
            
            print(f"Processing {SYMBOL} from {START_DATE} to {END_DATE}")
            
            company_data = fetch_historical_data(SYMBOL, START_DATE, END_DATE, company_details)
            
            if not company_data.empty:
                all_data.append(company_data)
            else:
                print(f"No data found for {SYMBOL}")
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            print(f"Data fetched for {len(all_data)} symbols.")
        else:
            combined_data = pd.DataFrame({'Message': ['No valid stock data found.']})
            print("No valid data to save. Placeholder sheet will be created.")
        
        print(f"Final combined data shape: {combined_data.shape}")
        
        # Removing 'Start Date' and 'End Date' columns if they exist (although these columns aren't part of combined_data already)
        if 'Start Date' in combined_data.columns:
            combined_data = combined_data.drop(columns=['Start Date'])
        if 'End Date' in combined_data.columns:
            combined_data = combined_data.drop(columns=['End Date'])



            
