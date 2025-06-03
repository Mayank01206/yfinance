# Loop through each company in the DataFrame
import os
import time
import pandas as pd
import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

# Chrome options
options = Options()
options.add_argument("--start-maximized")
options.add_argument("--headless")  # Comment this line to see browser interactions

# Initialize a list to hold log data
log_data = []

def log_message(symbol, file_name, url, status, error_line=None):
    log_data.append({
        "Symbol": symbol,
        "File Name": file_name,
        "URL": url,
        "Status": status,
        "Error Line": error_line
    })

def XML_extraction(security_code,symbol,start_period,end_period,save_folder):
    Top_URL = "https://www.bseindia.com/corporates/Comp_Resultsnew.aspx"

    for attempt in range(3):
        try:
            driver = webdriver.Chrome(option = options)
            driver.get(Top_URL)
            print(f"Processing: {symbol}")

            Security_Search = WebDriverWait(driver , 10).until(
                EC.visibility_of_element_located((By.ID, "ContentPlaceHolder1_SmartSearch_smartSearch"))
            )

            Security_Search.clear()
            Security_Search.send_keys(security_code)

            li_element = WebDriverWait(driver,10).until(
                EC.presence_of_element_located(By.XPATH, f"//li[contains(@onclick, \"'{security_code}'\")]")

            )
            li_element.click()

            dropdown = WebDriverWait(driver,10).until(
                EC.presence_of_element_located((By.ID,"ContentPlaceHolder1_broadcastdd"))
            )
            select = Select(dropdown)
            select.select_by_value("7")

            Submit_button = driver.find_element(By.ID, "ContentPlaceHolder1_btnSubmit")
            Submit_button.click()

            rows = WebDriverWait(driver,10).until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, f"//td[contains(text(), '{security_code}')]/following-sibling::td[3]")
                )
            )
            download_links = driver.find_elements(
                By.XPATH, f"//td[contains(text(), '{security_code}')]/following-sibling::td[6]//a"
            )

            # Flags to control range-based downloading
            found_start = False
            for i in range(len(rows)):
                period_text = ""
                try:
                    period_text == rows[i].text
                    link = download_links[i]
                except Exception as e :
                    error_msg = traceback.format_exc()
                    log_message(symbol,f"{symbol}_{period_text}", current_url,"File not saved", error_msg)

                # Start downloading when Start Period is found
                if period_text == start_period:
                    found_start = True

                if found_start:
                    link.click()
                    driver.switch_to.window(driver.window_handles[-1])
                    current_url = driver.current_url
                    
                    try:
                        time.sleep(2)
                        page_content = driver.page_source

                        if '<ix:header>' in page_content:
                            custom_file_name = f"{symbol}_{period_text}.html"
                            custom_file_path = os.path.join(save_folder, custom_file_name)
                            with open (custom_file_path, 'w', encoding = 'utf-8') as file:
                                file.write(page_content)

                        else:   
                            xml_div = WebDriverWait(driver,10).until(
                                EC.presence_of_element_located((By.ID , 'webkit-xml-viewer-source-xml'))
                            )
                            xml_content = xml_div.get_attribute('innerHTML')
                            custom_file_name = f"{symbol}_{period_text}.xml"
                            custom_file_path = os.path.join(save_folder, custom_file_name)
                            with open (custom_file_path, 'w', encoding = 'utf-8') as file:
                                file.write(xml_content)

                        print(f"File saved: {custom_file_name}")
                        log_message(symbol, custom_file_name, current_url, "Success")
                    
                    except Exception as e:
                        error_msg = traceback.format_exc()
                        log_message(symbol,f"{symbol}_{period_text}",current_url, "File not saved", error_msg)
                        print(f"Error saving file:{e}")

                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                
                    if period_text == end_period:
                        break
            driver.quit()
            return

        except Exception as e:
            error_msg = traceback.format_exc()
            print(f"Error occured for{symbol}, attempt{attempt +1}: {e}")

            if attempt == 2:
                log_message(symbol, "N/A", Top_URL,"Extraction Failed",error_msg)
        finally:
            try:
                driver.quit()
            except:
                pass

# Updated file path
file_path = r"D:\FinancialStatementAnalysis\03input\Taxomy_LOS_For_Period 1_6.xlsx"  # Update to new file

# Read the updated Excel file
df = pd.read_excel(file_path)

# Normalize column names to remove leading/trailing spaces
df.columns = df.columns.str.strip()

# Debugging: Print column names and first rows of data
print("Columns in the Excel file:", df.columns)
print("First few rows of the DataFrame:")
print(df.head())

# Ensure required columns exist
if 'Start Period' not in df.columns or 'End Period' not in df.columns:
    raise KeyError("The required columns 'Start Period' or 'End Period' are missing in the Excel file.")

# Base path for saving XML files
base_path = r"D:\FinancialStatementAnalysis\01ETL\extracted"

# Loop through each company in the DataFrame
for index, row in df.iterrows():
    sr_no = row['Sr No'] # Chenged from Serial_Number to Sr_No 
    symbol = row['Symbol']  # Changed from company_name to symbol
    security_code = row['Security Code'] 
    # Adjust for swapped Start and End Period
    start_period = row['End Period']  # 'End Period' now contains Start Period data
    end_period = row['Start Period']  # 'Start Period' now contains End Period data

    folder_name = f"{sr_no}_{symbol}"  # Use symbol instead of company name
    Save_Folder = os.path.join(base_path, folder_name)
    os.makedirs(Save_Folder, exist_ok=True)

    try:
        XML_extraction(security_code, symbol, start_period, end_period, Save_Folder)
    except Exception as e:
        error_msg = traceback.format_exc()
        log_message(symbol, "N/A", "N/A", "Extraction Failed", error_msg)

# Save the log data to an Excel file
base_log_path = r"D:\FinancialStatementAnalysis\04log"
os.makedirs(base_log_path, exist_ok=True)

log_file_name = "log_results_for_period_1_to_6.xlsx"
log_df = pd.DataFrame(log_data, columns=["Symbol", "File Name", "URL", "Status", "Error Line"])
log_df.to_excel(os.path.join(base_log_path, log_file_name), index=False)

print("Process complete")