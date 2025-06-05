import os
import time
import pandas as pd
import traceback
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

SAMPLE_LIST = r"D:\\FinancialStatementAnalysis\\03input\\Samples500_v2.xlsx"
SAVE_BASE_PATH = r"D:\FinancialStatementAnalysis\test1"
LOG_PATH = SAVE_BASE_PATH
MAX_RETRIES = 5

options = Options()
options.add_argument("--start-maximized")
options.add_argument("--headless")  # Comment to see browser actions

log_data = []

def log_message(stock_name, file_name, url, status, error_line=None):
    log_data.append({
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Stock Name": stock_name,
        "File Name": file_name,
        "URL": url,
        "Status": status,
        "Error Line": error_line
    })

def get_error_line():
    tb_str = traceback.format_exc()
    for line in tb_str.splitlines():
        if 'File' in line and ', line ' in line:
            return line.strip()
    return 'Unknown'

# ----------------------------- Extraction Logic -----------------------------
def XML_extraction_with_retry(driver, security_code, stock_name, save_folder):
    for retry_count in range(1, MAX_RETRIES + 1):
        print(f"Attempt {retry_count} for {stock_name}")
        if XML_extraction(driver, security_code, stock_name, save_folder):
            return True
        wait_time = 2 ** retry_count
        print(f"Retry {retry_count} for {stock_name} failed. Retrying in {wait_time} seconds...")
        time.sleep(wait_time)
    print(f"All {MAX_RETRIES} attempts failed for {stock_name}. Moving to the next company.")
    return False

def XML_extraction(driver, security_code, stock_name, save_folder):
    Top_URL = "https://www.bseindia.com/corporates/Comp_Resultsnew.aspx"
    try:
        driver.get(Top_URL)

        Security_Search = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, "ContentPlaceHolder1_SmartSearch_smartSearch"))
        )
        Security_Search.clear()
        Security_Search.send_keys(security_code)

        li_element = driver.find_element(By.XPATH, f"//li[contains(@onclick, \"'{security_code}'\")]" )
        li_element.click()

        dropdown = driver.find_element(By.ID, "ContentPlaceHolder1_broadcastdd")
        Select(dropdown).select_by_value("7")

        driver.find_element(By.ID, "ContentPlaceHolder1_btnSubmit").click()

        rows = driver.find_elements(By.XPATH, f"//td[text()='{security_code}']/following-sibling::td[5]//a")
        file_name_rows = driver.find_elements(By.XPATH, f"//td[text()='{security_code}']/following-sibling::td[3]//a")

        time.sleep(1)
        main_window = driver.current_window_handle

        for i, link in enumerate(rows):
            file_name = file_name_rows[i].text
            print(file_name)
            time.sleep(1)

            driver.execute_script("arguments[0].click();", link)
            WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > 1)
            driver.switch_to.window(driver.window_handles[-1])
            time.sleep(2)
            page_content = driver.page_source

            try:
                if '<ix:header>' in page_content:
                    final_file = os.path.join(save_folder, f"{stock_name}_{file_name}.html")
                    with open(final_file, 'w', encoding='utf-8') as file:
                        file.write(page_content)
                else:
                    xml_div = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.ID, 'webkit-xml-viewer-source-xml'))
                    )
                    xml_content = xml_div.get_attribute('innerHTML')
                    final_file = os.path.join(save_folder, f"{stock_name}_{file_name}.xml")
                    with open(final_file, 'w', encoding='utf-8') as file:
                        file.write(xml_content)

                log_message(stock_name, file_name, driver.current_url, "Success")

            except Exception as e:
                log_message(stock_name, file_name, driver.current_url, "File not saved", get_error_line())
                print(f"Error saving file for {stock_name} - {file_name}: {str(e)}")

            driver.close()
            driver.switch_to.window(main_window)
            time.sleep(1)
    
        return True
    
    except Exception as e:
        log_message(stock_name, "N/A", Top_URL, "Extraction Failed", get_error_line())
        print(f"Error during extraction for {stock_name}: {str(e)}")
        return False

# ----------------------------- Main Execution -----------------------------
def main():
    df = pd.read_excel(SAMPLE_LIST, sheet_name='Sheet1')
    start_row = int(input("Enter the start row number (e.g., 10): "))
    end_row = int(input("Enter the end row number (e.g., 20): "))

    if start_row < 1 or end_row > len(df):
        print(f"Invalid range. Please enter a range between 1 and {len(df)}")
        return

    df_range = df.iloc[start_row - 1:end_row]

    driver = webdriver.Chrome(options=options)
    try:
        for index, row in df_range.iterrows():
            security_code = str(row['Security Code'])
            stock_name = str(row['Symbol'])
            folder_name = f"{index+1}_{stock_name}"
            save_folder = os.path.join(SAVE_BASE_PATH, folder_name)
            os.makedirs(save_folder, exist_ok=True)

            XML_extraction_with_retry(driver, security_code, stock_name, save_folder)

    finally:
        driver.quit()

    log_df = pd.DataFrame(log_data)
    log_file = os.path.join(LOG_PATH, f"log_rows_{start_row}_to_{end_row}.xlsx")
    log_df.to_excel(log_file, index=False)
    print("Process complete")

if __name__ == "__main__":
    main()