
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
options.add_argument("--headless")  # Comment this line if you want to see browser actions

# Initialize a list to hold log data
log_data = []

def log_message(stock_name, file_name, url, status, error_line=None):
    log_data.append({
        "Stock Name": stock_name,
        "File Name": file_name,
        "URL": url,
        "Status": status,
        "Error Line": error_line
    })

def XML_extraction_with_retry(sr_no, row_number, security_code, stock_name, save_folder, max_retries=5):
    retry_count = 0
    success = False

    while retry_count < max_retries and not success:
        retry_count += 1
        print(f"Attempt {retry_count} for {stock_name}")
        success = XML_extraction(sr_no, row_number, security_code, stock_name, save_folder)

        if not success:
            wait_time = 2 ** retry_count
            print(f"Retry {retry_count} for {stock_name} failed. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    if not success:
        print(f"All {max_retries} attempts failed for {stock_name}. Moving to the next company.")

def XML_extraction(sr_no, row_number, security_code, stock_name, save_folder):
    Top_URL = "https://www.bseindia.com/corporates/Comp_Resultsnew.aspx"
    driver = webdriver.Chrome(options=options)
    driver.get(Top_URL)

    try:
        Security_Search = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, "ContentPlaceHolder1_SmartSearch_smartSearch"))
        )
        Security_Search.clear()
        Security_Search.send_keys(security_code)

        li_element = driver.find_element(By.XPATH, f"//li[contains(@onclick, \"'{security_code}'\")]")
        li_element.click()

        dropdown = driver.find_element(By.ID, "ContentPlaceHolder1_broadcastdd")
        select = Select(dropdown)
        select.select_by_value("7")

        Submit_button = driver.find_element(By.ID, "ContentPlaceHolder1_btnSubmit")
        Submit_button.click()

        rows = driver.find_elements(By.XPATH, f"//td[text()='{security_code}']/following-sibling::td[5]//a")
        File_Name_rows = driver.find_elements(By.XPATH, f"//td[text()='{security_code}']/following-sibling::td[3]//a")
        time.sleep(1)

        success_count = 0

        for i in range(len(rows)):
            link = rows[i]
            File_Name = File_Name_rows[i].text
            print(File_Name)
            time.sleep(1)

            main_window = driver.current_window_handle
            driver.execute_script("arguments[0].click();", link)

            WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > 1)
            driver.switch_to.window(driver.window_handles[-1])

            time.sleep(2)
            page_content = driver.page_source

            try:
                if '<ix:header>' in page_content:
                    custom_file_name = f"{stock_name}_{File_Name}.html"
                    custom_file_path = os.path.join(save_folder, custom_file_name)
                    with open(custom_file_path, 'w', encoding='utf-8') as file:
                        file.write(page_content)
                else:
                    xml_div = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.ID, 'webkit-xml-viewer-source-xml'))
                    )
                    xml_content = xml_div.get_attribute('innerHTML')
                    custom_file_name = f"{stock_name}_{File_Name}.xml"
                    custom_file_path = os.path.join(save_folder, custom_file_name)
                    with open(custom_file_path, 'w', encoding='utf-8') as file:
                        file.write(xml_content)

                log_message(stock_name, File_Name, driver.current_url, "Success")
                success_count += 1

            except Exception as e:
                tb_str = traceback.format_exc()
                error_line = 'Unknown'
                for line in tb_str.splitlines():
                    if 'File' in line and ', line ' in line:
                        error_line = line.strip()
                        break
                log_message(stock_name, File_Name, driver.current_url, "File not saved", error_line)
                print(f"Error saving file for {stock_name} - {File_Name}: {str(e)}")

            driver.close()
            driver.switch_to.window(main_window)
            time.sleep(1)

        return success_count > 0

    except Exception as e:
        tb_str = traceback.format_exc()
        error_line = 'Unknown'
        for line in tb_str.splitlines():
            if 'File' in line and ', line ' in line:
                error_line = line.strip()
                break
        log_message(stock_name, "N/A", Top_URL, "Extraction Failed", error_line)
        print(f"Error occurred during XML extraction for {stock_name}: {str(e)}")
        return False

    finally:
        driver.quit()

# Path to your input Excel file
Sample_List = r"D:\FinancialStatementAnalysis\03input\Samples500_v2.xlsx"
df = pd.read_excel(Sample_List, sheet_name='Sheet1')

start_row = int(input("Enter the start row number (e.g., 10): "))
end_row = int(input("Enter the end row number (e.g., 20): "))

if start_row < 1 or end_row > len(df):
    print(f"Invalid range. Please enter a range between 1 and {len(df)}")
else:
    df_range = df.iloc[start_row-1:end_row]
    base_path = r"D:\FinancialStatementAnalysis\test"

    for row_number, (index, row) in enumerate(df_range.iterrows(), start=start_row):
        sr_no = str(row['Sr. No.'])
        security_code = str(row['Security Code'])
        stock_name = str(row['Symbol'])

        folder_name = f"{sr_no}_{stock_name}"
        Save_Folder = os.path.join(base_path, folder_name)
        os.makedirs(Save_Folder, exist_ok=True)

        XML_extraction_with_retry(sr_no, row_number, security_code, stock_name, Save_Folder)

    base_log_path = r"D:\FinancialStatementAnalysis\test"
    os.makedirs(base_log_path, exist_ok=True)
    log_file_name = f"log_rows_{start_row}_to_{end_row}.xlsx"
    log_df = pd.DataFrame(log_data, columns=["Stock Name", "File Name", "URL", "Status", "Error Line"])
    log_df.to_excel(os.path.join(base_log_path, log_file_name), index=False)
    print("Process complete")
