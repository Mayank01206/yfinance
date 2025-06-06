
from lxml import etree, html
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path
import shutil
import traceback
from datetime import datetime

log_columns = ['Stock', 'Period', 'Status', 'Message', 'Error Line']
log_df = pd.DataFrame(columns=log_columns)

def load_xml_lxml(file_path):
    try:
        tree = etree.parse(file_path)
        root = tree.getroot()
        print(f"Root Element: {root.tag}")
        return root
    except etree.XMLSyntaxError as e:
        print(f"Error parsing XML file: {e}")
        raise
    except FileNotFoundError as e:
        print(f"File not found: {e}")
        raise

def load_html_lxml(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        root = html.fromstring(content)
        return root
    except Exception as e:
        print(f"Error parsing HTML file: {e}")
        raise

def extract_scrip_code_from_context(root):
    scrip_code_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "ScripCode"), None)
    return scrip_code_row.text.strip() if scrip_code_row is not None else 'Unknown'

def extract_financial_year_from_context(root):
    financial_year_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "DateOfEndOfFinancialYear"), None)
    if financial_year_row is not None:
        date_value = financial_year_row.text.strip()
        year = datetime.strptime(date_value, "%Y-%m-%d").year
        return f"{year}"
    return 'Unknown'

def extract_quarter_from_context(root):
    start_date_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "DateOfStartOfReportingPeriod"), None)
    end_date_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "DateOfEndOfReportingPeriod"), None)
    if start_date_row is not None and end_date_row is not None:
        start_date_value = start_date_row.text.strip()
        end_date_value = end_date_row.text.strip()
        start_month = datetime.strptime(start_date_value, "%Y-%m-%d").month
        end_month = datetime.strptime(end_date_value, "%Y-%m-%d").month

        def get_quarter(month):
            if 1 <= month <= 3:
                return '04'
            elif 4 <= month <= 6:
                return '01'
            elif 7 <= month <= 9:
                return '02'
            elif 10 <= month <= 12:
                return '03'
            return 'Unknown'

        start_quarter = get_quarter(start_month)
        end_quarter = get_quarter(end_month)

        if start_quarter != end_quarter:
            raise ValueError(f"Mismatched quarters: Start {start_quarter}, End {end_quarter}")

        return start_quarter

    raise ValueError("Missing required period start or end date in XML")

def extract_all_data(root):
    all_data = []
    scrip_code = extract_scrip_code_from_context(root)
    financial_year = extract_financial_year_from_context(root)
    quarter = extract_quarter_from_context(root)

    period_start_date_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "DateOfStartOfReportingPeriod"), None)
    period_end_date_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "DateOfEndOfReportingPeriod"), None)
    period_start_date = period_start_date_row.text.strip() if period_start_date_row is not None else 'Unknown'
    period_end_date = period_end_date_row.text.strip() if period_end_date_row is not None else 'Unknown'

    nature_of_report = None
    for elem in root.iter():
        if etree.QName(elem).localname == "NatureOfReportStandaloneConsolidated":
            nature_of_report = elem.text.strip() if elem.text else 'Unknown'
            break

    for elem in root.iter():
        tag = etree.QName(elem).localname
        value = elem.text.strip() if elem.text else None
        context_ref = elem.get('contextRef', 'OneD')
        decimals = elem.get('decimals', '')
        all_data.append({
            'Company Code': scrip_code,
            'Financial Year': financial_year,
            'Quarter': quarter,
            'Element Name': tag,
            'Unit': context_ref,
            'Value': value,
            'Decimal': decimals,
            'Period Start Date': period_start_date,
            'Period End Date': period_end_date,
            'Nature Of Report': nature_of_report
        })
    return all_data

def convert_to_dataframe(data):
    return pd.DataFrame(data)

def process_xml_files(xml_download_dir, excel_save_dir, processed_folder, stock_symbol):
    global log_df
    for root_dir, _, files in os.walk(xml_download_dir):
        for file_name in files:
            if file_name.endswith(('.xml', '.htm', '.html')):
                file_path = os.path.join(root_dir, file_name)
                print(f"Processing file: {file_path}")
                try:
                    if file_name.endswith(".xml"):
                        root = load_xml_lxml(file_path)
                    else:
                        root = load_html_lxml(file_path)

                    all_data = extract_all_data(root)
                    all_data_df = convert_to_dataframe(all_data)

                    period_start_date_row = all_data_df[(all_data_df["Element Name"] == "DateOfStartOfReportingPeriod")]
                    period_end_date_row = all_data_df[(all_data_df["Element Name"] == "DateOfEndOfReportingPeriod")]

                    period_start_date = period_start_date_row["Value"].values[0] if not period_start_date_row.empty else 'UNKNOWN_START_DATE'
                    period_end_date = period_end_date_row["Value"].values[0] if not period_end_date_row.empty else 'UNKNOWN_END_DATE'

                    reporting_period_str = f"{period_start_date}_{period_end_date}"
                    base_file_name = os.path.splitext(file_name)[0]
                    new_file_name = f"{reporting_period_str}_{base_file_name}.xlsx"

                    excel_path = os.path.join(excel_save_dir, new_file_name)
                    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                        all_data_df.to_excel(writer, sheet_name='All Data', index=False)
                    print(f"Saved Excel file: {excel_path}")

                    destination_path = os.path.join(processed_folder, file_name)
                    shutil.move(file_path, destination_path)

                    log_entry = pd.DataFrame([{
                        'Stock': str(stock_symbol),
                        'Period': str(file_name),
                        'Status': 'Success',
                        'Message': 'Processing completed successfully.',
                        'Error Line': None
                    }])
                    log_df = pd.concat([log_df, log_entry], ignore_index=True)

                except Exception as e:
                    tb_str = traceback.format_exc()
                    error_line = 'Unknown'
                    for line in tb_str.splitlines():
                        if 'File' in line and ', line ' in line:
                            error_line = line.strip()
                            break
                    log_entry = pd.DataFrame([{
                        'Stock': str(stock_symbol),
                        'Period': str(file_name),
                        'Status': 'Error',
                        'Message': str(e),
                        'Error Line': error_line
                    }])
                    log_df = pd.concat([log_df, log_entry], ignore_index=True)

def replace_year_quarter_prefix(file_name, new_prefix):
    import re
    pattern = r'^\d{4}-\d{4}_Q\d_'
    return re.sub(pattern, f"{new_prefix}_", file_name)

Input_Folder_path = Path(r"D:\webpage\xml_excel")
Output_Folder_Path = Path(r"D:\webpage\converted")
xml_folder_path = Path(r"D:\webpage\xmls_processed")
Input_File = Path(r"D:\webpage\Taxomy_LOS_For_Period 1_37.xlsx")
Log_Folder_Path = Path(r"D:\webpage\log")

os.makedirs(Log_Folder_Path, exist_ok=True)
df = pd.read_excel(Input_File, sheet_name='Sheet1')

for index, row in df.iterrows():
    serial_number = str(row['Sr No'])
    Name = str(row['Symbol'])
    folder_name = f"{serial_number}_{Name}"
    print(f"Looking for folder: {folder_name}")

    for current_folder in Input_Folder_path.iterdir():
        if folder_name == str(os.path.basename(current_folder)):
            current_folder_path = os.path.join(Input_Folder_path, current_folder)
            print(f"Processing folder: {current_folder_path}")
            xml_directory = current_folder_path
            Processed_XMLs_folder_name = folder_name + "_XMLS_Processed"
            Processed_XMLs_folder = os.path.join(xml_folder_path, Processed_XMLs_folder_name)
            Converted_Excels_folder_name = folder_name + "_Converted_Excels"
            Converted_Excels_folder = os.path.join(Output_Folder_Path, Converted_Excels_folder_name)
            os.makedirs(Processed_XMLs_folder, exist_ok=True)
            os.makedirs(Converted_Excels_folder, exist_ok=True)
            process_xml_files(xml_directory, Converted_Excels_folder, Processed_XMLs_folder, current_folder)

log_file_name = "xml_to_excel_51_to_100_.xlsx"
log_file_path = os.path.join(Log_Folder_Path, log_file_name)
log_df.to_excel(log_file_path, index=False)
print('Process complete. Log file saved to:', log_file_path)




from lxml import etree
import pandas as pd
from openpyxl.utils.dataframe import dataframe_to_rows
import os
from pathlib import Path
import shutil
import traceback
from datetime import datetime

log_columns = ['Stock', 'Period', 'Status', 'Message', 'Error Line']
log_df = pd.DataFrame(columns=log_columns)

def load_xml_lxml(file_path):
    try:
        tree = etree.parse(file_path)
        root = tree.getroot()
        return root
    except etree.XMLSyntaxError as e:
        raise
    except FileNotFoundError as e:
        raise

def load_html_lxml(file_path):
    try:
        parser = etree.XMLParser(recover=True, ns_clean=True)
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        root = etree.fromstring(content.encode('utf-8'), parser=parser)
        return root
    except Exception as e:
        raise

def extract_scrip_code_from_context(root):
    scrip_code_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "ScripCode"), None)
    return scrip_code_row.text.strip() if scrip_code_row is not None else 'Unknown'

def extract_financial_year_from_context(root):
    financial_year_row = next((elem for elem in root.iter() if etree.QName(elem).localname == "DateOfEndOfFinancialYear"), None)
    if financial_year_row is not None:
        date_value = financial_year_row.text.strip()
        year = datetime.strptime(date_value, "%Y-%m-%d").year
        return f"{year}"
    return 'Unknown'

def extract_quarter_from_context(root):
    def try_get_date(tag):
        elem = next((e for e in root.iter() if etree.QName(e).localname == tag), None)
        return elem.text.strip() if elem is not None else None

    start_date = try_get_date("DateOfStartOfReportingPeriod") or try_get_date("startDate")
    end_date = try_get_date("DateOfEndOfReportingPeriod") or try_get_date("endDate")

    if not start_date or not end_date:
        raise ValueError("Missing required period start or end date in XML")

    start_month = datetime.strptime(start_date, "%Y-%m-%d").month
    end_month = datetime.strptime(end_date, "%Y-%m-%d").month

    def get_quarter(month):
        if 1 <= month <= 3:
            return '04'
        elif 4 <= month <= 6:
            return '01'
        elif 7 <= month <= 9:
            return '02'
        elif 10 <= month <= 12:
            return '03'
        return 'Unknown'

    start_quarter = get_quarter(start_month)
    end_quarter = get_quarter(end_month)

    if start_quarter != end_quarter:
        raise ValueError(f"Mismatched quarters: Start {start_quarter}, End {end_quarter}")

    return start_quarter

def extract_all_data(root):
    all_data = []
    scrip_code = extract_scrip_code_from_context(root)
    financial_year = extract_financial_year_from_context(root)
    quarter = extract_quarter_from_context(root)

    period_start_date_row = next((elem for elem in root.iter() if etree.QName(elem).localname in ["DateOfStartOfReportingPeriod", "startDate"]), None)
    period_end_date_row = next((elem for elem in root.iter() if etree.QName(elem).localname in ["DateOfEndOfReportingPeriod", "endDate"]), None)
    period_start_date = period_start_date_row.text.strip() if period_start_date_row is not None else 'Unknown'
    period_end_date = period_end_date_row.text.strip() if period_end_date_row is not None else 'Unknown'

    nature_of_report = None
    for elem in root.iter():
        if etree.QName(elem).localname == "NatureOfReportStandaloneConsolidated":
            nature_of_report = elem.text.strip() if elem.text else 'Unknown'
            break

    for elem in root.iter():
        tag = etree.QName(elem).localname
        value = elem.text.strip() if elem.text else None
        context_ref = elem.get('contextRef', 'OneD')
        decimals = elem.get('decimals', '')
        all_data.append({
            'Company Code': scrip_code,
            'Financial Year': financial_year,
            'Quarter': quarter,
            'Element Name': tag,
            'Unit': context_ref,
            'Value': value,
            'Decimal': decimals,
            'Period Start Date': period_start_date,
            'Period End Date': period_end_date,
            'Nature Of Report': nature_of_report
        })
    return all_data

def convert_to_dataframe(data):
    return pd.DataFrame(data)

def process_xml_files(xml_download_dir, excel_save_dir, processed_folder, stock_symbol):
    global log_df
    for root_dir, _, files in os.walk(xml_download_dir):
        for file_name in files:
            if file_name.endswith(('.xml', '.htm', '.html')):
                file_path = os.path.join(root_dir, file_name)
                try:
                    if file_name.endswith(".xml"):
                        root = load_xml_lxml(file_path)
                    else:
                        root = load_html_lxml(file_path)

                    all_data = extract_all_data(root)
                    all_data_df = convert_to_dataframe(all_data)

                    period_start_date_row = all_data_df[(all_data_df["Element Name"] == "DateOfStartOfReportingPeriod") | (all_data_df["Element Name"] == "startDate")]
                    period_end_date_row = all_data_df[(all_data_df["Element Name"] == "DateOfEndOfReportingPeriod") | (all_data_df["Element Name"] == "endDate")]

                    period_start_date = period_start_date_row["Value"].values[0] if not period_start_date_row.empty else 'UNKNOWN_START_DATE'
                    period_end_date = period_end_date_row["Value"].values[0] if not period_end_date_row.empty else 'UNKNOWN_END_DATE'

                    reporting_period_str = f"{period_start_date}_{period_end_date}"
                    base_file_name = os.path.splitext(file_name)[0]
                    new_file_name = f"{reporting_period_str}_{base_file_name}.xlsx"

                    excel_path = os.path.join(excel_save_dir, new_file_name)
                    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                        all_data_df.to_excel(writer, sheet_name='All Data', index=False)

                    destination_path = os.path.join(processed_folder, file_name)
                    shutil.move(file_path, destination_path)

                    log_entry = pd.DataFrame([{
                        'Stock': str(stock_symbol),
                        'Period': str(file_name),
                        'Status': 'Success',
                        'Message': 'Processing completed successfully.',
                        'Error Line': None
                    }])
                    log_df = pd.concat([log_df, log_entry], ignore_index=True)

                except Exception as e:
                    tb_str = traceback.format_exc()
                    error_line = 'Unknown'
                    for line in tb_str.splitlines():
                        if 'File' in line and ', line ' in line:
                            error_line = line.strip()
                            break
                    log_entry = pd.DataFrame([{
                        'Stock': str(stock_symbol),
                        'Period': str(file_name),
                        'Status': 'Error',
                        'Message': str(e),
                        'Error Line': error_line
                    }])
                    log_df = pd.concat([log_df, log_entry], ignore_index=True)

if __name__ == "__main__":
    Input_Folder_path = Path(r"D:\webpage\xml_excel")
    Output_Folder_Path = Path(r"D:\webpage\converted")
    xml_folder_path = Path(r"D:\webpage\xmls_processed")
    Input_File = Path(r"D:\webpage\Taxomy_LOS_For_Period 1_37.xlsx")
    Log_Folder_Path = Path(r"D:\webpage\log")

    os.makedirs(Log_Folder_Path, exist_ok=True)

    try:
        df = pd.read_excel(Input_File, sheet_name='Sheet1')
    except FileNotFoundError:
        print(f"❌ Input file not found: {Input_File}")
        exit(1)

    for index, row in df.iterrows():
        serial_number = str(row['Sr No'])
        Name = str(row['Symbol'])
        folder_name = f"{serial_number}_{Name}"

        for current_folder in Input_Folder_path.iterdir():
            if folder_name == str(current_folder.name):
                xml_directory = os.path.join(Input_Folder_path, current_folder)
                Processed_XMLs_folder_name = folder_name + "_XMLS_Processed"
                Converted_Excels_folder_name = folder_name + "_Converted_Excels"

                Processed_XMLs_folder = os.path.join(xml_folder_path, Processed_XMLs_folder_name)
                Converted_Excels_folder = os.path.join(Output_Folder_Path, Converted_Excels_folder_name)

                os.makedirs(Processed_XMLs_folder, exist_ok=True)
                os.makedirs(Converted_Excels_folder, exist_ok=True)

                process_xml_files(xml_directory, Converted_Excels_folder, Processed_XMLs_folder, current_folder)

    log_file_name = "xml_to_excel_51_to_100_.xlsx"
    log_file_path = os.path.join(Log_Folder_Path, log_file_name)
    log_df.to_excel(log_file_path, index=False)
    print("✅ All files processed and log saved.")
                    
