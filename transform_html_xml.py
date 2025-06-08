from lxml import etree
import pandas as pd
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path
import shutil
import traceback
from datetime import datetime
from bs4 import BeautifulSoup

log_columns = ['Stock', 'Period', 'Status', 'Message', 'Error Line']
log_df = pd.DataFrame(columns=log_columns)

def XML_edit(filepath):
    print(filepath)
    print("Checking file:", filepath)
    time.sleep(2)
    with open(filepath, 'r', encoding='utf-8') as file:
        xml_content = file.read()
    root = ET.fromstring(xml_content)
    for elem in root.iter():
        if elem.tag == ET.Comment and elem.text.strip() == 'FRIndAs':
            root.remove(elem)
    tree = ET.ElementTree(root)
    tree.write(filepath)
    return filepath

def load_xml_lxml(file_path):
    try:
        tree = etree.parse(file_path)
        root = tree.getroot()
        return root
    except Exception as e:
        print(f"XML parsing error: {e}")
        raise

def load_html_bs4(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "lxml")
        return soup
    except Exception as e:
        print(f"HTML parsing error: {e}")
        raise

def extract_period_from_html_table(soup):
    def find_date(label):
        label_cell = soup.find(lambda tag: tag.name == "td" and label.lower() in tag.text.strip().lower())
        if label_cell and label_cell.find_next_sibling("td"):
            return label_cell.find_next_sibling("td").text.strip()
        return ""
    start = find_date("Date of start of reporting period")
    end = find_date("Date of end of reporting period")
    return start, end

def parse_contexts(soup):
    contexts = {}
    for ctx in soup.find_all("xbrli:context"):
        ctx_id = ctx.get("id")
        start_date = ctx.find("xbrli:startdate")
        end_date = ctx.find("xbrli:enddate")
        if start_date and end_date:
            contexts[ctx_id] = {
                "start": start_date.text,
                "end": end_date.text
            }
    return contexts

def extract_value_tags(soup):
    return soup.find_all(["ix:nonfraction", "ix:nonnumeric"])

def extract_company_info(soup):
    scrip_tag = soup.find("ix:nonnumeric", {"name": "in-capmkt:ScripCode"})
    nature_tag = soup.find("ix:nonnumeric", {"name": "in-capmkt:NatureOfReportStandaloneConsolidated"})
    scrip_code = scrip_tag.text.strip() if scrip_tag else "Unknown"
    nature = nature_tag.text.strip() if nature_tag else "Unknown"
    return scrip_code, nature

def map_quarter_from_period(start_date):
    if not start_date:
        return "Unknown"
    month = int(start_date.split("-")[1])
    quarter_map = {1: "04", 4: "01", 7: "02", 10: "03"}
    return quarter_map.get(month, "Unknown")

def convert_date_format(date_str):
    try:
        return datetime.strptime(date_str, "%d-%m-%Y").strftime("%Y-%m-%d")
    except:
        return date_str

def extract_all_data_from_html(file_path):
    soup = load_html_bs4(file_path)
    start, end = extract_period_from_html_table(soup)

    start = convert_date_format(start)
    end = convert_date_format(end)

    quarter = map_quarter_from_period(start)
    elements = extract_value_tags(soup)
    scrip_code, nature = extract_company_info(soup)

    records = []
    for tag in elements:
        name = tag.get("name")
        context = tag.get("contextref")
        value = tag.text.strip()
        decimals = tag.get("decimals", "")

        record = {
            "Company Code": scrip_code,
            "Financial Year": end.split("-")[0] if end else "",
            "Quarter": quarter,
            "Element Name": name.split(":")[-1] if name else "",
            "Unit": context,
            "Value": value,
            "Decimal": decimals,
            "Period Start Date": start,
            "Period End Date": end,
            "Nature Of Report": nature
        }
        records.append(record)

    return pd.DataFrame(records)

def extract_all_data_from_xml(root):
    def extract_common_metadata():
        scrip_code_elem = next((e for e in root.iter() if etree.QName(e).localname == "ScripCode"), None)
        scrip_code = scrip_code_elem.text.strip() if scrip_code_elem is not None else 'Unknown'

        fy_elem = next((e for e in root.iter() if etree.QName(e).localname == "DateOfEndOfFinancialYear"), None)
        financial_year = 'Unknown'
        if fy_elem is not None:
            try:
                financial_year = str(datetime.strptime(fy_elem.text.strip(), "%Y-%m-%d").year)
            except:
                pass

        start_elem = next((e for e in root.iter() if etree.QName(e).localname == "DateOfStartOfReportingPeriod"), None)
        end_elem = next((e for e in root.iter() if etree.QName(e).localname == "DateOfEndOfReportingPeriod"), None)
        start = start_elem.text.strip() if start_elem is not None else 'Unknown'
        end = end_elem.text.strip() if end_elem is not None else 'Unknown'
        quarter = map_quarter_from_period(start)

        nor_elem = next((e for e in root.iter() if etree.QName(e).localname == "NatureOfReportStandaloneConsolidated"), None)
        nor = nor_elem.text.strip() if nor_elem is not None else 'Unknown'

        return scrip_code, financial_year, start, end, quarter, nor

    scrip_code, fy, start, end, quarter, nor = extract_common_metadata()
    data = []
    for elem in root.iter():
        tag = etree.QName(elem).localname
        val = elem.text.strip() if elem.text else None
        context = elem.get('contextRef', 'OneD')
        decimals = elem.get('decimals', '')
        data.append({
            'Company Code': scrip_code,
            'Financial Year': fy,
            'Quarter': quarter,
            'Element Name': tag,
            'Unit': context,
            'Value': val,
            'Decimal': decimals,
            'Period Start Date': start,
            'Period End Date': end,
            'Nature Of Report': nor
        })
    return pd.DataFrame(data)

def process_financial_files(xml_download_dir, excel_save_dir, Processed_XMLs_folder, Stock_Symbol):
    global log_df
    for root_dir, _, files in os.walk(xml_download_dir):
        for file_name in files:
            print(f"Found file: {file_name}") 
            if file_name.endswith(('.xml', '.html', '.htm')):
                file_path = os.path.join(root_dir, file_name)
                try:
                    if file_name.endswith('.xml'):
                        revised_file_path = XML_edit(file_path)
                        root = load_xml_lxml(revised_file_path)
                        df = extract_all_data_from_xml(root)
                    else:
                        df = extract_all_data_from_html(file_path)

                    if 'Period Start Date' not in df.columns or 'Period End Date' not in df.columns:
                        raise ValueError(f"'Period Start Date' or 'Period End Date' missing in {file_name}")

                    start = str(df['Period Start Date'].iloc[0]).replace("/", "-")
                    end = str(df['Period End Date'].iloc[0]).replace("/", "-")
                    new_file_name = f"{start}_{end}_{file_name.split('.')[0]}.xlsx"

                    excel_path = os.path.join(excel_save_dir, new_file_name)
                    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False, sheet_name='All Data')

                    shutil.move(file_path, os.path.join(Processed_XMLs_folder, file_name))
                    log_df.loc[len(log_df)] = [Stock_Symbol, file_name, 'Success', 'Processed', None]

                except Exception as e:
                    tb_str = traceback.format_exc()
                    error_line = 'Unknown'
                    for line in tb_str.splitlines():
                        if 'File' in line and ', line ' in line:
                            error_line = line.strip()
                            break
                    log_df.loc[len(log_df)] = [Stock_Symbol, file_name, 'Error', str(e), error_line]

def replace_year_quarter_prefix(file_name, new_prefix):
    import re
    pattern = r'^\d{4}-\d{4}_Q\d_'
    return re.sub(pattern, f"{new_prefix}_", file_name)

Input_Folder_path = Path(r"D:\test_consolidated_xml_html\Extracted")
Output_Folder_Path = Path(r"D:\test_consolidated_xml_html\converted")
xml_folder_path = Path(r"D:\test_consolidated_xml_html\xmls_processed")
Input_File = (r"D:\test_consolidated_xml_html\Samples500_v2.xlsx")
Log_Folder_Path = Path(r"D:\test_consolidated_xml_html\log")

os.makedirs(Log_Folder_Path, exist_ok=True)
df = pd.read_excel(Input_File, sheet_name='Sheet1')

for index, row in df.iterrows():
    serial_number = str(row['Sr. No.'])
    Name = str(row['Symbol'])
    folder_name = f"{serial_number}_{Name}"

    for current_folder in Input_Folder_path.iterdir():
        if folder_name == str(os.path.basename(current_folder)):
            current_folder_path = os.path.join(Input_Folder_path, current_folder)
            xml_directory = current_folder_path
            Processed_XMLs_folder = os.path.join(xml_folder_path, folder_name + "_XMLS_Processed")
            Converted_Excels_folder = os.path.join(Output_Folder_Path, folder_name + "_Converted_Excels")
            os.makedirs(Processed_XMLs_folder, exist_ok=True)
            os.makedirs(Converted_Excels_folder, exist_ok=True)
            process_financial_files(xml_directory, Converted_Excels_folder, Processed_XMLs_folder, current_folder)

log_file_path = os.path.join(Log_Folder_Path, "xml_to_excel_51_to_100_.xlsx")
log_df.to_excel(log_file_path, index=False)
print('Process complete. Log file saved to:', log_file_path)
