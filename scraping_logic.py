import os
import yaml
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor


import pandas as pd
import pyarrow
from pandas import DataFrame
from logger_file import logger

def save_dataframepqt_pd(df: DataFrame, path: str):
    df.to_parquet(f"{path}.parquet", index=False)

def modified_split(cik_name: str, date: str, url, contents, max_retries=3):
    retries = 0
    page_splitted = []
    pages_with_tables = []
    page_number = 0
    matching_elements = []
    while retries < max_retries:
        try:
            soup = BeautifulSoup(contents, 'html.parser')
            soupbody = soup.body
            headers = soupbody.find_all('ix:header')
            for header in headers:
                header.extract()
            pattern = re.compile(r'page[-_ ]?break|break[-_ ]?page|break[-_ ]?before|page[-_ ]?break[-_ ]?before|page[-_ ]?break[-_ ]?after', re.IGNORECASE)
        
            for tag in soupbody.find_all(True, recursive=True): 
                if 'style' in tag.attrs and pattern.search(tag['style']):
                    matching_elements.append(tag)



            if not matching_elements:
                pages_with_tables.append({
                    "cik_name": cik_name,
                    "reporting_date": date,
                    "url": url,
                    "page_number": page_number,
                    "page_content": str(soupbody)
                })
                return pages_with_tables
            
            elif matching_elements:
                current_position = 0
                soup_contents = str(soupbody)
                matching_str = [str(element) for element in matching_elements]
                while current_position < len(soup_contents):
                    next_occurrences = [(soup_contents[current_position:].find(s), s) for s in matching_str if s in soup_contents[current_position:]]
                    if not next_occurrences:
                        break
                    next_occurrence, next_string = min((occurrence, string) for occurrence, string in next_occurrences if occurrence != -1)
                    page_splitted.append(soup_contents[current_position:current_position + next_occurrence])
                    if next_occurrence == -1:
                        break
                    current_position += next_occurrence + len(next_string)

                if current_position < len(soup_contents):
                    page_splitted.append(soup_contents[current_position:])

            for page_content in page_splitted:
                page_number += 1
                soup = BeautifulSoup(page_content, "html.parser")
                table_tags = soup.find_all("table")

                if table_tags:
                    for tag in soup.find_all():
                        if "style" in tag.attrs:
                            del tag.attrs["style"]
                    table_text_lengths = [len(tag.get_text(strip=True)) for tag in table_tags]

                    if any(length > 35 for length in table_text_lengths):
                        page_content = str(soup)

                    pages_with_tables.append({
                        "cik_name": cik_name,
                        "reporting_date": date,
                        "url": url,
                        "page_number": page_number,
                        "page_content": page_content
                    })
            return pages_with_tables
            
        except Exception as e:
            logger.info(f"An error occured:", e)
        retries += 1
        time.sleep(5)
    return None

if __name__ == "__main__":
    logger.info("Cleaning contents Started")
    folder_path = "data/2023"
    filenames = sorted(os.listdir(folder_path))
    for filename in filenames:
        if filename.endswith(".parquet"):
            logger.info(f"Processing file: {filename}")
            
            input_file_path = os.path.join(folder_path, filename)
            dfb0 = pd.read_parquet(input_file_path)
            
            all_table_df = []
            
            dfb0c = dfb0[dfb0["contents"] != "No Soup! Got Value other than 200"]
            
            dfb0f = dfb0c.drop_duplicates(subset=['url'])
            
            form_description_links = dfb0f.to_dict(orient="records")
            
            for form_description_link in form_description_links:
                cik_name = form_description_link["cik_name"]
                date = form_description_link["reporting_date"]
                url = form_description_link["url"]
                contents = form_description_link["contents"]
                
                tables = modified_split(cik_name, date, url, contents)
                logger.info(f"Cleaned the document from {url} of the CIK name {cik_name}!!")
                
                table_df = pd.DataFrame(tables)
                all_table_df.append(table_df)
                logger.info(f"appended the document to the pool!!")
            
            final_table_df = pd.concat(all_table_df, ignore_index=True)
            
            input_file_name = os.path.splitext(os.path.basename(input_file_path))[0]
            
            output_file_path = f"data/output/table_contents/2023/{input_file_name}.parquet"
            save_dataframepqt_pd(final_table_df, output_file_path)
            logger.info(f"{input_file_name} Saved to {output_file_path}!!")

