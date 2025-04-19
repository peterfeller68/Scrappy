
from bs4 import BeautifulSoup
import requests
import sys
import pandas as pd
import csv 

request_headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
class Scrappy:
    def scrape_data_cms_restated_drug(url):
        try:
            # Initiate HTTP request
            response = requests.get(url, headers=request_headers)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            soup = BeautifulSoup(response.content, 'html.parser')
            # Extract data using BeautifulSoup methods (e.g., find, find_all)
            # For example, to get all the links on the page:
            # links = [a['href'] for a in soup.find_all('a', href=True)]
            links = [soup.find_all('table')]

            table = soup.find('table')

            if table is None:
                raise ValueError("No table found in the HTML content.")

            rows = []
            headers = [th.text.replace(",", "").strip() for th in table.find_all('th')]
            rows.append(headers)
        
            for tr in table.find_all('tr')[1:]:  # Skip header row
                cells = [td.text.replace(",", "").replace("\n","").strip() for td in tr.find_all('td')]
                rows.append(cells)
            return rows
    
        except requests.exceptions.RequestException as e:
                return f"Request error: {e}"
        except Exception as e:
            return f"An error occurred: {e}"
    
    def scrape_data_nys_apg_modifiers(url):
        try:
            # Initiate HTTP request
            response = requests.get(url, headers=request_headers)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            soup = BeautifulSoup(response.content, 'html.parser')
            # Extract data using BeautifulSoup methods (e.g., find, find_all)
            # For example, to get all the links on the page:
            # links = [a['href'] for a in soup.find_all('a', href=True)]
            links = [soup.find_all('table')]

            table = soup.find('table')

            if table is None:
                raise ValueError("No table found in the HTML content.")

            rows = []
            headers = [th.text.replace(",", "").strip() for th in table.find_all('th')]
            rows.append(headers)
        
            for tr in table.find_all('tr')[1:]:  # Skip header row
                cells = [td.text.replace(",", "").strip() for td in tr.find_all('td')]
                rows.append(cells)
            return rows
    
        except requests.exceptions.RequestException as e:
                return f"Request error: {e}"
        except Exception as e:
            return f"An error occurred: {e}"
    
    if __name__ == "__main__":
        # url = "https://www.health.ny.gov/health_care/medicaid/rates/methodology/modifiers.htm" #sys.argv[1]
        # result = scrape_data_nys_apg_modifiers(url)
    
        # # Save the data into a CSV file
        # with open('table_data.csv', 'w', newline='') as file:
        #     writer = csv.writer(file)
        #     writer.writerows(result)  # Write the headers
    

        url = "https://www.cms.gov/medicare/payment/prospective-payment-systems/ambulatory-surgical-center-asc/restated-drug-and-biological-payment-rates"
        result = scrape_data_cms_restated_drug(url)
        print(result)
         # # Save the data into a CSV file
        with open('cms_data.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(result)  # Write the headers
   




# https://www.health.ny.gov/health_care/medicaid/rates/methodology/modifiers.htm
# NYS APG Modifiers
# Please do a full-field comparison, including field-by-field, contextual checks, blank vs filled, subtle word change: between the two files 

# https://www.cms.gov/medicare/payment/prospective-payment-systems/ambulatory-surgical-center-asc/restated-drug-and-biological-payment-rates