import time
import os
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timezone


# Load environment variables
load_dotenv()

# MongoDB setup
MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
client = MongoClient(MONGODB_URI)
db = client[DATABASE_NAME]

def insert_data(collection_name, data):
    """Insert data into MongoDB collection."""
    collection = db[collection_name]
    current_time = datetime.now(timezone.utc)  # Use timezone-aware datetime
    # Add a createdAt field to each document
    if isinstance(data, list):  # Insert multiple documents
        for doc in data:
            doc["createdAt"] = current_time
            doc["updatedAt"] = current_time
        result = collection.insert_many(data)
        print(f"Data inserted with IDs: {result.inserted_ids}")
    else:  # Insert a single document
        data["createdAt"] = current_time
        data["updatedAt"] = current_time
        result = collection.insert_one(data)
        print(f"Data inserted with ID: {result.inserted_id}")

def save_to_csv(headers, table_data, filename="followers_data.csv"):
    """Save the table data to a CSV file."""
    try:
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            # Write headers first
            writer.writerow(headers)
            # Write table rows
            for row in table_data:
                writer.writerow(row.values())
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"An error occurred while saving to CSV: {e}")


def scrape_table_data(url):
    """Scrape table data from a webpage and store it in MongoDB."""
    # Set up the browser driver
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # Open the target URL
        driver.get(url)

        # Step 1: Find and click the "trigger-followers" button
        trigger_followers = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "radix-:r0:-trigger-followers"))
        )
        trigger_followers.click()

        # Step 2: Wait for the table to load
        content_followers = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "radix-:r0:-content-followers"))
        )

        # Extra wait for AJAX data to load
        time.sleep(5)  # Adjust if necessary

        # Wait until the number of rows in the table body is greater than 1
        WebDriverWait(driver, 10).until(
            lambda d: len(content_followers.find_elements(By.XPATH, ".//tbody/tr")) > 1
        )

        # Step 3: Locate the table
        table = content_followers.find_element(By.TAG_NAME, "table")

        # # Debug: Print the table's innerHTML to inspect the structure
        # print("Table innerHTML:")
        # print(table.get_attribute("innerHTML"))

        # Extract headers and convert them to PascalCase directly
        headers = [
            ''.join(word.capitalize() for word in header.text.strip().split()) 
            for header in table.find_elements(By.XPATH, ".//thead/tr/th")
        ]

        print("Headers:", headers)

        # Extract data rows
        rows = table.find_elements(By.XPATH, ".//tbody/tr")
        table_data = []

        for row in rows:
            cells = row.find_elements(By.XPATH, ".//td")
            if len(cells) == len(headers):  # Ensure the row matches header length
                row_data = {headers[i]: cells[i].text.strip() for i in range(len(cells))}
                # Only add the row if UserId is not empty
                if row_data.get('UserId'):  # Check if UserId exists and is not empty
                    table_data.append(row_data)
        
        # Debug: Print extracted table data
        print("Extracted Table Data:")
        for row in table_data:
            print(row)

        # Step 4: Save the data to a CSV file
        save_to_csv(headers, table_data)

        # Step 5: Insert data into MongoDB
        # insert_data("followers_table", table_data)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Quit the browser
        driver.quit()

# Run the scraper
scrape_table_data("https://www.bybit.com/copyTrade/trade-center/detail?leaderMark=ZsnoyKzFgiFy9eFnnR2UKA%3D%3D&profileDay=30&copyFrom=CTIndex")
