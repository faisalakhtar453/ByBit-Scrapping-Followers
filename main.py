import time
import os
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timezone
from selenium.common.exceptions import NoSuchElementException

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
    current_time = datetime.now(timezone.utc)
    if isinstance(data, list):
        for doc in data:
            doc["createdAt"] = current_time
            doc["updatedAt"] = current_time
        result = collection.insert_many(data)
        print(f"Data inserted with IDs: {result.inserted_ids}")
    else:
        data["createdAt"] = current_time
        data["updatedAt"] = current_time
        result = collection.insert_one(data)
        print(f"Data inserted with ID: {result.inserted_id}")

def scrape_table_data(url):
    """Scrape table data from a webpage and store it in MongoDB."""
    service = Service(ChromeDriverManager().install())
    options = Options()
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get(url)

        # Trigger followers list
        trigger_followers = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "radix-:r0:-trigger-followers"))
        )
        trigger_followers.click()

        # Wait for the content to load
        content_followers = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "radix-:r0:-content-followers"))
        )
        time.sleep(5)

        # Locate the table and extract data
        table = content_followers.find_element(By.TAG_NAME, "table")
        headers = [
            ''.join(word.capitalize() for word in header.text.strip().split())
            for header in table.find_elements(By.XPATH, ".//thead/tr/th")
        ]
        
        all_data = set()  # Use a set to store unique data
        while True:
            # Extract rows data from the table
            rows = table.find_elements(By.XPATH, ".//tbody/tr")
            for row in rows:
                cells = row.find_elements(By.XPATH, ".//td")
                if len(cells) == len(headers):
                    row_data = {headers[i]: cells[i].text.strip() for i in range(len(cells))}
                    if row_data.get("UserId"):
                        # Add the frozenset of row_data to ensure uniqueness
                        all_data.add(frozenset(row_data.items()))  # Use frozenset to make it hashable

            print(f"all_data =====> {all_data} <===========> {len(all_data)}")


            # Check if next page exists
            try:
                pagination_box = content_followers.find_element(By.CLASS_NAME, "quick-page-box")
                next_button = pagination_box.find_element(By.CSS_SELECTOR, ".quick-pg__np-page.quick-pg__last-page.cursor-pointer")

                # Scroll the element into view if it's out of view
                driver.execute_script("arguments[0].scrollIntoView();", next_button)
                time.sleep(1)  # Allow some time for smooth scrolling

                # Wait for the button to be clickable
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".quick-pg__np-page.quick-pg__last-page.cursor-pointer"))
                )

                # Check if the next button is disabled or not clickable
                if "quick-pg__cursor-not-allow" in next_button.get_attribute("class") or not next_button.is_enabled():
                    print("No more pages left to scrape.")
                    break

                # Click using JavaScript if it's still blocked
                try:
                    next_button.click()
                except:
                    print("Element click intercepted, attempting JavaScript click.")
                    driver.execute_script("arguments[0].click();", next_button)

                # Wait for new rows to load
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, ".//tbody/tr"))  # Wait for new rows to load
                )
                time.sleep(3)  # Short delay for additional AJAX loading if needed

            except NoSuchElementException:
                print("Next button not found or pagination ended.")
                break

        # Convert frozensets back to dictionaries for saving
        unique_data = [dict(item) for item in all_data]

        # Save and insert data
        insert_data("followers_table", unique_data)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        driver.quit()

# Run the scraper
scrape_table_data("https://www.bybit.com/copyTrade/trade-center/detail?leaderMark=JmKLl8FcguqEBL%2Fvc7Z3YQ%3D%3D&copyFrom=Search")
