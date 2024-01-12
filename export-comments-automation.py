import selenium.webdriver as webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep
import configparser
from datetime import datetime, timedelta
import pytz
import os
import logging




# Configure logging to output to the terminal
logging.basicConfig(level=logging.INFO,  # Set the logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')  # Set the format
# Create a config object
CONFIG = configparser.ConfigParser()
# Read the configuration file
CONFIG.read('export-comments-config.ini')
DOWNLOAD_DIRECTORY = CONFIG.get('Settings','downloadDirectory')
CHROME_PROFILES_FOLDER = './chrome-profiles-folder'
PAGE_LOAD_WAIT_TIME = int(CONFIG.get('Settings','pageLoadWaitTime'))
ACTION_WAIT_TIME = int(CONFIG.get('Settings','actionWaitTime'))
EXPORT_WAIT_TIME = int(CONFIG.get('Settings','exportWaitTime'))


def get_web_driver():
    # Set up Chrome options
    logging.info('initializing chrome webdriver')
    options = Options()
    options.add_argument(f"user-data-dir={CHROME_PROFILES_FOLDER}")
    prefs = {"download.default_directory" : f"{DOWNLOAD_DIRECTORY}"}
    options.add_experimental_option("prefs",prefs)
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(PAGE_LOAD_WAIT_TIME)  # Wait up to 10 seconds for elements to appear
    return driver


def login_to_export_comments_site(driver):
    logging.info('Starting Login Process')
    driver.get("https://exportcomments.com/login")
    # if we're already logged in, this will direct us to the input page
    #search for unique element, if it's found, we're already logged in
    try:
        login_form_span_element = driver.find_element(By.XPATH, "//span[@class='login-form__or text-muted']")
        if login_form_span_element:
            logging.info('Logging in with username and password')
            # Access settings
            username = CONFIG.get('Credentials', 'username')
            password = CONFIG.get('Credentials', 'password')
            email_text_box = driver.find_element(By.XPATH, "//input[@placeholder='Email']")
            password_text_box = driver.find_element(By.XPATH, "//input[@placeholder='Password']")
            login_button = driver.find_element(By.XPATH, "//button[text()=' Login']")

            email_text_box.send_keys(username)
            password_text_box.send_keys(password)
            sleep(ACTION_WAIT_TIME)
            login_button.click()
            sleep(ACTION_WAIT_TIME)
    except NoSuchElementException:
        logging.info('Already logged in')


def export_followed_accounts(driver, twitter_handle):
    driver.get("https://exportcomments.com/")
    logging.info(f'Fetching followed accounts for user with handle "{twitter_handle}"')
    # Find the input box and enter the twitter account
    export_text_box = driver.find_element(By.ID, "export_url")
    export_text_box.send_keys(twitter_handle)
    sleep(ACTION_WAIT_TIME)

    # select "followers"
    following_button = driver.find_element(By.XPATH, "//input[@value='Following']/following-sibling::label")
    following_button.click()

    # Find and click the "Start Export Process" button
    export_button = driver.find_element(By.XPATH, '//button[@class="btn btn-block btn-primary btn-lg"]')
    export_button.click()
    # Go to the desired URL


# downloads exports from the last num_days days 
def download_exports_by_date(driver, num_days=1):
    logging.info('Beginning export download process')
    logging.info(f'Expecting downloads in directory: {DOWNLOAD_DIRECTORY}')
    # go to the exports page
    driver.get('https://exportcomments.com/user/exports')
    
    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.XPATH, '//table[@class="exportcomments-table exportcomments-table--exports"]'))
                                )
    table_element = driver.find_element(By.XPATH, '//table[@class="exportcomments-table exportcomments-table--exports"]')
    # get the row elements of the exports table
    table_row_elements = table_element.find_elements(By.XPATH,'.//tbody//tr')
    current_time_utc = datetime.now(pytz.utc)
    
    logging.info(f'Found {len(table_row_elements)} files available for download')

    
    # iterate through row elements, extracting the date of completion, status, filename and the download button
    for row_element in table_row_elements:
        # check if the export process is done
        status = row_element.find_element(By.XPATH,"./td[@data-label='Status']//span[@class='v-chip__content']").text
        if status != "Done":
            logging.info(f'skipping export with status "{status}"')
            continue
        
        
        # Check if the time difference is less than num_days days
        date_title_attr = row_element.find_element(By.XPATH,'.//td[@data-label="Date Exported"]//span').get_attribute("title")
        date_exported_utc = datetime.strptime(date_title_attr, "%b %d, %Y %I:%M %p").replace(tzinfo=pytz.utc)
        time_difference = current_time_utc - date_exported_utc
        if time_difference > timedelta(days=num_days):
            logging.info(f'Skipped file that is not ready')
            continue
        
        # check if you have already downloaded the file
        export_filename = row_element.find_element(By.XPATH,"./td[@data-label='Export File']//a").text
        export_file_path = os.path.join(DOWNLOAD_DIRECTORY, export_filename)
        download_button = row_element.find_element(By.XPATH,"./td[@data-label='Actions']//button[@title='Download file']")

        if os.path.exists(export_file_path):
            logging.info(f"File '{export_filename}' exists in the download directory. Skipping download")
        else:
            logging.info(f'Downloading file {export_filename}')
            download_button.click()
            sleep(ACTION_WAIT_TIME)

        
def get_handle_links_from_file(filename):
    
    logging.info('Reading handles from file and converting to profile links')
    # Open the file and read lines
    with open(filename, 'r') as file:
        lines = file.readlines()
        
    # Initialize an empty list to hold modified lines
    modified_lines = []

    # Strip @ characters and craft twitter url
    for line in lines:
        modified_line = line.replace('@','').strip()
        modified_line = f'https://twitter.com/{modified_line}/following'
        modified_lines.append(modified_line)

    # Return the list of modified lines
    return modified_lines

def main():
    twitter_profile_followers_links = get_handle_links_from_file(CONFIG.get('Settings','handleList'))
    driver = get_web_driver()
    login_to_export_comments_site(driver)
    
    # add error handling and de-duplication
    for link in twitter_profile_followers_links:
        export_followed_accounts(driver, link)
        sleep(ACTION_WAIT_TIME)
    
    # wait before beginning download process     
    logging.info(f'waiting {EXPORT_WAIT_TIME} seconds before starting download process')
    # download exports
    download_exports_by_date(driver, num_days=2)

if __name__ == '__main__':
    main()