import os
import json
import logging
from datetime import datetime
import pytz

local_tz = pytz.timezone('Europe/Kiev')
TEMP_FILE_PATH = "/Users/dmitrijnazdrin/temp"

def save_stock_log(enterprise_code, formatted_json):
    try:
        stock_folder = os.path.join(TEMP_FILE_PATH, enterprise_code)
        os.makedirs(stock_folder, exist_ok=True)
        file_name = f"stock_{datetime.now(local_tz).strftime('%Y%m%d')}.json"
        file_path = os.path.join(stock_folder, file_name)

        logging.info(f"Attempting to write to: {file_path}")
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(formatted_json, file, ensure_ascii=False, indent=4)
        logging.info(f"Stock JSON log saved at {file_path}")
    except Exception as e:
        logging.error(f"Failed to save stock JSON log: {str(e)}")

test_data = {"Branches": [{"Code": "238", "Rests": [{"Code": "1001", "Price": 10, "Qty": 5, "PriceReserve": 9}]}]}
save_stock_log("238", test_data)