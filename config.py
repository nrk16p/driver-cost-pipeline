import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://adminbew:K879w5XpBm3QL046@mn-mongodb-ops-2c8032e6.mongo.ondigitalocean.com/terminus?authSource=admin",
)

SPREADSHEET_ID = "1ekZ2t1F4ENc4H6uByPzjxJ9Xbeci5p4oeeqyW2pPpKg"
NC_API_BASE = os.getenv("NC_API_BASE", "https://api-ncac.onrender.com")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials/service_account.json")

# Source collections
ATMS_DB = "atms"
DRIVERCOST_COLLECTION = "drivercost_tail"
VEHICLE_DAILY_COLLECTION = "vehicle_daily_asia"

# Target collection
TARGET_DB = "asia_incentive"
TARGET_COLLECTION = "driver_incentive_data"
