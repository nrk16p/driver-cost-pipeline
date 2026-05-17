# Driver Cost Pipeline

Daily ETL pipeline for Asia fleet driver incentive data.

**Sources:** MongoDB (`atms`), Google Sheets, NC/AC APIs  
**Target:** MongoDB `asia_incentive.driver_incentive_data`  
**Schedule:** Every day at 09:00 AM Bangkok (02:00 UTC)

---

## Folder Structure

```
driver-cost-pipeline/
├── Jenkinsfile                   # CI/CD pipeline definition
├── main.py                       # Pipeline entry point
├── config.py                     # Config loaded from env vars
├── requirements.txt
├── .env.example                  # Template — copy to .env for local runs
├── .gitignore
└── credentials/
    └── service_account.json      # Google service account (NOT committed)
```

---

## Setup

### 1. Install dependencies
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Add credentials

Copy the Google service account JSON file to:
```
credentials/service_account.json
```

Copy `.env.example` to `.env` and fill in `MONGO_URI`:
```bash
cp .env.example .env
```

### 3. Run locally
```bash
python main.py
```

---

## Jenkins Setup

### Credentials to add in Jenkins

| ID | Type | Value |
|----|------|-------|
| `MONGO_URI` | Secret text | MongoDB connection string |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Secret file | `service_account.json` |

Go to **Jenkins → Manage Jenkins → Credentials** and add both.

### Create the job
1. New Item → Pipeline
2. Set SCM to this Git repo
3. Script Path: `Jenkinsfile`
4. The cron trigger `0 2 * * *` is already defined in the Jenkinsfile

---

## Data Flow

```
MongoDB atms.drivercost_tail       → LDT trip summary per driver
MongoDB atms.vehicle_daily_asia    → Working days per driver
Google Sheets (4tripdata, q_data)  → GPM trips & cubic meters
API /case_reports                  → NC cases per driver
API /accident-cases                → Accident cases per driver
                                        ↓
                        asia_incentive.driver_incentive_data
                        (upsert on mmyy + fleet + driver_id)
```
