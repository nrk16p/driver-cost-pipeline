"""
Driver Cost Pipeline — Asia Incentive
Queries MongoDB, Google Sheets, and NC/AC APIs then upserts to asia_incentive.driver_incentive_data
"""

import json
import logging
from datetime import datetime

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials
from pymongo import MongoClient, UpdateOne

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

STATUS_CATEGORY_MAP = {
    "A": "ทำงานปกติ",
    "A75": "ทำงานบางเวลา",
    "A50": "ทำงานบางเวลา",
    "A25": "ทำงานบางเวลา",
    "Aส": "มาสาย / ผิดวินัย",
    "Aน": "มาสาย / ผิดวินัย",
    "Aด": "มาสาย / ผิดวินัย",
    "Aป": "ปฏิเสธงาน / โอนย้าย",
    "ลซ": "ปฏิเสธงาน / โอนย้าย",
    "Aค": "ทำงานพิเศษ / ครูฝึก",
    "AX": "ทำงานพิเศษ / ครูฝึก",
    "Aซ": "รถซ่อมแต่คนขับไปขับคันอื่น",
    "Aล": "รถซ่อมแต่คนขับไปขับคันอื่น",
    "ฝ": "ฝึกงาน / อบรม",
    "ลส": "ฝึกงาน / อบรม",
    "ล": "ลางาน",
    "ก": "ลางาน",
    "ป": "ลางาน",
    "กด": "กักตัว",
    "กม": "กักตัว",
    "ย": "หยุด / ไม่มีรายได้",
    "จ": "เจ็บจากงาน",
    "ลอ": "พ้นสภาพ",
    "ปอ": "พ้นสภาพ",
    "X": "ไม่ได้งาน / ตกคิว",
    "ลพ": "รถว่างเพราะปัญหาคนขับ",
    "ลข": "รถว่างเพราะปัญหาคนขับ",
}

WORK_GROUP_MAP = {
    "ทำงานปกติ": "ทำงาน",
    "ทำงานบางเวลา": "ทำงาน",
    "มาสาย / ผิดวินัย": "ทำงาน",
    "ทำงานพิเศษ / ครูฝึก": "ทำงาน",
    "รถซ่อมแต่คนขับไปขับคันอื่น": "ทำงาน",
    "ฝึกงาน / อบรม": "ไม่ใช่งานปกติ",
    "ลางาน": "ไม่ทำงาน",
    "กักตัว": "ไม่ทำงาน",
    "หยุด / ไม่มีรายได้": "ไม่ทำงาน",
    "เจ็บจากงาน": "ไม่ทำงาน",
    "พ้นสภาพ": "ไม่ทำงาน",
    "ไม่ได้งาน / ตกคิว": "ไม่ได้งาน",
    "รถว่างเพราะปัญหาคนขับ": "ไม่ได้งาน",
    "ปฏิเสธงาน / โอนย้าย": "ไม่ได้งาน",
    "ไม่ระบุ": "ไม่ระบุ",
}


def fetch_driver_cost(mongo_client: MongoClient) -> pd.DataFrame:
    """Query drivercost_tail and return LDT summary grouped by mmyy + driver name."""
    year = str(datetime.now().year)
    collection = mongo_client[config.ATMS_DB][config.DRIVERCOST_COLLECTION]

    cursor = collection.find(
        {
            "mmyy": {"$regex": rf"/{year}$"},
            "report_title": {"$regex": "ลาดกระบัง", "$options": "i"},
        },
        {"_id": 0},
    )
    df = pd.DataFrame(list(cursor))
    log.info(f"drivercost_tail: {len(df)} rows")

    cols = [
        "ออก LDT", "mmyy", "บริการ", "LDT", "แพล้นท์", "Route/Ship To", "subcode",
        "ชื่อshipto", "โซน", "รหัสต้นทาง", "ชื่อต้นทาง", "รหัสปลายทาง",
        "พจส1", "ประเภทรถร่วม", "เลขรถ", "หัว", "น้ำหนักปลายทาง",
    ]
    df = df[cols].copy()
    df["น้ำหนักปลายทาง"] = pd.to_numeric(df["น้ำหนักปลายทาง"], errors="coerce").fillna(0).astype(int)
    df = df[df["บริการ"] == "Mixer เอเชีย - MIXB"]

    summary = (
        df.groupby(["mmyy", "พจส1"], dropna=False)
        .agg(
            count_LDT=("LDT", "nunique"),
            count_unique_LDT_Date=("ออก LDT", "nunique"),
            total_q_ldt=("น้ำหนักปลายทาง", "sum"),
        )
        .reset_index()
    )
    log.info(f"driver_cost summary: {len(summary)} rows")
    return summary


def fetch_vehicle_daily(mongo_client: MongoClient) -> pd.DataFrame:
    """Query vehicle_daily_asia and return working-day rows for Asia fleet."""
    collection = mongo_client[config.ATMS_DB][config.VEHICLE_DAILY_COLLECTION]
    df = pd.DataFrame(list(collection.find({}, {"_id": 0})))
    df = df[df["ฟลีท"] == "Asia"]

    df = df[["วันที่", "ฟลีท", "ลูกค้า", "รหัส", "แพล้นท์", "เบอร์รถ",
             "ทะเบียน", "สถานะ", "คนขับ", "รหัสคนขับ", "ชื่อคนขับ"]].copy()

    df["status_category"] = df["คนขับ"].map(STATUS_CATEGORY_MAP).fillna("ไม่ระบุ")
    df["work_group"] = df["status_category"].map(WORK_GROUP_MAP).fillna("ไม่ระบุ")
    df["mmyy"] = df["วันที่"].astype(str).str[3:10]

    df_work = df[
        (df["work_group"] == "ทำงาน")
        & (df["ชื่อคนขับ"].notna())
        & (df["ชื่อคนขับ"].astype(str).str.strip() != "")
    ].copy()

    df_work["is_late"] = (df_work["คนขับ"] == "Aส").astype(int)
    log.info(f"vehicle_daily_asia working rows: {len(df_work)}")
    return df_work


def _gsheet_client() -> gspread.Client:
    with open(config.GOOGLE_CREDENTIALS_PATH) as f:
        info = json.load(f)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def fetch_gsheet_trips(df_work: pd.DataFrame) -> pd.DataFrame:
    """Merge Google Sheets 4tripdata and q_data into df_work."""
    gs = _gsheet_client()
    spreadsheet = gs.open_by_key(config.SPREADSHEET_ID)

    df_work["วันที่"] = pd.to_datetime(df_work["วันที่"], format="%d/%m/%Y", errors="coerce")

    # 4tripdata
    ws = spreadsheet.worksheet("4tripdata")
    df_gpm = pd.DataFrame(ws.get_all_records())
    df_gpm["Date"] = pd.to_datetime(df_gpm["Date"], format="%Y-%m-%d", errors="coerce")
    df_work = df_work.merge(
        df_gpm[["Date", "ทะเบียนรถ", "# Trip"]],
        how="left",
        left_on=["วันที่", "ทะเบียน"],
        right_on=["Date", "ทะเบียนรถ"],
    )
    log.info(f"After 4tripdata merge: {len(df_work)} rows")

    # q_data
    ws_q = spreadsheet.worksheet("q_data")
    df_gpm_q = pd.DataFrame(ws_q.get_all_records())
    df_gpm_q["Date"] = pd.to_datetime(df_gpm_q["Date"], format="%Y-%m-%d", errors="coerce")
    df_work = df_work.merge(
        df_gpm_q[["Date", "ทะเบียนรถ", "#_Q"]],
        how="left",
        left_on=["วันที่", "ทะเบียน"],
        right_on=["Date", "ทะเบียนรถ"],
    )
    log.info(f"After q_data merge: {len(df_work)} rows")
    return df_work


def fetch_nc_cases() -> pd.DataFrame:
    """Fetch NC case reports from API for current year."""
    start_date = datetime.now().strftime("%Y-%m-01")
    resp = requests.get(f"{config.NC_API_BASE}/case_reports", params={"start_date": start_date}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data if isinstance(data, list) else next(v for v in data.values() if isinstance(v, list)))

    df = df[["case_id", "document_no", "driver_name", "casestatus", "incident_date"]]
    df = df[df["casestatus"] != "Voided"].copy()
    df["incident_date"] = pd.to_datetime(df["incident_date"], errors="coerce", format="mixed")
    df["mmyy"] = df["incident_date"].dt.strftime("%m/%Y")

    current_mmyy = [datetime.now().strftime("%m/%Y")]
    df = df[df["mmyy"].isin(current_mmyy)]

    driver_nc = (
        df.groupby(["mmyy", "driver_name"], dropna=False)
        .agg(total_nc=("document_no", "size"))
        .reset_index()
    )
    log.info(f"NC cases: {len(driver_nc)} driver-month rows")
    return driver_nc


def fetch_ac_cases() -> pd.DataFrame:
    """Fetch accident cases from API for current year."""
    start_date = datetime.now().strftime("%Y-%m-01")
    resp = requests.get(f"{config.NC_API_BASE}/accident-cases", params={"start_date": start_date}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data if isinstance(data, list) else next(v for v in data.values() if isinstance(v, list)))

    df = df[["accident_case_id", "document_no_ac", "driver_name", "casestatus", "incident_datetime"]].copy()
    df = df[df["casestatus"] != "Voided"]
    df["incident_datetime"] = pd.to_datetime(df["incident_datetime"], errors="coerce", format="mixed")
    df["mmyy"] = df["incident_datetime"].dt.strftime("%m/%Y")

    driver_ac = (
        df.groupby(["mmyy", "driver_name"], dropna=False)
        .agg(total_ac=("document_no_ac", "size"))
        .reset_index()
    )
    log.info(f"AC cases: {len(driver_ac)} driver-month rows")
    return driver_ac


def build_summary(df_work: pd.DataFrame, ldt_summary: pd.DataFrame, driver_nc: pd.DataFrame, driver_ac: pd.DataFrame) -> pd.DataFrame:
    """Aggregate work days and merge all sources into final summary."""
    summary = (
        df_work.groupby(["mmyy", "ฟลีท", "รหัสคนขับ", "ชื่อคนขับ", "สถานะ", "รหัส", "แพล้นท์"], dropna=False)
        .agg(
            working_days=("work_group", "size"),
            late_days=("is_late", "sum"),
            gpm_total_trip=("# Trip", "sum"),
            gpm_total_q=("#_Q", "sum"),
        )
        .reset_index()
        .rename(columns={
            "ฟลีท": "fleet",
            "รหัสคนขับ": "driver_id",
            "ชื่อคนขับ": "driver_name",
            "สถานะ": "สถานะ",
            "รหัส": "รหัส",
            "แพล้นท์": "แพล้นท์",
        })
    )

    summary = summary.merge(ldt_summary, how="left", left_on=["mmyy", "driver_name"], right_on=["mmyy", "พจส1"])
    summary = summary.drop(columns=["พจส1"], errors="ignore")

    summary = summary.merge(driver_nc, how="left", on=["mmyy", "driver_name"])
    summary = summary.merge(driver_ac, how="left", on=["mmyy", "driver_name"])

    summary = summary[[
        "mmyy", "fleet", "driver_id", "driver_name",
        "working_days", "late_days",
        "count_LDT", "count_unique_LDT_Date", "total_q_ldt",
        "gpm_total_trip", "gpm_total_q", "สถานะ", "รหัส", "แพล้นท์",
        "total_nc", "total_ac",
    ]].rename(columns={
        "count_LDT": "total_trip",
        "count_unique_LDT_Date": "total_unique_date_trip",
        "total_q_ldt": "total_q",
    })

    summary = summary.fillna(0)
    log.info(f"Final summary: {len(summary)} rows")
    return summary


def upsert_to_mongo(mongo_client: MongoClient, df: pd.DataFrame) -> None:
    """Upsert summary rows into asia_incentive.driver_incentive_data."""
    collection = mongo_client[config.TARGET_DB][config.TARGET_COLLECTION]

    collection.create_index([("mmyy", 1), ("fleet", 1), ("driver_id", 1)], unique=True)

    df_upload = df.copy()
    numeric_cols = ["working_days", "late_days", "total_trip", "total_unique_date_trip",
                    "total_q", "gpm_total_trip", "gpm_total_q", "total_nc", "total_ac"]
    for col in numeric_cols:
        if col in df_upload.columns:
            df_upload[col] = pd.to_numeric(df_upload[col], errors="coerce").fillna(0)

    for col in ["mmyy", "fleet", "driver_id", "driver_name"]:
        if col in df_upload.columns:
            df_upload[col] = df_upload[col].astype(str).str.strip()

    df_upload = df_upload.drop_duplicates(subset=["mmyy", "fleet", "driver_id"], keep="last")
    df_upload["updated_at"] = datetime.now()

    now = datetime.now()
    ops = [
        UpdateOne(
            {"mmyy": r["mmyy"], "fleet": r["fleet"], "driver_id": r["driver_id"]},
            {"$set": r, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        for r in df_upload.to_dict("records")
    ]

    result = collection.bulk_write(ops, ordered=False)
    log.info(f"Upsert done — matched: {result.matched_count}, inserted: {result.upserted_count}, modified: {result.modified_count}")


def main():
    log.info("=== Driver Cost Pipeline START ===")
    mongo_client = MongoClient(
        config.MONGO_URI,
        serverSelectionTimeoutMS=60000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000,
    )
    # verify connection before proceeding
    try:
        mongo_client.admin.command("ping")
        log.info("MongoDB connected OK")
    except Exception as e:
        log.error(f"MongoDB connection failed: {e}")
        raise

    try:
        ldt_summary = fetch_driver_cost(mongo_client)
        df_work = fetch_vehicle_daily(mongo_client)
        df_work = fetch_gsheet_trips(df_work)
        driver_nc = fetch_nc_cases()
        driver_ac = fetch_ac_cases()
        final = build_summary(df_work, ldt_summary, driver_nc, driver_ac)
        upsert_to_mongo(mongo_client, final)
    finally:
        mongo_client.close()

    log.info("=== Driver Cost Pipeline DONE ===")


if __name__ == "__main__":
    main()
