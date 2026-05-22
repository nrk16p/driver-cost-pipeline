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
    """Query drivercost_tail for current month only, return LDT summary by driver."""
    now = datetime.now()
    mmyy = now.strftime("%m/%Y")  # e.g. "05/2026"
    collection = mongo_client[config.ATMS_DB][config.DRIVERCOST_COLLECTION]

    # project only columns we need to minimise memory
    projection = {
        "_id": 0,
        "ออก LDT": 1, "mmyy": 1, "บริการ": 1, "LDT": 1,
        "พจส1": 1, "น้ำหนักปลายทาง": 1,
    }
    cursor = collection.find(
        {
            "mmyy": mmyy,
            "report_title": {"$regex": "ลาดกระบัง", "$options": "i"},
        },
        projection,
    )
    df = pd.DataFrame(list(cursor))
    log.info(f"drivercost_tail ({mmyy}): {len(df)} rows")

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
    """Query vehicle_daily_asia for current month, Asia fleet only."""
    now = datetime.now()
    mmyy = now.strftime("%m/%Y")
    collection = mongo_client[config.ATMS_DB][config.VEHICLE_DAILY_COLLECTION]

    projection = {
        "_id": 0,
        "วันที่": 1, "ฟลีท": 1, "ลูกค้า": 1, "รหัส": 1, "แพล้นท์": 1,
        "เบอร์รถ": 1, "ทะเบียน": 1, "สถานะ": 1, "คนขับ": 1, "รหัสคนขับ": 1, "ชื่อคนขับ": 1,
    }
    # filter by fleet and current month via mmyy field
    cursor = collection.find({"ฟลีท": "Asia", "mmyy": mmyy}, projection)
    df = pd.DataFrame(list(cursor))

    if df.empty:
        # fallback: mmyy field may not exist — filter by date prefix in วันที่
        cursor = collection.find({"ฟลีท": "Asia"}, projection)
        df = pd.DataFrame(list(cursor))
        df["mmyy_tmp"] = df["วันที่"].astype(str).str[3:10]
        df = df[df["mmyy_tmp"] == mmyy].drop(columns=["mmyy_tmp"])

    log.info(f"vehicle_daily_asia ({mmyy}): {len(df)} raw rows")
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
    # fill NaT dates reconstructed from Month + Day columns (Date column is blank for older rows)
    reconstructed = pd.to_datetime(
        df_gpm["Month"].astype(str) + "-" + df_gpm["Day"].astype(str).str.zfill(2),
        format="%Y-%m-%d", errors="coerce",
    )
    df_gpm["Date"] = df_gpm["Date"].where(df_gpm["Date"].notna(), reconstructed)
    log.info(f"4tripdata: {len(df_gpm)} rows, {df_gpm['Date'].notna().sum()} with valid Date after fill")
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
    # same date fill for q_data
    reconstructed_q = pd.to_datetime(
        df_gpm_q["Month"].astype(str) + "-" + df_gpm_q["Day"].astype(str).str.zfill(2),
        format="%Y-%m-%d", errors="coerce",
    )
    df_gpm_q["Date"] = df_gpm_q["Date"].where(df_gpm_q["Date"].notna(), reconstructed_q)
    log.info(f"q_data: {len(df_gpm_q)} rows, {df_gpm_q['Date'].notna().sum()} with valid Date after fill")
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

    df = df[["accident_case_id", "document_no_ac", "driver_name", "casestatus", "incident_datetime","fault_party"]].copy()

    df = df[df["casestatus"] != "Voided"]
    df["incident_datetime"] = pd.to_datetime(df["incident_datetime"], errors="coerce", format="mixed")
    df["mmyy"] = df["incident_datetime"].dt.strftime("%m/%Y")
    df = df[df['fault_party']=="เป็นฝ่ายผิด"]
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


def debug_merge():
    """Diagnostic only — prints key join columns from both sides, no writes."""
    print("\n" + "="*60)
    print("DEBUG: checking 4tripdata merge keys")
    print("="*60)

    mongo_client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=30000)

    # ── MongoDB side (mirror fallback logic in fetch_vehicle_daily) ──
    now = datetime.now()
    mmyy = now.strftime("%m/%Y")
    col = mongo_client[config.ATMS_DB][config.VEHICLE_DAILY_COLLECTION]

    # try mmyy field first
    cursor = col.find({"ฟลีท": "Asia", "mmyy": mmyy}, {"_id": 0, "วันที่": 1, "ทะเบียน": 1})
    df_mongo = pd.DataFrame(list(cursor))
    print(f"\n[MongoDB] direct mmyy={mmyy} query: {len(df_mongo)} rows")

    # fallback: filter by วันที่ date prefix (same as fetch_vehicle_daily)
    if df_mongo.empty:
        print("  → 0 rows via mmyy field — trying fallback (all Asia, filter by วันที่ prefix)")
        cursor = col.find({"ฟลีท": "Asia"}, {"_id": 0, "วันที่": 1, "ทะเบียน": 1})
        df_all = pd.DataFrame(list(cursor))
        print(f"  → All Asia rows fetched: {len(df_all)}")
        if not df_all.empty:
            df_all["mmyy_tmp"] = df_all["วันที่"].astype(str).str[3:10]
            print(f"  → Sample mmyy_tmp values: {df_all['mmyy_tmp'].unique()[:10].tolist()}")
            df_mongo = df_all[df_all["mmyy_tmp"] == mmyy].drop(columns=["mmyy_tmp"])
            print(f"  → Rows after fallback filter for mmyy={mmyy}: {len(df_mongo)}")

    mongo_client.close()

    if not df_mongo.empty:
        df_mongo["วันที่_parsed"] = pd.to_datetime(df_mongo["วันที่"], format="%d/%m/%Y", errors="coerce")
        print("  Sample วันที่ (raw)   :", df_mongo["วันที่"].head(5).tolist())
        print("  Sample วันที่ (parsed):", df_mongo["วันที่_parsed"].head(5).tolist())
        print("  Sample ทะเบียน       :", df_mongo["ทะเบียน"].head(5).tolist())
    else:
        print("  !! No MongoDB rows found even after fallback — vehicle_daily_asia may be empty for this month")

    # ── Google Sheet side ────────────────────────────────────
    gs = _gsheet_client()
    ws = gs.open_by_key(config.SPREADSHEET_ID).worksheet("4tripdata")
    df_gpm = pd.DataFrame(ws.get_all_records())
    print(f"\n[GSheet]  4tripdata rows total: {len(df_gpm)}")
    print(f"  Columns: {df_gpm.columns.tolist()}")

    df_gpm["Date_parsed"] = pd.to_datetime(df_gpm["Date"], format="%Y-%m-%d", errors="coerce")
    current_month_rows = df_gpm[df_gpm["Date_parsed"].dt.strftime("%m/%Y") == mmyy]
    print(f"  Rows where Date matches mmyy={mmyy}: {len(current_month_rows)}")

    print("  Sample Date (raw)    :", df_gpm["Date"].head(5).tolist())
    print("  Sample Date (parsed) :", df_gpm["Date_parsed"].head(5).tolist())
    print("  Sample ทะเบียนรถ     :", df_gpm["ทะเบียนรถ"].head(5).tolist())

    # ── Rows with real trips this month ──────────────────────
    has_trips = current_month_rows[current_month_rows["# Trip"].apply(pd.to_numeric, errors="coerce").fillna(0) > 0]
    print(f"\n  Rows with # Trip > 0 in current month: {len(has_trips)}")
    if not has_trips.empty:
        print(has_trips[["Date", "ทะเบียนรถ", "# Trip"]].head(5).to_string(index=False))

    # ── Overlap check ─────────────────────────────────────────
    if not df_mongo.empty and not df_gpm.empty:
        mongo_plates = set(df_mongo["ทะเบียน"].astype(str).str.strip())
        sheet_plates = set(df_gpm["ทะเบียนรถ"].astype(str).str.strip())
        overlap = mongo_plates & sheet_plates
        print(f"\n  Plate overlap (MongoDB ∩ GSheet): {len(overlap)} plates")
        print(f"  MongoDB-only plates (sample)    : {list(mongo_plates - sheet_plates)[:5]}")
        print(f"  GSheet-only plates  (sample)    : {list(sheet_plates - mongo_plates)[:5]}")
        if overlap:
            print(f"  Overlapping plates  (sample)    : {list(overlap)[:5]}")

    # ── Actual merge result check ──────────────────────────────
    if not df_mongo.empty and not df_gpm.empty:
        print("\n--- Merge result (WITH date reconstruction fix) ---")
        df_mongo["วันที่_parsed"] = pd.to_datetime(df_mongo["วันที่"], format="%d/%m/%Y", errors="coerce")

        # apply the fix: reconstruct Date from Month + Day where empty
        df_gpm["Date_parsed"] = pd.to_datetime(df_gpm["Date"], format="%Y-%m-%d", errors="coerce")
        reconstructed_debug = pd.to_datetime(
            df_gpm["Month"].astype(str) + "-" + df_gpm["Day"].astype(str).str.zfill(2),
            format="%Y-%m-%d", errors="coerce",
        )
        df_gpm["Date_parsed"] = df_gpm["Date_parsed"].where(df_gpm["Date_parsed"].notna(), reconstructed_debug)
        may_rows = (df_gpm["Date_parsed"].dt.strftime("%m/%Y") == mmyy).sum()
        print(f"  GSheet rows with valid Date for {mmyy} AFTER reconstruction: {may_rows}")

        merged = df_mongo.merge(
            df_gpm[["Date_parsed", "ทะเบียนรถ", "# Trip"]],
            how="left",
            left_on=["วันที่_parsed", "ทะเบียน"],
            right_on=["Date_parsed",  "ทะเบียนรถ"],
        )
        matched   = merged["# Trip"].notna().sum()
        unmatched = merged["# Trip"].isna().sum()
        print(f"  Total merged rows : {len(merged)}")
        print(f"  Matched (# Trip not null) : {matched}")
        print(f"  Unmatched (# Trip null)   : {unmatched}")

        hits = merged[merged["# Trip"].notna() & (merged["# Trip"] > 0)]
        print(f"  Rows with # Trip > 0      : {len(hits)}")
        if not hits.empty:
            print("  Sample matched rows:")
            print(hits[["วันที่", "ทะเบียน", "Date_parsed", "ทะเบียนรถ", "# Trip"]].head(5).to_string(index=False))
        else:
            print("  !! No rows matched with # Trip > 0 — join is producing no hits")
            # show one overlapping plate's data from both sides
            if overlap:
                sample_plate = list(overlap)[0]
                print(f"\n  Investigating plate: {sample_plate}")
                print("  MongoDB dates for this plate:")
                print(df_mongo[df_mongo["ทะเบียน"] == sample_plate]["วันที่_parsed"].head(5).tolist())
                print("  GSheet dates for this plate:")
                gsheet_rows = df_gpm[df_gpm["ทะเบียนรถ"] == sample_plate]
                print(gsheet_rows[["Date_parsed", "# Trip"]].head(5).to_string(index=False))

    # ── เบอร์รถ overlap check ──────────────────────────────────
    print("\n--- เบอร์รถ overlap check ---")
    col2 = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=30000)[config.ATMS_DB][config.VEHICLE_DAILY_COLLECTION]
    cursor2 = col2.find({"ฟลีท": "Asia"}, {"_id": 0, "วันที่": 1, "เบอร์รถ": 1, "ทะเบียน": 1})
    df_mongo2 = pd.DataFrame(list(cursor2))
    df_mongo2["mmyy_tmp"] = df_mongo2["วันที่"].astype(str).str[3:10]
    df_mongo2 = df_mongo2[df_mongo2["mmyy_tmp"] == mmyy]

    mongo_bor = set(df_mongo2["เบอร์รถ"].astype(str).str.strip())
    sheet_bor = set(df_gpm["เบอร์รถ"].astype(str).str.strip())
    overlap_bor = mongo_bor & sheet_bor

    print(f"  MongoDB เบอร์รถ unique  : {len(mongo_bor)}")
    print(f"  GSheet  เบอร์รถ unique  : {len(sheet_bor)}")
    print(f"  Overlap เบอร์รถ         : {len(overlap_bor)}")
    print(f"  Sample MongoDB เบอร์รถ  : {list(mongo_bor)[:8]}")
    print(f"  Sample GSheet  เบอร์รถ  : {list(sheet_bor)[:8]}")
    print(f"  Sample overlap เบอร์รถ  : {list(overlap_bor)[:8]}")

    # test merge on เบอร์รถ + date
    if not df_mongo2.empty:
        df_mongo2["วันที่_parsed"] = pd.to_datetime(df_mongo2["วันที่"], format="%d/%m/%Y", errors="coerce")
        merged_bor = df_mongo2.merge(
            df_gpm[["Date_parsed", "เบอร์รถ", "# Trip"]],
            how="left",
            left_on=["วันที่_parsed", "เบอร์รถ"],
            right_on=["Date_parsed",   "เบอร์รถ"],
        )
        matched_bor   = merged_bor["# Trip"].notna().sum()
        has_trips_bor = (merged_bor["# Trip"].fillna(0) > 0).sum()
        print(f"\n  Merge on (date + เบอร์รถ):")
        print(f"    Matched rows       : {matched_bor} / {len(merged_bor)}")
        print(f"    Rows with # Trip>0 : {has_trips_bor}")
        if has_trips_bor > 0:
            hits2 = merged_bor[merged_bor["# Trip"] > 0]
            print("    Sample matched rows:")
            print(hits2[["วันที่", "เบอร์รถ", "# Trip"]].head(5).to_string(index=False))

    print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "debug":
        debug_merge()
    else:
        main()
