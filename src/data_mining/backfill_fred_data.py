import os
import sys
import requests
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
import time

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

if not FRED_API_KEY or not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ 누락된 환경 변수가 있습니다. (.env 파일 확인 필요)")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 기존 fetch_fred_data와 동일한 지표 세트
DAILY_SERIES = {
    "fred_wti": "DCOILWTICO",
    "fred_brent": "DCOILBRENTE",
    "fred_usd_index": "DTWEXBGS",  
    "fred_treasury_10y": "DGS10",
    "fred_treasury_2y": "DGS2",
    "fred_natural_gas": "DHHNGSP",
    "fred_heating_oil": "DHOILNYH"
}

MONTHLY_SERIES = {
    "fred_gepu": "GEPUCURRENT",
    "fred_cpi": "CPIAUCSL",
    "fred_pce": "PCEPI",
    "fred_unrate": "UNRATE",
    "fred_gdp": "GDP",        
    "fred_indpro": "INDPRO",
    "fred_fedfunds": "FEDFUNDS",
    "fred_csushpisa": "CSUSHPISA",
    "fred_pcedg": "PCEDG",
    "fred_wheat": "PWHEAMTUSDM",
    "fred_corn": "PMAIZMTUSDM",
    "fred_soybean": "PSOYBUSDQ",
    "fred_ppi": "PPIACO",
    "fred_bdi": "BDI"
}

# 과거 몇 년 치 데이터를 가져올지 설정 (예: 10년 전인 2014-01-01부터)
OBSERVATION_START = "2014-01-01"

def fetch_historical_fred_values(series_id: str) -> list:
    """지정된 기간부터 현재까지의 모든 관측값을 반환합니다."""
    # API 호출 제한 방지 (Rate Limit 대응)
    time.sleep(1)
    
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&observation_start={OBSERVATION_START}&sort_order=asc"
    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        data = res.json()
        valid_obs = []
        if "observations" in data:
            for obs in data["observations"]:
                val = obs.get("value", ".")
                if val != ".":
                    valid_obs.append({"date": obs["date"], "value": float(val)})
        return valid_obs
    except Exception as e:
        print(f"⚠️ FRED API 조회 에러 ({series_id}): {e}")
    return []

def chunk_list(lst, n=100):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def backfill_daily_fred():
    print(f"📊 [일간 백필] {OBSERVATION_START} 이후 FRED 데일리 데이터 취합 중...")
    updates_by_date = {}
    
    for db_col, series_id in DAILY_SERIES.items():
        print(f"  - {db_col} ({series_id}) 다운로드 중...")
        results = fetch_historical_fred_values(series_id)
        for row in results:
            date_str = row["date"]
            if date_str not in updates_by_date:
                updates_by_date[date_str] = {"reference_date": date_str}
            updates_by_date[date_str][db_col] = row["value"]

    print(f"🚀 [일간 백필] 총 {len(updates_by_date)}일 치 데이터 수집 완료. Supabase 업로드 시작 (Chunk 단위)...")
    
    all_keys = ["reference_date"] + list(DAILY_SERIES.keys())
    upsert_list = []
    for row in updates_by_date.values():
        for k in all_keys:
            if k not in row:
                row[k] = None
        upsert_list.append(row)
        
    for chunk in chunk_list(upsert_list, 500):
        try:
            supabase.table("indicator_fred_daily_logs").upsert(chunk, on_conflict="reference_date").execute()
            print(f"✅ {len(chunk)}개 일간 로우 업로드 완료")
        except Exception as e:
            print(f"❌ Supabase Upsert 에러: {e}")

def backfill_monthly_fred():
    print(f"📊 [월간 백필] {OBSERVATION_START} 이후 FRED 먼슬리 데이터 취합 중...")
    updates_by_month = {}
    
    for db_col, series_id in MONTHLY_SERIES.items():
        print(f"  - {db_col} ({series_id}) 다운로드 중...")
        results = fetch_historical_fred_values(series_id)
        for row in results:
            # "YYYY-MM-DD" -> "YYYY-MM"
            month_str = row["date"][:7]
            if month_str not in updates_by_month:
                updates_by_month[month_str] = {"reference_month": month_str}
            updates_by_month[month_str][db_col] = row["value"]

    print(f"🚀 [월간 백필] 총 {len(updates_by_month)}개월 치 데이터 수집 완료. Supabase 업로드 시작...")
    
    all_keys = ["reference_month"] + list(MONTHLY_SERIES.keys())
    upsert_list = []
    for row in updates_by_month.values():
        for k in all_keys:
            if k not in row:
                row[k] = None
        upsert_list.append(row)
        
    for chunk in chunk_list(upsert_list, 500):
        try:
            supabase.table("indicator_fred_monthly_logs").upsert(chunk, on_conflict="reference_month").execute()
            print(f"✅ {len(chunk)}개 월간 로우 업로드 완료")
        except Exception as e:
            print(f"❌ Supabase Upsert 에러: {e}")

if __name__ == "__main__":
    backfill_daily_fred()
    backfill_monthly_fred()
    print("🎉 백필(초기 과거 데이터 적재) 세팅이 완벽하게 끝났습니다!")
