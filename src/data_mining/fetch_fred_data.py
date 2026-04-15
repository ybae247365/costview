import os
import sys
import requests
from datetime import datetime
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

if not FRED_API_KEY or not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ 누락된 환경 변수가 있습니다. (.env 파일에 FRED_API_KEY, SUPABASE_URL, SUPABASE_ANON_KEY 확인 필요)")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 수집할 지표의 FRED Series ID 정의
# WTI: DCOILWTICO, Brent: DCOILBRENTE, Dollar Index: DTWEXBGS, 10Y Treasury: DGS10, Supply Chain: GSCPI (또는 유사 proxy)
# 실제 GSCPI는 뉴욕 연방준비은행 지표라 FRED에 없을 수 있으나, 있으면 GSCPI로 조회, 없으면 추후 교체
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
    "fred_gdp": "GDP",        # 원래 분기 데이터이나 DB 편의상 Monthly 테이블에 저장
    "fred_indpro": "INDPRO",
    "fred_fedfunds": "FEDFUNDS",
    "fred_csushpisa": "CSUSHPISA",
    "fred_pcedg": "PCEDG",
    "fred_wheat": "PWHEAMTUSDM",
    "fred_corn": "PMAIZMTUSDM",
    "fred_soybean": "PSOYBUSDQ",
    "fred_ppi": "PPIACO",
    "fred_bdi": "BDI"         # BDI는 FRED 공식지원X일수 있으나 에러 스킵처리됨
}

def fetch_latest_fred_value(series_id: str) -> dict:
    """FRED API를 호출하여 해당 시리즈의 가장 최신 관측값 1개를 가져옵니다."""
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&sort_order=desc&limit=1"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        if "observations" in data and len(data["observations"]) > 0:
            obs = data["observations"][0]
            val = obs.get("value", ".")
            if val == ".":  # FRED에서 간혹 휴일/장마감 등으로 값이 결측치인 경우 '.' 로 옴
                return None
            return {"date": obs["date"], "value": float(val)}
    except Exception as e:
        print(f"⚠️ FRED API 조회 에러 ({series_id}): {e}")
    return None

def sync_daily_fred():
    """일별 FRED 지표들을 indicator_daily_logs 테이블에 Upsert 합니다."""
    print("🔍 [일간] FRED 매크로 지표 업데이트 시작...")
    updates_by_date = {}
    
    # 각 지표별로 최신 날짜의 데이터를 호출하여 날짜별로 그룹핑
    for db_col, series_id in DAILY_SERIES.items():
        result = fetch_latest_fred_value(series_id)
        if result:
            date_str = result["date"]
            if date_str not in updates_by_date:
                updates_by_date[date_str] = {"reference_date": date_str}
            updates_by_date[date_str][db_col] = result["value"]

    # Supabase Upsert 처리
    for date_str, row_data in updates_by_date.items():
        # ai_gpr_index 로직 삭제(분리된 테이블이므로)
        
        try:
            supabase.table("indicator_fred_daily_logs").upsert(row_data, on_conflict="reference_date").execute()
            print(f"✅ [Daily] {date_str}의 지표 업데이트 완료: {row_data}")
        except Exception as e:
            print(f"❌ [Daily] Supabase Upsert 에러 ({date_str}): {e}")

def sync_monthly_fred():
    """월별 FRED 지표들을 indicator_fred_monthly_logs 테이블에 Upsert 합니다."""
    print("🔍 [월간] FRED 매크로 지표 업데이트 시작...")
    updates_by_month = {}
    
    for db_col, series_id in MONTHLY_SERIES.items():
        result = fetch_latest_fred_value(series_id)
        if result:
            # 월간 데이터의 reference_date는 "YYYY-MM-DD" 로 오므로 "YYYY-MM"으로 자름
            month_str = result["date"][:7] 
            if month_str not in updates_by_month:
                updates_by_month[month_str] = {"reference_month": month_str}
            updates_by_month[month_str][db_col] = result["value"]

    for month_str, row_data in updates_by_month.items():
        try:
            supabase.table("indicator_fred_monthly_logs").upsert(row_data, on_conflict="reference_month").execute()
            print(f"✅ [Monthly] {month_str}의 지표 업데이트 완료: {row_data}")
        except Exception as e:
            print(f"❌ [Monthly] Supabase Upsert 에러 ({month_str}): {e}")

if __name__ == "__main__":
    sync_daily_fred()
    sync_monthly_fred()
