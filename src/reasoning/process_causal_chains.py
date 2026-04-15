import os
import sys
import json
from dotenv import load_dotenv
from supabase import create_client, Client
import time

try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

# STEP 1에서 만든 프롬프트 엔진 가져오기
from reasoning_agent import analyze_impact

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ 환경 변수 오류: SUPABASE_URL, SUPABASE_ANON_KEY 확인 필요")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def process_latest_news_to_causal_chains():
    print("🔍 [STEP 2] Supabase에서 최근 분석되지 않은 뉴스 데이터를 가져옵니다...")
    
    # 1. 원본 뉴스 가져오기 (가장 최근 5개만 샘플 처리)
    # 실제 프로덕션에서는 is_analyzed 같은 플래그 컬럼을 만들어 필터링하는 것이 좋습니다.
    res_news = supabase.table("raw_news").select("*").order("created_at", desc=True).limit(5).execute()
    news_data = res_news.data
    
    if not news_data:
        print("📭 처리할 뉴스가 없습니다.")
        return

    # 2. 최신 FRED 지표 가져오기 (WTI, GPR 등)
    # 여기서는 간단히 최신 1개 로우만 가져옵니다.
    res_fred = supabase.table("indicator_fred_daily_logs").select("*").order("reference_date", desc=True).limit(1).execute()
    fred_latest = res_fred.data[0] if res_fred.data else {"fred_wti": 0.0}

    print(f"총 {len(news_data)}개의 뉴스를 Reasoning Engine(Claude/Gemini)에 통과시킵니다...\n")

    for idx, news in enumerate(news_data, start=1):
        title = news.get("title", "")
        content = news.get("content", "")
        news_url = news.get("news_url", "")
        
        print(f"[{idx}/{len(news_data)}] '{title[:30]}...' 분석 중...")
        
        # 3. LLM에게 분석 요청 (STEP 1 함수 호출)
        # GPR(지정학적 리스크 지수)은 가상의 지표 또는 DB 값으로 대체 가능합니다.
        result_json = analyze_impact(
            news_title=title,
            news_content=content,
            wti_price=fred_latest.get("fred_wti", 80.0),
            gpr_index=150.0  
        )
        
        if result_json and "category" in result_json:
            # 4. JSON 결과를 Supabase `causal_chains` 테이블에 적재
            insert_payload = {
                "category": result_json.get("category", "Energy"),
                "tags": result_json.get("tags", []),
                "title": result_json.get("title", title),
                "description": result_json.get("description", ""),
                "raw_shock_percent": result_json.get("metrics", {}).get("raw_shock_percent", 0),
                "wallet_hit_percent": result_json.get("metrics", {}).get("wallet_hit_percent", 0),
                "transmission_time_months": result_json.get("metrics", {}).get("transmission_time_months", 0),
                "source_news_url": news_url
            }
            
            try:
                supabase.table("causal_chains").insert(insert_payload).execute()
                print(f"  👉 분석 완료 및 DB 저장 성공! (카테고리: {insert_payload['category']})")
            except Exception as e:
                print(f"  ❌ DB 저장 오류: {e}")
        else:
            print("  ⚠️ LLM 추론 실패 또는 모델 로드 에러로 건너뜁니다.")
        
        # API Rate Limit 제한 보호
        time.sleep(2)

if __name__ == "__main__":
    process_latest_news_to_causal_chains()
    print("\n🎉 모든 추론 파이프라인 작업이 완료되었습니다!")
