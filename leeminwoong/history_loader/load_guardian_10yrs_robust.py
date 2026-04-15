import os
import sys
import time
import httpx
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# 기존에 있던 내부 모듈 경로 추가 (leeminwoong 폴더 루트 인식용)
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Supabase 클라이언트 임포트
from common.supabase_client import create_sb, is_supabase_configured
from supabase_store import fetch_consumer_keywords_sb, upsert_history_sb

# 한글 깨짐 방지
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

import re

GUARDIAN_API = "https://content.guardianapis.com/search"

# 기존 키워드 로직 (그대로 유지)
WAR_TERMS = ["war", "conflict", "invasion", "military", "attack", "geopolitical", "ukraine", "russia", "gaza", "middle east"]
ITEM_TERMS = ["oil", "energy", "gas", "food", "wheat", "grain", "shipping", "fuel", "commodity", "price", "cost", "inflation"]
UP_TERMS = ["rise", "up", "increase", "jump", "surge", "soar", "high"]
UP_PATTERNS = [r"\brise(s|n)?\b", r"\bup\b", r"\bincrease(s|d)?\b", r"\bjump(s|ed)?\b", r"\bsurge(s|d)?\b", r"\bsoar(s|ed)?\b", r"\bhigh(er)?\b"]
DOWN_TERMS = ["fall", "down", "decrease", "drop", "decline", "lower", "slump"]
DOWN_PATTERNS = [r"\bfall(s|en)?\b", r"\bdown\b", r"\bdecrease(s|d)?\b", r"\bdrop(s|ped)?\b", r"\bdecline(s|d)?\b", r"\blower(ed)?\b", r"\bslump(s|ed)?\b"]
WAR_PATTERNS = [rf"\b{re.escape(t)}\b" for t in WAR_TERMS]
ITEM_PATTERNS = [rf"\b{re.escape(t)}(s)?\b" for t in ITEM_TERMS]
SENTENCE_SPLIT_RE = re.compile(r"(?<=[\.\!\?])\s+")

def extract_war_price_signals(title: str, content: str) -> dict:
    text = f"{title}\n{content}".lower()
    wars = sorted({t for t in WAR_TERMS if re.search(rf"\b{re.escape(t)}\b", text)})
    items = sorted({t for t in ITEM_TERMS if re.search(rf"\b{re.escape(t)}(s)?\b", text)})
    up_signals = sorted({t for t, p in zip(UP_TERMS, UP_PATTERNS) if re.search(p, text)})
    down_signals = sorted({t for t, p in zip(DOWN_TERMS, DOWN_PATTERNS) if re.search(p, text)})
    return {"wars": wars, "items": items, "up_signals": up_signals, "down_signals": down_signals}

def extract_directional_items(title: str, content: str) -> dict:
    text = f"{title}. {content}".lower()
    sentences = [s.strip() for s in SENTENCE_SPLIT_RE.split(text) if s.strip()]
    increased = set()
    decreased = set()
    up_hits = down_hits = 0

    for sent in sentences:
        has_up = any(re.search(p, sent) for p in UP_PATTERNS)
        has_down = any(re.search(p, sent) for p in DOWN_PATTERNS)
        if not (has_up or has_down): continue
        sent_items = {t for t in ITEM_TERMS if re.search(rf"\b{re.escape(t)}(s)?\b", sent)}
        if not sent_items: continue
        
        if has_up and not has_down:
            increased.update(sent_items); up_hits += 1
        elif has_down and not has_up:
            decreased.update(sent_items); down_hits += 1

    overlap = increased & decreased
    if overlap:
        increased -= overlap
        decreased -= overlap

    if increased and decreased:
        if up_hits > down_hits: decreased.clear()
        elif down_hits > up_hits: increased.clear()
        else:
            if len(increased) > len(decreased): decreased.clear()
            elif len(decreased) > len(increased): increased.clear()
            else:
                increased.clear()
                decreased.clear()

    return {"increased_items": sorted(increased), "decreased_items": sorted(decreased)}

def is_relevant_war_price_article(title: str, content: str) -> bool:
    text = f"{title}\n{content}".lower()
    has_war = any(re.search(p, text) for p in WAR_PATTERNS)
    has_item = any(re.search(p, text) for p in ITEM_PATTERNS)
    has_up_or_down = any(re.search(p, text) for p in UP_PATTERNS + DOWN_PATTERNS)
    return has_war and has_item and has_up_or_down

def normalize_item(item: dict, keyword: str) -> dict:
    fields = item.get("fields") or {}
    title = (fields.get("headline") or item.get("webTitle") or "").strip()
    trail = (fields.get("trailText") or "").strip()
    body = (fields.get("bodyText") or "").strip()
    content = body or trail or title
    news_url = (item.get("webUrl") or "").strip()
    published = item.get("webPublicationDate")
    if not title or not content or not news_url:
        return None
    
    extracted = extract_war_price_signals(title, content)
    directional = extract_directional_items(title, content)
    if not (bool(directional["increased_items"]) ^ bool(directional["decreased_items"])):
        return None
        
    all_keywords = sorted(set(extracted["wars"] + extracted["items"]))
    return {
        "title": title[:1000],
        "content": content[:12000],
        "news_url": news_url,
        "origin_published_at": published,
        "keyword": all_keywords,
        "increased_items": directional["increased_items"] if directional["increased_items"] else None,
        "decreased_items": directional["decreased_items"] if directional["decreased_items"] else None,
    }

# ==========================================
# 핵심: 429 에러를 방지하는 Retry 포함 통신 함수
# ==========================================
def guardian_search_with_retry(api_key: str, query: str, from_date: str, to_date: str, page: int, page_size: int = 50) -> dict:
    params = {
        "api-key": api_key, "q": query, "from-date": from_date, "to-date": to_date,
        "page-size": page_size, "page": page, "order-by": "newest", "show-fields": "headline,trailText,bodyText"
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 💡 [핵심] API 한도(Rate Limit) 초과를 막기 위해 매 요청마다 1.5초 대기
            time.sleep(1.5)
            
            with httpx.Client(timeout=30) as client:
                res = client.get(GUARDIAN_API, params=params)
                
                if res.status_code == 429:
                    print(f"  ⚠️ HTTP 429 (Too Many Requests). 10초 휴식 후 재시도... ({attempt+1}/{max_retries})")
                    time.sleep(10)
                    continue
                
                res.raise_for_status()
                return res.json()
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                print(f"  ⚠️ HTTP 429 (Too Many Requests). 10초 휴식 후 재시도... ({attempt+1}/{max_retries})")
                time.sleep(10)
            else:
                print(f"  ❌ HTTP 에러: {e}")
                break
        except Exception as e:
            print(f"  ❌ 예상치 못한 에러: {e}")
            time.sleep(5)
            
    return {}

# 10년을 한 달 단위로 쪼개는 함수
def generate_month_ranges(start_date: str, end_date: str):
    ranges = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current < end:
        # 다음달 1일 구하기 계산
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        chunk_end = next_month - timedelta(days=1)
        if chunk_end > end:
            chunk_end = end
        ranges.append((current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        current = next_month
    return ranges

def main():
    root = Path(__file__).resolve().parents[2]
    load_dotenv(root / ".env", override=True)
    guardian_key = os.environ.get("GUARDIAN_API_KEY")
    
    if not is_supabase_configured():
        print("❌ Supabase 환경변수가 없습니다.")
        return
        
    sb = create_sb()
    keywords = fetch_consumer_keywords_sb(sb, limit=50)
    if not keywords:
        print("[history] 소비재 키워드가 없습니다.")
        return

    # 10년 기간 셋팅 (예: 2016-01-01 ~ 오늘)
    FROM_DATE = "2016-01-01"
    TO_DATE = datetime.now(timezone.utc).date().isoformat()
    MAX_PAGES_PER_MONTH = 5  # 달마다 읽어들일 최대 페이지

    print(f"🚀 [10년 백필 시작] {FROM_DATE} ~ {TO_DATE}")
    month_chunks = generate_month_ranges(FROM_DATE, TO_DATE)
    
    total_fetched = 0
    total_saved = 0

    for kw in keywords:
        print(f"\n🔑 키워드 탐색 시작: '{kw}'")
        for chunk_start, chunk_end in month_chunks:
            keyword_rows = []
            
            for page in range(1, MAX_PAGES_PER_MONTH + 1):
                data = guardian_search_with_retry(
                    api_key=guardian_key, query=kw,
                    from_date=chunk_start, to_date=chunk_end, page=page, page_size=50
                )
                
                response = data.get("response") or {}
                results = response.get("results") or []
                if not results:
                    break

                for item in results:
                    row = normalize_item(item, kw)
                    if row and is_relevant_war_price_article(row["title"], row["content"]):
                        keyword_rows.append(row)

                pages = int(response.get("pages") or 1)
                if page >= pages:
                    break
                    
            if keyword_rows:
                # Supabase 저장 (덩어리가 크면 supabase가 502 에러를 내므로 100개씩 분할)
                for i in range(0, len(keyword_rows), 100):
                    chunk_to_save = keyword_rows[i:i+100]
                    try:
                        saved = upsert_history_sb(sb, chunk_to_save)
                        total_saved += saved
                    except Exception as e:
                        print(f"  ❌ Supabase 업로드 실패 ({chunk_start}): {e}")
                
            total_fetched += len(results) if 'results' in locals() else 0
            print(f"   📅 {chunk_start} ~ {chunk_end} 완료 (누적 저장: {total_saved}개)")

    print(f"\n🎉 전체 10년 치 뉴스 백필 수집이 종료되었습니다! 총 {total_saved}개 저장됨.")

if __name__ == "__main__":
    main()
