from __future__ import annotations

"""Load Guardian archive articles into raw_news."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import io
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, TypedDict

import httpx
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, StateGraph

FROM_DATE = "2016-08-01"  # Guardian archive starts from 1999-01-01
TO_DATE = datetime.now(timezone.utc).date().isoformat()
CONSUMER_CATEGORY = "소비재"
GUARDIAN_REQUEST_INTERVAL_SECONDS = 0.25
GUARDIAN_RETRY_LIMIT = 5
GUARDIAN_RETRY_BACKOFF_SECONDS = 5.0
GUARDIAN_MAX_RETRY_DELAY_SECONDS = 30.0
SEARCH_LABEL = "war_cost"

if sys.platform.startswith("win"):
    sys.stdout = io.TextIOWrapper(
        sys.stdout.detach(),
        encoding="utf-8",
        errors="replace",
        line_buffering=True,
        write_through=True,
    )

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from common.supabase_client import (
    create_sb,
    describe_supabase_config,
    is_supabase_configured,
)
from supabase_store import (
    fetch_consumer_keywords_sb,
    fetch_cost_categories_sb,
    upsert_history_sb,
)

GUARDIAN_API = "https://content.guardianapis.com/search"

WAR_TERMS = [
    "war",
    "conflict",
    "invasion",
    "military",
    "attack",
    "geopolitical",
    "ukraine",
    "russia",
    "gaza",
    "middle east",
]
ITEM_TERMS = [
    "oil",
    "energy",
    "gas",
    "food",
    "wheat",
    "grain",
    "shipping",
    "fuel",
    "commodity",
    "price",
    "cost",
    "inflation",
]
UP_TERMS = ["rise", "up", "increase", "jump", "surge", "soar", "high"]
UP_PATTERNS = [
    r"\brise(s|n)?\b",
    r"\bup\b",
    r"\bincrease(s|d)?\b",
    r"\bjump(s|ed)?\b",
    r"\bsurge(s|d)?\b",
    r"\bsoar(s|ed)?\b",
    r"\bhigh(er)?\b",
]
DOWN_TERMS = ["fall", "down", "decrease", "drop", "decline", "lower", "slump"]
DOWN_PATTERNS = [
    r"\bfall(s|en)?\b",
    r"\bdown\b",
    r"\bdecrease(s|d)?\b",
    r"\bdrop(s|ped)?\b",
    r"\bdecline(s|d)?\b",
    r"\blower(ed)?\b",
    r"\bslump(s|ed)?\b",
]
WAR_PATTERNS = [rf"\b{re.escape(term)}\b" for term in WAR_TERMS]
ITEM_PATTERNS = [rf"\b{re.escape(term)}(s)?\b" for term in ITEM_TERMS]
SENTENCE_SPLIT_RE = re.compile(r"(?<=[\.\!\?])\s+")


class PeriodTask(TypedDict):
    category_code: str
    category_name: str
    search_query: str
    from_date: str
    to_date: str


class PeriodResult(TypedDict):
    category_code: str
    category_name: str
    search_query: str
    from_date: str
    to_date: str
    fetched: int
    filtered: int
    rows: list[dict[str, Any]]
    skipped: bool
    skip_reason: str | None


class HistoryRunState(TypedDict, total=False):
    guardian_key: str
    args: argparse.Namespace
    use_supabase: bool
    conn: Any
    sb: Any
    domain_keywords: list[str]
    cost_categories: list[dict[str, Any]]
    date_ranges: list[tuple[str, str]]
    pending_tasks: list[PeriodTask]
    current_batch: list[PeriodTask]
    current_batch_results: list[PeriodResult]
    current_batch_saved: int
    total_tasks: int
    completed_tasks: int
    total_fetched: int
    total_saved: int
    category_saved_totals: dict[str, int]


def log(message: str) -> None:
    print(message, flush=True)


def load_env() -> None:
    here = Path(__file__).resolve().parent
    root = here.parent
    load_dotenv(here / ".env", override=True)
    load_dotenv(root / ".env", override=True)


def get_connection():
    load_env()
    if is_supabase_configured():
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_ANON_KEY are configured. Use create_sb() and supabase_store instead."
        )
    url = os.environ.get("API_KEY") or os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError(
            "Set API_KEY or DATABASE_URL, or configure SUPABASE_URL with SUPABASE_ANON_KEY."
        )
    if "sslmode" not in url:
        url += "?sslmode=require" if "?" not in url else "&sslmode=require"
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


def fetch_consumer_keywords(conn, limit: int = 50) -> list[str]:
    sql = """
        SELECT COALESCE(NULLIF(BTRIM(keyword_en), ''), NULLIF(BTRIM(keyword_kr), '')) AS keyword
        FROM consumer_items
        WHERE category_kr = %s
          AND COALESCE(is_deleted, false) = false
          AND COALESCE(NULLIF(BTRIM(keyword_en), ''), NULLIF(BTRIM(keyword_kr), '')) IS NOT NULL
        ORDER BY COALESCE(NULLIF(BTRIM(keyword_en), ''), NULLIF(BTRIM(keyword_kr), '')) ASC
        LIMIT %s;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (CONSUMER_CATEGORY, limit))
        rows = cur.fetchall()
    keywords = [str(row["keyword"]).strip() for row in rows if row.get("keyword")]
    log(f"[consumer_keywords] count={len(keywords)}")
    for i, keyword in enumerate(keywords, 1):
        log(f"  {i:3d}. {keyword}")
    return keywords


def fetch_cost_categories(conn, limit: int = 50) -> list[dict[str, Any]]:
    sql = """
        SELECT code, name_ko, name_en, group_code, keywords, sort_order
        FROM cost_categories
        WHERE is_active = true
        ORDER BY sort_order ASC, code ASC
        LIMIT %s;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    categories: list[dict[str, Any]] = []
    for row in rows:
        keywords = [
            str(keyword).strip()
            for keyword in (row.get("keywords") or [])
            if str(keyword).strip()
        ]
        if not keywords:
            continue
        categories.append(
            {
                "code": str(row.get("code") or "").strip(),
                "name_ko": str(row.get("name_ko") or "").strip(),
                "name_en": str(row.get("name_en") or "").strip(),
                "group_code": str(row.get("group_code") or "").strip(),
                "keywords": keywords,
                "sort_order": int(row.get("sort_order") or 0),
            }
        )

    log(f"[cost_categories] count={len(categories)}")
    for i, category in enumerate(categories, 1):
        log(f"  {i:3d}. {category['code']} keywords={len(category['keywords'])}")
    return categories


def build_guardian_query(cost_categories: list[dict[str, Any]]) -> str:
    # cost_categories 전체를 하나의 OR 검색식으로 합쳐 호출 수를 줄인다.
    category_terms: list[str] = []
    seen_terms: set[str] = set()
    for category in cost_categories:
        for keyword in category.get("keywords") or []:
            term = str(keyword).strip()
            if term and term.lower() not in seen_terms:
                seen_terms.add(term.lower())
                category_terms.append(term)

    if not category_terms:
        return "war"

    war_clause = " OR ".join(
        f'"{term}"' if " " in term else term for term in ["war"]
    )
    category_clause = " OR ".join(
        f'"{term}"' if " " in term else term for term in category_terms
    )
    return f"({war_clause}) AND ({category_clause})"


def guardian_search(
    api_key: str,
    query: str,
    from_date: str,
    to_date: str,
    page: int,
    page_size: int = 50,
) -> dict[str, Any]:
    params = {
        "api-key": api_key,
        "q": query,
        "from-date": from_date,
        "to-date": to_date,
        "page-size": page_size,
        "page": page,
        "order-by": "newest",
        "show-fields": "headline,trailText,bodyText",
    }
    with httpx.Client(timeout=20) as client:
        for attempt in range(1, GUARDIAN_RETRY_LIMIT + 1):
            try:
                res = client.get(GUARDIAN_API, params=params)
                if res.status_code == 429:
                    if attempt >= GUARDIAN_RETRY_LIMIT:
                        res.raise_for_status()
                    retry_after = res.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = max(
                                float(retry_after), GUARDIAN_RETRY_BACKOFF_SECONDS
                            )
                        except ValueError:
                            delay = GUARDIAN_RETRY_BACKOFF_SECONDS * (
                                2 ** (attempt - 1)
                            )
                    else:
                        delay = GUARDIAN_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    if delay > GUARDIAN_MAX_RETRY_DELAY_SECONDS:
                        raise RuntimeError(
                            "Guardian rate limit delay too large: "
                            f"keyword={query} period={from_date}~{to_date} "
                            f"page={page} retry_after={delay:.1f}s"
                        )
                    log(
                        f"[guardian] rate_limited keyword='{query}' "
                        f"period={from_date}~{to_date} page={page} "
                        f"attempt={attempt}/{GUARDIAN_RETRY_LIMIT} sleep={delay:.1f}s"
                    )
                    time.sleep(delay)
                    continue

                res.raise_for_status()
                time.sleep(GUARDIAN_REQUEST_INTERVAL_SECONDS)
                return res.json()
            except httpx.HTTPStatusError:
                raise
            except httpx.HTTPError as e:
                if attempt >= GUARDIAN_RETRY_LIMIT:
                    raise RuntimeError(
                        "Guardian request failed after retries: "
                        f"keyword={query} period={from_date}~{to_date} page={page}"
                    ) from e
                delay = GUARDIAN_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
                log(
                    f"[guardian] transient_error keyword='{query}' "
                    f"period={from_date}~{to_date} page={page} "
                    f"attempt={attempt}/{GUARDIAN_RETRY_LIMIT} sleep={delay:.1f}s error={e}"
                )
                time.sleep(delay)

    raise RuntimeError(
        "Guardian request exhausted retries: "
        f"keyword={query} period={from_date}~{to_date} page={page}"
    )


def normalize_item(item: dict[str, Any], keyword: str) -> dict[str, Any] | None:
    fields = item.get("fields") or {}
    title = (fields.get("headline") or item.get("webTitle") or "").strip()
    trail = (fields.get("trailText") or "").strip()
    body = (fields.get("bodyText") or "").strip()
    content = body or trail or title
    news_url = (item.get("webUrl") or "").strip()
    published = item.get("webPublicationDate")
    if not title or not content or not news_url:
        return None

    # 저장 전 단계에서 기사 내용을 한 번 더 분석해 전쟁/품목/방향성 신호를 만든다.
    extracted = extract_war_price_signals(title, content)
    directional = extract_directional_items(title, content)
    if not (
        bool(directional["increased_items"]) ^ bool(directional["decreased_items"])
    ):
        return None

    all_keywords = sorted(set(extracted["wars"] + extracted["items"]))
    increased = directional["increased_items"] or None
    decreased = directional["decreased_items"] or None

    return {
        "title": title[:1000],
        "content": content[:12000],
        "news_url": news_url,
        "origin_published_at": published,
        "keyword": all_keywords,
        "increased_items": increased,
        "decreased_items": decreased,
    }


def is_relevant_war_price_article(title: str, content: str) -> bool:
    text = f"{title}\n{content}".lower()
    has_war = any(re.search(pattern, text) for pattern in WAR_PATTERNS)
    has_item = any(re.search(pattern, text) for pattern in ITEM_PATTERNS)
    has_up_or_down = any(
        re.search(pattern, text) for pattern in UP_PATTERNS + DOWN_PATTERNS
    )
    return has_war and has_item and has_up_or_down


def extract_war_price_signals(title: str, content: str) -> dict[str, list[str]]:
    text = f"{title}\n{content}".lower()
    wars = sorted(
        {term for term in WAR_TERMS if re.search(rf"\b{re.escape(term)}\b", text)}
    )
    items = sorted(
        {term for term in ITEM_TERMS if re.search(rf"\b{re.escape(term)}(s)?\b", text)}
    )
    up_signals = sorted(
        {
            term
            for term, pattern in zip(UP_TERMS, UP_PATTERNS)
            if re.search(pattern, text)
        }
    )
    down_signals = sorted(
        {
            term
            for term, pattern in zip(DOWN_TERMS, DOWN_PATTERNS)
            if re.search(pattern, text)
        }
    )
    return {
        "wars": wars,
        "items": items,
        "up_signals": up_signals,
        "down_signals": down_signals,
    }


def extract_directional_items(title: str, content: str) -> dict[str, list[str]]:
    text = f"{title}. {content}".lower()
    sentences = [
        sentence.strip()
        for sentence in SENTENCE_SPLIT_RE.split(text)
        if sentence.strip()
    ]

    increased: set[str] = set()
    decreased: set[str] = set()
    up_hits = 0
    down_hits = 0

    # 문장 단위로 방향성과 품목이 함께 등장하는 경우만 유효 신호로 본다.
    for sentence in sentences:
        has_up = any(re.search(pattern, sentence) for pattern in UP_PATTERNS)
        has_down = any(re.search(pattern, sentence) for pattern in DOWN_PATTERNS)
        if not (has_up or has_down):
            continue

        sentence_items = {
            term
            for term in ITEM_TERMS
            if re.search(rf"\b{re.escape(term)}(s)?\b", sentence)
        }
        if not sentence_items:
            continue

        if has_up and not has_down:
            increased.update(sentence_items)
            up_hits += 1
        elif has_down and not has_up:
            decreased.update(sentence_items)
            down_hits += 1
        else:
            continue

    overlap = increased & decreased
    if overlap:
        increased -= overlap
        decreased -= overlap

    if increased and decreased:
        if up_hits > down_hits:
            decreased.clear()
        elif down_hits > up_hits:
            increased.clear()
        else:
            if len(increased) > len(decreased):
                decreased.clear()
            elif len(decreased) > len(increased):
                increased.clear()
            else:
                increased.clear()
                decreased.clear()

    return {
        "increased_items": sorted(increased),
        "decreased_items": sorted(decreased),
    }


def upsert_history(conn, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    db_rows = []
    skipped_conflicted = 0
    for item in rows:
        row = dict(item)
        has_increased = row.get("increased_items") is not None
        has_decreased = row.get("decreased_items") is not None
        if not (has_increased ^ has_decreased):
            skipped_conflicted += 1
            continue
        db_rows.append(row)

    if skipped_conflicted:
        print(f"[history] skipped_conflicted_direction={skipped_conflicted}")
    if not db_rows:
        return 0

    sql = """
        INSERT INTO raw_news
            (title, content, news_url, origin_published_at, keyword, increased_items, decreased_items)
        SELECT
            %(title)s, %(content)s, %(news_url)s, %(origin_published_at)s, %(keyword)s,
            %(increased_items)s, %(decreased_items)s
        WHERE (%(increased_items)s IS NOT NULL) != (%(decreased_items)s IS NOT NULL)
        ON CONFLICT (news_url) DO UPDATE SET
            title = EXCLUDED.title,
            content = EXCLUDED.content,
            origin_published_at = EXCLUDED.origin_published_at,
            keyword = (
                SELECT ARRAY(
                    SELECT DISTINCT x
                    FROM unnest(raw_news.keyword || EXCLUDED.keyword) AS t(x)
                )
            ),
            increased_items = EXCLUDED.increased_items,
            decreased_items = EXCLUDED.decreased_items,
            updated_at = NOW();
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, db_rows, page_size=100)
    conn.commit()
    return len(db_rows)


def parse_args() -> argparse.Namespace:
    now = datetime.now(timezone.utc).date()
    default_from = FROM_DATE if FROM_DATE else (now - timedelta(days=365)).isoformat()
    default_to = TO_DATE if TO_DATE else now.isoformat()
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-date", default=default_from, help="YYYY-MM-DD")
    parser.add_argument("--to-date", default=default_to, help="YYYY-MM-DD")
    parser.add_argument(
        "--days", type=int, default=365, help="Legacy option kept for compatibility"
    )
    parser.add_argument("--max-pages", type=int, default=2, help="Max pages per month")
    parser.add_argument(
        "--page-size", type=int, default=50, help="Articles per page (max 50)"
    )
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=1,
        help="Parallel month fetch workers per keyword batch",
    )
    return parser.parse_args()


def iter_month_ranges(from_date: str, to_date: str) -> list[tuple[str, str]]:
    start_date = datetime.fromisoformat(from_date).date()
    end_date = datetime.fromisoformat(to_date).date()
    if start_date > end_date:
        raise ValueError("from-date must be less than or equal to to-date")

    ranges: list[tuple[str, str]] = []
    current = start_date
    while current <= end_date:
        if current.month == 12:
            next_month_start = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month_start = current.replace(month=current.month + 1, day=1)

        range_end = min(end_date, next_month_start - timedelta(days=1))
        ranges.append((current.isoformat(), range_end.isoformat()))
        current = next_month_start

    return ranges


def _build_pending_tasks(
    date_ranges: list[tuple[str, str]], search_query: str
) -> list[PeriodTask]:
    tasks: list[PeriodTask] = []
    # 검색식은 하나로 유지하고, 기간만 월 단위 task로 나눈다.
    for from_date, to_date in date_ranges:
        tasks.append(
            {
                "category_code": SEARCH_LABEL,
                "category_name": SEARCH_LABEL,
                "search_query": search_query,
                "from_date": from_date,
                "to_date": to_date,
            }
        )
    return tasks


def _fetch_rows_for_period_task(
    task: PeriodTask, guardian_key: str, args: argparse.Namespace
) -> PeriodResult:
    category_code = task["category_code"]
    category_name = task["category_name"]
    search_query = task["search_query"]
    from_date = task["from_date"]
    to_date = task["to_date"]
    log(
        f"[history] fetch:start category='{category_code}' period={from_date}~{to_date} "
        f"max_pages={args.max_pages} page_size={args.page_size}"
    )

    keyword_rows: list[dict[str, Any]] = []
    range_fetched = 0
    # 한 period 안에서는 Guardian 페이지를 순차적으로 넘기면서 최대 max_pages까지만 수집한다.
    for page in range(1, args.max_pages + 1):
        try:
            data = guardian_search(
                api_key=guardian_key,
                query=search_query,
                from_date=from_date,
                to_date=to_date,
                page=page,
                page_size=args.page_size,
            )
        except RuntimeError as e:
            error_text = str(e)
            if "rate limit delay too large" in error_text:
                log(
                    f"[history] skipped_due_to_rate_limit category='{category_code}' "
                    f"period={from_date}~{to_date} page={page}"
                )
                return {
                    "category_code": category_code,
                    "category_name": category_name,
                    "search_query": search_query,
                    "from_date": from_date,
                    "to_date": to_date,
                    "fetched": range_fetched,
                    "filtered": len(keyword_rows),
                    "rows": [],
                    "skipped": True,
                    "skip_reason": "rate_limit",
                }
            raise
        response = data.get("response") or {}
        results = response.get("results") or []
        if not results:
            break

        range_fetched += len(results)
        for item in results:
            row = normalize_item(item, category_code)
            if row and is_relevant_war_price_article(row["title"], row["content"]):
                keyword_rows.append(row)

        pages = int(response.get("pages") or 1)
        if page >= pages:
            break

    return {
        "category_code": category_code,
        "category_name": category_name,
        "search_query": search_query,
        "from_date": from_date,
        "to_date": to_date,
        "fetched": range_fetched,
        "filtered": len(keyword_rows),
        "rows": keyword_rows,
        "skipped": False,
        "skip_reason": None,
    }


def _save_rows_for_batch(state: HistoryRunState) -> dict[str, Any]:
    current_batch_results = state.get("current_batch_results", [])
    batch_rows = [row for result in current_batch_results for row in result["rows"]]
    if state["use_supabase"]:
        saved = upsert_history_sb(state["sb"], batch_rows)
    else:
        saved = upsert_history(state["conn"], batch_rows)

    category_saved_totals = dict(state.get("category_saved_totals", {}))
    saved_by_category: dict[str, int] = {}
    # 현재는 통합 검색이지만, 집계 구조는 이후 다중 검색식으로 다시 넓혀도 재사용 가능하게 둔다.
    for result in current_batch_results:
        category_code = result["category_code"]
        saved_by_category[category_code] = (
            saved_by_category.get(category_code, 0) + len(result["rows"])
        )
    for category_code, category_saved in saved_by_category.items():
        category_saved_totals[category_code] = (
            category_saved_totals.get(category_code, 0) + category_saved
        )

    return {
        "current_batch_saved": saved,
        "total_saved": state.get("total_saved", 0) + saved,
        "category_saved_totals": category_saved_totals,
    }


def _fetch_rows_for_batch(state: HistoryRunState) -> dict[str, Any]:
    current_batch = state.get("current_batch", [])
    if not current_batch:
        return {"current_batch_results": []}

    # 월별 task 묶음을 ThreadPool로 병렬 처리한다.
    max_workers = min(max(1, state["args"].parallel_workers), len(current_batch))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(
            executor.map(
                lambda task: _fetch_rows_for_period_task(
                    task, state["guardian_key"], state["args"]
                ),
                current_batch,
            )
        )

    return {
        "current_batch_results": results,
        "total_fetched": state.get("total_fetched", 0)
        + sum(result["fetched"] for result in results),
    }


fetch_rows_runnable = RunnableLambda(_fetch_rows_for_batch)
save_rows_runnable = RunnableLambda(_save_rows_for_batch)


def init_run_node(_: HistoryRunState) -> dict[str, Any]:
    load_env()
    log("[history] startup")

    guardian_key = os.environ["GUARDIAN_API_KEY"]
    args = parse_args()
    use_supabase = is_supabase_configured()
    conn = None
    sb = None

    if use_supabase:
        info = describe_supabase_config()
        log(
            "[history] supabase_config "
            f"host={info['host']} path={info['path']} "
            f"has_key={info['has_key']} key_prefix={info['key_prefix']} "
            f"dns_443={info['dns_443']} tcp_443={info['tcp_443']} "
            f"proxies={info['proxies']}"
        )
        log("[history] creating supabase client")
        sb = create_sb()
        log("[history] DB: Supabase (SUPABASE_URL + SUPABASE_ANON_KEY)")
        log("[history] fetching consumer keywords from supabase")
        domain_keywords = fetch_consumer_keywords_sb(sb, limit=50)
        log("[history] fetching cost categories from supabase")
        cost_categories = fetch_cost_categories_sb(sb, limit=50)
    else:
        log("[history] opening postgres connection")
        conn = get_connection()
        log("[history] DB: Postgres (API_KEY / DATABASE_URL)")
        log("[history] fetching consumer keywords from postgres")
        domain_keywords = fetch_consumer_keywords(conn, limit=50)
        log("[history] fetching cost categories from postgres")
        cost_categories = fetch_cost_categories(conn, limit=50)

    if not domain_keywords:
        log("[history] no consumer domain keywords found; exiting")
    if not cost_categories:
        log("[history] no cost categories found; exiting")

    date_ranges = iter_month_ranges(args.from_date, args.to_date)
    search_query = build_guardian_query(cost_categories)
    log(f"[history] range: {args.from_date} ~ {args.to_date}")
    log(f"[history] consumer domain keyword count: {len(domain_keywords)}")
    log(f"[history] cost category count: {len(cost_categories)}")
    log(f"[history] search query: {search_query}")
    log(f"[history] monthly task count: {len(date_ranges)} (parallel_workers={args.parallel_workers})")

    return {
        "guardian_key": guardian_key,
        "args": args,
        "use_supabase": use_supabase,
        "conn": conn,
        "sb": sb,
        "domain_keywords": domain_keywords,
        "cost_categories": cost_categories,
        "date_ranges": date_ranges,
        "pending_tasks": _build_pending_tasks(date_ranges, search_query),
        "current_batch": [],
        "current_batch_results": [],
        "current_batch_saved": 0,
        "total_tasks": len(date_ranges),
        "completed_tasks": 0,
        "total_fetched": 0,
        "total_saved": 0,
        "category_saved_totals": {},
    }


def prepare_batch_node(state: HistoryRunState) -> dict[str, Any]:
    pending_tasks = list(state.get("pending_tasks", []))
    if not pending_tasks:
        return {
            "current_batch": [],
            "pending_tasks": [],
            "current_batch_results": [],
            "current_batch_saved": 0,
        }

    max_batch_size = max(1, state["args"].parallel_workers)
    # pending queue 앞부분부터 병렬 worker 수만큼 잘라 현재 배치를 만든다.
    current_batch = pending_tasks[:max_batch_size]
    pending_tasks = pending_tasks[max_batch_size:]

    batch_categories = sorted({task["category_code"] for task in current_batch})
    batch_periods = sorted(
        {f"{task['from_date']}~{task['to_date']}" for task in current_batch}
    )
    category_preview = ",".join(batch_categories[:4])
    if len(batch_categories) > 4:
        category_preview += ",..."
    period_preview = ",".join(batch_periods[:2])
    if len(batch_periods) > 2:
        period_preview += ",..."

    log(
        f"[graph] prepare_batch tasks={len(current_batch)} "
        f"categories=[{category_preview}] periods=[{period_preview}] "
        f"remaining={len(pending_tasks)}"
    )
    return {
        "current_batch": current_batch,
        "pending_tasks": pending_tasks,
        "current_batch_results": [],
        "current_batch_saved": 0,
    }


def fetch_batch_parallel_node(state: HistoryRunState) -> dict[str, Any]:
    return fetch_rows_runnable.invoke(state)


def persist_batch_node(state: HistoryRunState) -> dict[str, Any]:
    return save_rows_runnable.invoke(state)


def log_batch_progress_node(state: HistoryRunState) -> dict[str, Any]:
    current_batch_results = state.get("current_batch_results", [])
    if not current_batch_results:
        return {}

    for result in current_batch_results:
        if result["skipped"]:
            log(
                f"[history] period_skipped category='{result['category_code']}' "
                f"period={result['from_date']}~{result['to_date']} "
                f"reason={result['skip_reason']}"
            )
        else:
            log(
                f"[history] period_done category='{result['category_code']}' "
                f"period={result['from_date']}~{result['to_date']} "
                f"fetched={result['fetched']} filtered={result['filtered']}"
            )

    completed_tasks = state.get("completed_tasks", 0) + len(current_batch_results)
    batch_categories = sorted({result["category_code"] for result in current_batch_results})
    category_preview = ",".join(batch_categories[:4])
    if len(batch_categories) > 4:
        category_preview += ",..."
    log(
        f"[history] batch_done categories=[{category_preview}] "
        f"periods={len(current_batch_results)} "
        f"saved={state.get('current_batch_saved', 0)} "
        f"progress={completed_tasks}/{state.get('total_tasks', 0)} "
        f"total_fetched={state.get('total_fetched', 0)} "
        f"total_saved={state.get('total_saved', 0)}"
    )
    return {"completed_tasks": completed_tasks}


def log_completion_node(state: HistoryRunState) -> dict[str, Any]:
    war_total = state.get("category_saved_totals", {}).get("war_cost", 0)
    log(f"[history] category='war_cost' saved={war_total}")
    log(
        f"[history] done fetched={state.get('total_fetched', 0)} "
        f"saved={state.get('total_saved', 0)}"
    )
    return {}


def has_pending_batches(state: HistoryRunState) -> str:
    return "process" if state.get("pending_tasks") else "complete"


def build_history_graph():
    # bootstrap -> batch 준비 -> fetch -> 저장 -> 진행 로그 -> 다음 batch 반복
    graph = StateGraph(HistoryRunState)
    graph.add_node("bootstrap_run", init_run_node)
    graph.add_node("prepare_batch", prepare_batch_node)
    graph.add_node("fetch_batch_parallel", fetch_batch_parallel_node)
    graph.add_node("persist_batch", persist_batch_node)
    graph.add_node("log_batch_progress", log_batch_progress_node)
    graph.add_node("finalize_run", log_completion_node)

    graph.set_entry_point("bootstrap_run")
    graph.add_edge("bootstrap_run", "prepare_batch")
    graph.add_conditional_edges(
        "prepare_batch",
        has_pending_batches,
        {
            "process": "fetch_batch_parallel",
            "complete": "finalize_run",
        },
    )
    graph.add_edge("fetch_batch_parallel", "persist_batch")
    graph.add_edge("persist_batch", "log_batch_progress")
    graph.add_edge("log_batch_progress", "prepare_batch")
    graph.add_edge("finalize_run", END)

    return graph.compile()


def main() -> None:
    app = build_history_graph()
    try:
        state = app.invoke({})
    finally:
        # Postgres 모드에서는 연결을 명시적으로 닫고 종료한다.
        conn = locals().get("state", {}).get("conn") if "state" in locals() else None
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
