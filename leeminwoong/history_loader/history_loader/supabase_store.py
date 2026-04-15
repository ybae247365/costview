# history_loader/supabase_store.py - PostgREST helpers for Guardian raw_news
from __future__ import annotations

from typing import Any

CONSUMER_CATEGORY = "소비재"
BATCH_SIZE = 200


def log(message: str) -> None:
    print(message, flush=True)


def _execute_or_raise(query: Any, *, action: str) -> Any:
    try:
        return query.execute()
    except Exception as e:
        message = str(e).strip() or e.__class__.__name__
        raise RuntimeError(
            f"Supabase PostgREST request failed during {action}: {message}\n"
            "Check SUPABASE_URL format, API key validity, and whether the response is HTML instead of JSON."
        ) from e


def fetch_consumer_keywords_sb(sb: Any, limit: int = 50) -> list[str]:
    log(
        f"[history] fetch_consumer_keywords_sb start category={CONSUMER_CATEGORY} limit={limit}"
    )
    res = _execute_or_raise(
        sb.table("consumer_items")
        .select("keyword_kr,keyword_en")
        .eq("category_kr", CONSUMER_CATEGORY)
        .or_("is_deleted.is.null,is_deleted.eq.false")
        .order("keyword_kr")
        .limit(min(limit * 3, 500)),
        action="select consumer_items",
    )
    seen: set[str] = set()
    out: list[str] = []
    for r in res.data or []:
        keyword = (r.get("keyword_en") or "").strip() or (r.get("keyword_kr") or "").strip()
        if keyword and keyword not in seen:
            seen.add(keyword)
            out.append(keyword)
        if len(out) >= limit:
            break
    log(f"[consumer_keywords] count={len(out)}")
    for i, keyword in enumerate(out, 1):
        log(f"  {i:3d}. {keyword}")
    return out


def fetch_cost_categories_sb(sb: Any, limit: int = 50) -> list[dict[str, Any]]:
    log(f"[history] fetch_cost_categories_sb start limit={limit}")
    res = _execute_or_raise(
        sb.table("cost_categories")
        .select("code,name_ko,name_en,group_code,keywords,sort_order")
        .eq("is_active", True)
        .order("sort_order")
        .limit(limit),
        action="select cost_categories",
    )
    rows: list[dict[str, Any]] = []
    for row in res.data or []:
        keywords = [str(keyword).strip() for keyword in (row.get("keywords") or []) if str(keyword).strip()]
        if not keywords:
            continue
        rows.append(
            {
                "code": str(row.get("code") or "").strip(),
                "name_ko": str(row.get("name_ko") or "").strip(),
                "name_en": str(row.get("name_en") or "").strip(),
                "group_code": str(row.get("group_code") or "").strip(),
                "keywords": keywords,
                "sort_order": int(row.get("sort_order") or 0),
            }
        )
    log(f"[cost_categories] count={len(rows)}")
    for i, row in enumerate(rows, 1):
        log(f"  {i:3d}. {row['code']} keywords={len(row['keywords'])}")
    return rows


def _iso(value: Any) -> Any:
    if value is None:
        return None
    from datetime import datetime, timezone

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value


def _chunked(values: list[Any], size: int) -> list[list[Any]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def _merge_keywords(*keyword_lists: Any) -> list[str]:
    merged: set[str] = set()
    for keywords in keyword_lists:
        for keyword in keywords or []:
            keyword_text = str(keyword).strip()
            if keyword_text:
                merged.add(keyword_text)
    return sorted(merged)


def _merge_row_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    base = dict(rows[-1])
    base["keyword"] = _merge_keywords(*(row.get("keyword") for row in rows))
    return base


def _normalize_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    grouped_by_url: dict[str, list[dict[str, Any]]] = {}
    skipped_conflicted = 0

    for item in rows:
        row = dict(item)
        has_increased = row.get("increased_items") is not None
        has_decreased = row.get("decreased_items") is not None
        if not (has_increased ^ has_decreased):
            skipped_conflicted += 1
            continue

        row["origin_published_at"] = _iso(row.get("origin_published_at"))
        row["keyword"] = _merge_keywords(row.get("keyword"))
        grouped_by_url.setdefault(row["news_url"], []).append(row)

    normalized_rows = [_merge_row_group(group) for group in grouped_by_url.values()]
    return normalized_rows, skipped_conflicted


def _fetch_existing_keywords(sb: Any, news_urls: list[str]) -> dict[str, list[str]]:
    existing_keywords: dict[str, list[str]] = {}
    for chunk in _chunked(news_urls, BATCH_SIZE):
        response = _execute_or_raise(
            sb.table("raw_news").select("news_url,keyword").in_("news_url", chunk),
            action=f"select existing raw_news batch_size={len(chunk)}",
        )
        for row in response.data or []:
            news_url = str(row.get("news_url") or "").strip()
            if news_url:
                existing_keywords[news_url] = _merge_keywords(row.get("keyword"))
    return existing_keywords


def upsert_history_sb(sb: Any, rows: list[dict[str, Any]]) -> int:
    """raw_news: news_url, origin_published_at, keyword[], increased_items, decreased_items"""
    if not rows:
        return 0

    normalized_rows, skipped_conflicted = _normalize_rows(rows)
    if not normalized_rows:
        if skipped_conflicted:
            log(f"[history] skipped_conflicted_direction={skipped_conflicted}")
        return 0

    existing_keywords = _fetch_existing_keywords(
        sb, [row["news_url"] for row in normalized_rows]
    )
    payloads: list[dict[str, Any]] = []
    for row in normalized_rows:
        payloads.append(
            {
                "title": row["title"],
                "content": row["content"],
                "news_url": row["news_url"],
                "origin_published_at": row["origin_published_at"],
                "keyword": _merge_keywords(
                    existing_keywords.get(row["news_url"]), row.get("keyword")
                ),
                "increased_items": row.get("increased_items"),
                "decreased_items": row.get("decreased_items"),
            }
        )

    saved = 0
    batches = _chunked(payloads, BATCH_SIZE)
    for chunk in batches:
        _execute_or_raise(
            sb.table("raw_news").upsert(chunk, on_conflict="news_url"),
            action=f"upsert raw_news batch_size={len(chunk)}",
        )
        saved += len(chunk)

    if skipped_conflicted:
        log(f"[history] skipped_conflicted_direction={skipped_conflicted}")
    log(
        f"[history] supabase_batch_upsert rows={len(normalized_rows)} "
        f"batches={len(batches)} saved={saved}"
    )
    return saved
