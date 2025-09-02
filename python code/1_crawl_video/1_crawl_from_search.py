
from __future__ import annotations

import os
import json
import time
from typing import Iterable, List, Dict, Any, Optional
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import requests
import sys
from pathlib import Path
# Project root = 1 cấp trên file hiện tại
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))
import env_utils
import pandas as pd
import gcp_io



SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
TZ = ZoneInfo("Asia/Ho_Chi_Minh")



def crawl_youtube_videos(
    api_key: str,
    keywords: Iterable[str],
    max_results: int = 300,
    request_pause: float = 0.1,
) -> List[Dict[str, Any]]:
    """
    Crawl video theo nhiều từ khóa, tối đa max_results mỗi keyword.
    Trả về list dict “snippet-lite”.
    """
    results: List[Dict[str, Any]] = []
    today = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

    for keyword in keywords:
        print(f"🔍 Crawling keyword: {keyword}")
        next_page_token: Optional[str] = None
        total_collected = 0

        while total_collected < max_results:
            page_size = min(50, max_results - total_collected)
            params = {
                "part": "snippet",
                "q": keyword,
                "type": "video",
                "order": "relevance",
                "maxResults": page_size,
                "key": api_key,
            }
            if next_page_token:
                params["pageToken"] = next_page_token

            resp = requests.get(SEARCH_URL, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"❌ Error {resp.status_code}: {resp.text[:300]}")
                break

            data = resp.json()
            items = data.get("items", [])
            if not items:
                break

            for item in items:
                id_ = item.get("id", {})
                snippet = item.get("snippet", {})
                vid = id_.get("videoId")
                if not vid:
                    continue

                results.append(
                    {
                        "videoId": vid,
                        "title": snippet.get("title"),
                        "description": snippet.get("description"),
                        "channelId": snippet.get("channelId"),
                        "channelTitle": snippet.get("channelTitle"),
                        "publishedAt": snippet.get("publishedAt"),
                        "searchKeyword": keyword,
                        "crawlDate": today,
                    }
                )
                total_collected += 1
                if total_collected >= max_results:
                    break

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break

            time.sleep(request_pause)  # hạn chế rate limit

    return results



def main() -> None:
    # 1) Load env & biến cấu hình
    cfg = env_utils.load_env()
    ts = datetime.now(TZ).strftime("_%Y%m%d_%H%M%S")

    # 2) Từ khóa & crawl
    keywords = [
        "AI tool", "Artificial Intelligence", "AI agent",
        "Generative AI", "AI Automation", "AI for", "Learn AI",
        "AI Algorithms", "AI in Business",
        "Prompt Engineering", "AI for Data Science", "AI for Project Management",
        "AI in Education", "AI Career", "AI Productivity", "xAI",
        "AI and Big Data", "AI for Developers", "ChatGPT", "Cursor AI", "Claude AI",
        "Google Gemini", "No/low code AI", "AI tutorial", "Machine Learning AI",
        "Deep Learning AI", "AI coding", "Chatbox AI", "How to AI",
        "Latest AI", "AI application", "AI robot", "AI trends",
    ]

    results = crawl_youtube_videos(cfg.api, keywords, max_results=1)

    gcp_io.upload_json_to_gcs(
        bucket=cfg.bucket_name,
        project_id=cfg.project_id,
        path=f"{cfg.search_result_raw}ai_videos_snippets{ts}.json",
        data=results
    )

if __name__ == "__main__":
    main()
