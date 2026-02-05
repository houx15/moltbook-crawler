#!/usr/bin/env python3
"""Moltbook crawler.

Fetches post lists and per-post details, then saves a normalized structure
containing posts and nested comments.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
try:
    from tqdm import tqdm
except Exception:  # noqa: BLE001 - optional dependency for progress display
    tqdm = None

BASE_URL = "https://www.moltbook.com/api/v1"
DEFAULT_HEADERS = {
    "User-Agent": "moltbook-crawler/1.0 (+https://www.moltbook.com)",
    "Accept": "application/json",
}


@dataclass
class SleepConfig:
    min_s: float
    max_s: float

    def sleep(self) -> None:
        if self.max_s <= 0:
            return
        low = min(self.min_s, self.max_s)
        high = max(self.min_s, self.max_s)
        time.sleep(random.uniform(low, high))


class Crawler:
    def __init__(
        self,
        out_dir: Path,
        sleep_cfg: SleepConfig,
        timeout_s: float = 20.0,
        max_retries: int = 3,
        force: bool = False,
        resume: bool = False,
        status_path: Optional[Path] = None,
    ) -> None:
        self.out_dir = out_dir
        self.sleep_cfg = sleep_cfg
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.force = force
        self.resume = resume
        self.status_path = status_path
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def _request_json(self, url: str) -> Dict[str, Any]:
        last_err: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout_s)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:  # noqa: BLE001 - keep retry logic simple
                last_err = exc
                if attempt < self.max_retries:
                    backoff = 0.5 * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                    time.sleep(backoff)
                else:
                    raise
        raise RuntimeError(f"unreachable: {last_err}")

    def _ensure_dir(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    def _save_json(self, path: Path, data: Dict[str, Any]) -> None:
        self._ensure_dir(path.parent)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_status(self, path: Path) -> Dict[str, Any]:
        if not path.exists():
            return {}
        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def fetch_post_list(self, limit: int, offset: int, sort: str) -> Dict[str, Any]:
        url = f"{BASE_URL}/posts?limit={limit}&sort={sort}&offset={offset}"
        return self._request_json(url)

    def fetch_post_detail(self, post_id: str) -> Dict[str, Any]:
        url = f"{BASE_URL}/posts/{post_id}"
        return self._request_json(url)

    def normalize_comment(self, comment: Dict[str, Any]) -> Dict[str, Any]:
        replies = comment.get("replies") or []
        return {
            "id": comment.get("id"),
            "content": comment.get("content"),
            "upvotes": comment.get("upvotes"),
            "downvotes": comment.get("downvotes"),
            "parent_id": comment.get("parent_id"),
            "created_at": comment.get("created_at"),
            "author": comment.get("author"),
            "replies": [self.normalize_comment(r) for r in replies],
        }

    def normalize_post_bundle(
        self, post_detail: Dict[str, Any], post_api_url: str
    ) -> Dict[str, Any]:
        post = post_detail.get("post") or {}
        comments = post_detail.get("comments") or []
        return {
            "post": {
                "id": post.get("id"),
                "title": post.get("title"),
                "content": post.get("content"),
                "url": post.get("url"),
                "upvotes": post.get("upvotes"),
                "downvotes": post.get("downvotes"),
                "comment_count": post.get("comment_count"),
                "created_at": post.get("created_at"),
                "submolt": post.get("submolt"),
                "author": post.get("author"),
            },
            "comments": [self.normalize_comment(c) for c in comments],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source": {"post_api": post_api_url},
        }

    def crawl(
        self,
        limit: int,
        offset: int,
        sort: str,
        max_pages: int,
        crawler_id: str,
        continuous: bool = False,
        restart_wait_s: float = 3600.0,
        cold_start_pages: int = 0,
        cold_start_limit: Optional[int] = None,
    ) -> None:
        raw_lists_dir = self.out_dir / "raw" / "postlists"
        raw_posts_dir = self.out_dir / "raw" / "posts"
        normalized_dir = self.out_dir / "posts"
        index_path = self.out_dir / "post_index.jsonl"
        status_path = self.status_path or (self.out_dir / f"status_{crawler_id}.json")

        self._ensure_dir(raw_lists_dir)
        self._ensure_dir(raw_posts_dir)
        self._ensure_dir(normalized_dir)

        status = self._load_status(status_path) if self.resume else {}
        total_posts = int(status.get("total_posts", 0)) if self.resume else 0
        total_comments = int(status.get("total_comments", 0)) if self.resume else 0
        cold_start_complete = bool(status.get("cold_start_complete")) if self.resume else False
        if self.resume and "current_offset" in status:
            offset = int(status.get("current_offset", offset))
        if self.resume and "current_sort" in status:
            sort = str(status.get("current_sort", sort))
        if self.resume and "current_limit" in status:
            limit = int(status.get("current_limit", limit))

        def _parse_created_at(value: Optional[str]) -> Optional[datetime]:
            if not value:
                return None
            try:
                if value.endswith("Z"):
                    value = value[:-1] + "+00:00"
                return datetime.fromisoformat(value)
            except Exception:
                return None

        while True:
            page = 0
            has_more = True
            reached_known = False
            current_offset = offset if sort != "new" else 0
            cycle_limit = limit
            cycle_max_pages = max_pages

            if sort == "new" and not cold_start_complete and cold_start_pages > 0:
                cycle_limit = cold_start_limit or limit
                cycle_max_pages = cold_start_pages

            latest_created_at: Optional[datetime] = None
            latest_post_id: Optional[str] = None

            while has_more and (cycle_max_pages <= 0 or page < cycle_max_pages):
                page += 1
                list_path = raw_lists_dir / f"offset_{current_offset}.json"

                use_cache = list_path.exists() and not self.force and sort != "new"
                if use_cache:
                    with list_path.open("r", encoding="utf-8") as f:
                        post_list = json.load(f)
                else:
                    post_list = self.fetch_post_list(
                        limit=cycle_limit, offset=current_offset, sort=sort
                    )
                    self._save_json(list_path, post_list)

                posts = post_list.get("posts") or []
                for post in posts:
                    created_at = _parse_created_at(post.get("created_at"))
                    if created_at and (
                        latest_created_at is None or created_at > latest_created_at
                    ):
                        latest_created_at = created_at
                        latest_post_id = post.get("id")

                iterable = (
                    tqdm(posts, desc=f"Page {page}", unit="post")
                    if tqdm is not None
                    else posts
                )
                for post in iterable:
                    post_id = post.get("id")
                    if not post_id:
                        continue
                    detail_path = raw_posts_dir / f"{post_id}.json"
                    normalized_path = normalized_dir / f"{post_id}.json"

                    if normalized_path.exists() and not self.force:
                        if sort == "new":
                            reached_known = True
                            break
                        continue

                    if detail_path.exists() and not self.force:
                        with detail_path.open("r", encoding="utf-8") as f:
                            detail = json.load(f)
                    else:
                        try:
                            detail = self.fetch_post_detail(post_id)
                        except Exception as exc:  # noqa: BLE001 - keep crawl moving
                            print(
                                f"Failed to fetch detail for {post_id}: {exc}",
                                file=sys.stderr,
                            )
                            continue
                        self._save_json(detail_path, detail)

                    normalized = self.normalize_post_bundle(
                        detail, f"{BASE_URL}/posts/{post_id}"
                    )
                    self._save_json(normalized_path, normalized)

                    with index_path.open("a", encoding="utf-8") as f:
                        f.write(
                            json.dumps(
                                {
                                    "id": post_id,
                                    "title": post.get("title"),
                                    "created_at": post.get("created_at"),
                                    "submolt": post.get("submolt"),
                                    "comment_count": post.get("comment_count"),
                                },
                                ensure_ascii=False,
                            )
                            + "\n"
                        )

                    total_posts += 1
                    total_comments += len(normalized.get("comments") or [])
                    self.sleep_cfg.sleep()

                if reached_known:
                    has_more = False
                else:
                    has_more = bool(post_list.get("has_more"))
                current_offset = int(
                    post_list.get("next_offset") or (current_offset + cycle_limit)
                )

                self._save_json(
                    status_path,
                    {
                        "total_posts": total_posts,
                        "total_comments": total_comments,
                        "current_offset": current_offset,
                        "current_sort": sort,
                        "current_limit": cycle_limit,
                        "has_more": has_more,
                        "crawler_id": crawler_id,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "latest_post_created_at": latest_created_at.isoformat()
                        if latest_created_at
                        else None,
                        "latest_post_id": latest_post_id,
                        "cold_start_complete": cold_start_complete,
                    },
                )

                # Gap between list pages
                self.sleep_cfg.sleep()

            if sort == "new" and not cold_start_complete and cold_start_pages > 0:
                cold_start_complete = True
                self._save_json(
                    status_path,
                    {
                        "total_posts": total_posts,
                        "total_comments": total_comments,
                        "current_offset": current_offset,
                        "current_sort": sort,
                        "current_limit": cycle_limit,
                        "has_more": has_more,
                        "crawler_id": crawler_id,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "latest_post_created_at": latest_created_at.isoformat()
                        if latest_created_at
                        else None,
                        "latest_post_id": latest_post_id,
                        "cold_start_complete": cold_start_complete,
                    },
                )

            if not continuous or sort != "new":
                break

            time.sleep(restart_wait_s)


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl Moltbook posts and comments.")
    parser.add_argument(
        "--config", default="crawler_config.json", help="Config file path"
    )
    parser.add_argument(
        "--crawler-id", default=None, help="Crawler id for status/resume"
    )
    parser.add_argument("--out-dir", default=None, help="Output directory")
    parser.add_argument("--limit", type=int, default=None, help="Posts per page")
    parser.add_argument("--offset", type=int, default=None, help="Start offset")
    parser.add_argument("--sort", default=None, help="Sort type (e.g. hot, new)")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Max pages to crawl (0 = no limit)",
    )
    parser.add_argument(
        "--sleep-min", type=float, default=None, help="Min sleep seconds"
    )
    parser.add_argument(
        "--sleep-max", type=float, default=None, help="Max sleep seconds"
    )
    parser.add_argument(
        "--timeout", type=float, default=None, help="Request timeout seconds"
    )
    parser.add_argument(
        "--retries", type=int, default=None, help="Max retries per request"
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        default=None,
        help="Continuously crawl (restart loop for new sort)",
    )
    parser.add_argument(
        "--no-continuous",
        action="store_false",
        dest="continuous",
        help="Disable continuous crawl",
    )
    parser.add_argument(
        "--restart-wait",
        type=float,
        default=None,
        help="Seconds to wait before restarting from offset 0",
    )
    parser.add_argument(
        "--cold-start-pages",
        type=int,
        default=None,
        help="Pages to crawl for cold start",
    )
    parser.add_argument(
        "--cold-start-limit",
        type=int,
        default=None,
        help="Posts per page during cold start",
    )
    parser.add_argument(
        "--separate-by-sort",
        action="store_true",
        default=None,
        help="Store data in a per-sort subdirectory",
    )
    parser.add_argument(
        "--no-separate-by-sort",
        action="store_false",
        dest="separate_by_sort",
        help="Disable per-sort output directory",
    )
    parser.add_argument(
        "--force", action="store_true", help="Re-download existing files"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=True,
        help="Resume from status.json (default: true)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_false",
        dest="resume",
        help="Disable resume from status.json",
    )
    return parser.parse_args(list(argv))


def _load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_config(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _resolve_settings(
    args: argparse.Namespace, config: Dict[str, Any]
) -> Dict[str, Any]:
    base_defaults = {
        "crawler_id": "default",
        "out_dir": "data",
        "limit": 1000,
        "offset": 0,
        "sort": "hot",
        "max_pages": 0,
        "sleep_min": 0.8,
        "sleep_max": 2.5,
        "timeout": 20.0,
        "retries": 3,
        "continuous": False,
        "restart_wait": 3600.0,
        "cold_start_pages": 0,
        "cold_start_limit": None,
        "separate_by_sort": False,
    }

    resolved_sort = args.sort or config.get("sort") or base_defaults["sort"]
    defaults = dict(base_defaults)
    if resolved_sort == "new":
        defaults.update(
            {
                "limit": 100,
                "max_pages": 0,
                "continuous": True,
                "restart_wait": 3600.0,
                "cold_start_pages": 100,
                "cold_start_limit": 500,
                "separate_by_sort": True,
            }
        )

    resolved = {
        "crawler_id": args.crawler_id
        or config.get("crawler_id")
        or defaults["crawler_id"],
        "out_dir": args.out_dir or config.get("out_dir") or defaults["out_dir"],
        "limit": (
            args.limit
            if args.limit is not None
            else config.get("limit", defaults["limit"])
        ),
        "offset": (
            args.offset
            if args.offset is not None
            else config.get("offset", defaults["offset"])
        ),
        "sort": args.sort or config.get("sort") or defaults["sort"],
        "max_pages": (
            args.max_pages
            if args.max_pages is not None
            else config.get("max_pages", defaults["max_pages"])
        ),
        "sleep_min": (
            args.sleep_min
            if args.sleep_min is not None
            else config.get("sleep_min", defaults["sleep_min"])
        ),
        "sleep_max": (
            args.sleep_max
            if args.sleep_max is not None
            else config.get("sleep_max", defaults["sleep_max"])
        ),
        "timeout": (
            args.timeout
            if args.timeout is not None
            else config.get("timeout", defaults["timeout"])
        ),
        "retries": (
            args.retries
            if args.retries is not None
            else config.get("retries", defaults["retries"])
        ),
        "continuous": (
            args.continuous
            if args.continuous is not None
            else config.get("continuous", defaults["continuous"])
        ),
        "restart_wait": (
            args.restart_wait
            if args.restart_wait is not None
            else config.get("restart_wait", defaults["restart_wait"])
        ),
        "cold_start_pages": (
            args.cold_start_pages
            if args.cold_start_pages is not None
            else config.get("cold_start_pages", defaults["cold_start_pages"])
        ),
        "cold_start_limit": (
            args.cold_start_limit
            if args.cold_start_limit is not None
            else config.get("cold_start_limit", defaults["cold_start_limit"])
        ),
        "separate_by_sort": (
            args.separate_by_sort
            if args.separate_by_sort is not None
            else config.get("separate_by_sort", defaults["separate_by_sort"])
        ),
    }
    return resolved


def main(argv: Iterable[str]) -> int:
    args = parse_args(argv)
    config_path = Path(args.config)
    config = _load_config(config_path)
    settings = _resolve_settings(args, config)

    _write_config(
        config_path,
        {
            "crawler_id": settings["crawler_id"],
            "out_dir": settings["out_dir"],
            "limit": settings["limit"],
            "offset": settings["offset"],
            "sort": settings["sort"],
            "max_pages": settings["max_pages"],
            "sleep_min": settings["sleep_min"],
            "sleep_max": settings["sleep_max"],
            "timeout": settings["timeout"],
            "retries": settings["retries"],
            "continuous": settings["continuous"],
            "restart_wait": settings["restart_wait"],
            "cold_start_pages": settings["cold_start_pages"],
            "cold_start_limit": settings["cold_start_limit"],
            "separate_by_sort": settings["separate_by_sort"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    out_dir = Path(settings["out_dir"])
    if settings["separate_by_sort"] and settings["sort"] != "hot":
        out_dir = out_dir / str(settings["sort"])
    sleep_cfg = SleepConfig(
        min_s=float(settings["sleep_min"]), max_s=float(settings["sleep_max"])
    )
    crawler = Crawler(
        out_dir=out_dir,
        sleep_cfg=sleep_cfg,
        timeout_s=float(settings["timeout"]),
        max_retries=int(settings["retries"]),
        force=args.force,
        resume=args.resume,
    )

    try:
        crawler.crawl(
            limit=int(settings["limit"]),
            offset=int(settings["offset"]),
            sort=str(settings["sort"]),
            max_pages=int(settings["max_pages"]),
            crawler_id=str(settings["crawler_id"]),
            continuous=bool(settings["continuous"]),
            restart_wait_s=float(settings["restart_wait"]),
            cold_start_pages=int(settings["cold_start_pages"]),
            cold_start_limit=(
                int(settings["cold_start_limit"])
                if settings["cold_start_limit"] is not None
                else None
            ),
        )
    except requests.HTTPError as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - keep CLI behavior consistent
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
