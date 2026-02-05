#!/usr/bin/env python3
"""Standalone crawler for sort=new posts.

Phases
------
cold_start – pages 0…cold_start_pages at cold_start_limit posts/page.
             Persisted offset so it resumes after a crash mid-way.
poll       – restart from offset 0 every poll_interval seconds; stop as
             soon as a post that already exists on disk is encountered.

Error recovery: list-page fetch failure → log, back off 30-60 s (random),
resume current phase from the last persisted offset.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

try:
    from tqdm import tqdm
except Exception:  # noqa: BLE001
    tqdm = None  # type: ignore[assignment, misc]

BASE_URL = "https://www.moltbook.com/api/v1"
_HEADERS = {
    "User-Agent": "moltbook-crawler/1.0 (+https://www.moltbook.com)",
    "Accept": "application/json",
}


# ---------------------------------------------------------------------------
# module-level helpers
# ---------------------------------------------------------------------------


def _log_error_backoff(phase: str, exc: Exception) -> None:
    """Print error, sleep 30-60 s."""
    delay = random.uniform(30, 60)
    print(
        f"[{phase}] error: {exc} — backing off {delay:.0f}s …",
        file=sys.stderr,
    )
    time.sleep(delay)


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Crawl sort=new posts continuously.")
    p.add_argument("--config", type=Path, default=Path("config.new.json"))
    p.add_argument("--out-dir", type=Path, default=None)
    p.add_argument("--cold-start-pages", type=int, default=None)
    p.add_argument("--cold-start-limit", type=int, default=None)
    p.add_argument("--poll-limit", type=int, default=None)
    p.add_argument("--sleep-min", type=float, default=None)
    p.add_argument("--sleep-max", type=float, default=None)
    p.add_argument("--timeout", type=float, default=None)
    p.add_argument("--retries", type=int, default=None)
    p.add_argument("--poll-interval", type=float, default=None)
    return p.parse_args(argv)


_DEFAULTS: Dict[str, Any] = {
    "out_dir": "data/new",
    "cold_start_pages": 500,
    "cold_start_limit": 100,
    "poll_limit": 100,
    "sleep_min": 0.8,
    "sleep_max": 2.5,
    "timeout": 20.0,
    "retries": 3,
    "poll_interval": 3600.0,
}


def _load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return {}


def _resolve(args: argparse.Namespace) -> Dict[str, Any]:
    """Merge priority: CLI flag > config file > hardcoded defaults."""
    cfg = _load_config(args.config)
    resolved: Dict[str, Any] = {}
    for key, default in _DEFAULTS.items():
        cli_val = getattr(args, key, None)
        if cli_val is not None:
            resolved[key] = cli_val
        elif key in cfg:
            resolved[key] = cfg[key]
        else:
            resolved[key] = default
    resolved["out_dir"] = Path(resolved["out_dir"])
    return resolved


# ---------------------------------------------------------------------------
# crawler
# ---------------------------------------------------------------------------


class NewPostCrawler:
    def __init__(
        self,
        out_dir: Path,
        cold_start_pages: int = 500,
        cold_start_limit: int = 100,
        poll_limit: int = 100,
        sleep_min: float = 0.8,
        sleep_max: float = 2.5,
        timeout: float = 20.0,
        retries: int = 3,
        poll_interval: float = 3600.0,
    ) -> None:
        self.out_dir = out_dir
        self.cold_start_pages = cold_start_pages
        self.cold_start_limit = cold_start_limit
        self.poll_limit = poll_limit
        self.sleep_min = sleep_min
        self.sleep_max = sleep_max
        self.timeout = timeout
        self.retries = retries
        self.poll_interval = poll_interval

        # derived paths
        self._raw_lists = out_dir / "raw" / "postlists"
        self._raw_posts = out_dir / "raw" / "posts"
        self._posts_dir = out_dir / "posts"
        self._index_path = out_dir / "post_index.jsonl"
        self._status_path = out_dir / "status_new.json"

        # mutable counters (restored from status in run())
        self._total_posts = 0
        self._total_comments = 0
        self._cold_start_complete = False

        self._session = requests.Session()
        self._session.headers.update(_HEADERS)

    # ------------------------------------------------------------------
    # HTTP layer
    # ------------------------------------------------------------------

    def _get(self, url: str) -> Dict[str, Any]:
        """GET with retry + exponential backoff. Raises on final failure."""
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                resp = self._session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt < self.retries:
                    time.sleep(0.5 * (2 ** (attempt - 1)) + random.uniform(0, 0.25))
                else:
                    raise
        raise RuntimeError(f"unreachable: {last_exc}")  # pragma: no cover

    def _fetch_list(self, offset: int, limit: int) -> Dict[str, Any]:
        url = f"{BASE_URL}/posts?limit={limit}&sort=new&time=all&offset={offset}"
        return self._get(url)

    def _fetch_detail(self, post_id: str) -> Dict[str, Any]:
        url = f"{BASE_URL}/posts/{post_id}"
        return self._get(url)

    # ------------------------------------------------------------------
    # persistence
    # ------------------------------------------------------------------

    def _write_json(self, path: Path, data: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_status(self) -> Dict[str, Any]:
        if not self._status_path.exists():
            return {}
        try:
            with self._status_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:  # noqa: BLE001
            return {}

    def _save_status(self, offset: int, phase: str) -> None:
        self._write_json(
            self._status_path,
            {
                "phase": phase,
                "current_offset": offset,
                "cold_start_complete": self._cold_start_complete,
                "total_posts": self._total_posts,
                "total_comments": self._total_comments,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    # ------------------------------------------------------------------
    # normalization
    # ------------------------------------------------------------------

    def _norm_comment(self, c: Dict[str, Any]) -> Dict[str, Any]:
        replies = c.get("replies") or []
        return {
            "id": c.get("id"),
            "content": c.get("content"),
            "upvotes": c.get("upvotes"),
            "downvotes": c.get("downvotes"),
            "parent_id": c.get("parent_id"),
            "created_at": c.get("created_at"),
            "author": c.get("author"),
            "replies": [self._norm_comment(r) for r in replies],
        }

    def _normalize(self, detail: Dict[str, Any], post_id: str) -> Dict[str, Any]:
        post = detail.get("post") or {}
        comments = detail.get("comments") or []
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
            "comments": [self._norm_comment(c) for c in comments],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source": {"post_api": f"{BASE_URL}/posts/{post_id}"},
        }

    # ------------------------------------------------------------------
    # core loop
    # ------------------------------------------------------------------

    def _sleep(self) -> None:
        if self.sleep_max <= 0:
            return
        lo = min(self.sleep_min, self.sleep_max)
        hi = max(self.sleep_min, self.sleep_max)
        time.sleep(random.uniform(lo, hi))

    def _process_post(self, post: Dict[str, Any]) -> bool:
        """Fetch, normalize, persist one post.

        Returns True if the normalized file already existed on disk
        (i.e. this is a known post — the caller should stop scanning).
        """
        post_id = post.get("id")
        if not post_id:
            return False

        normalized_path = self._posts_dir / f"{post_id}.json"
        if normalized_path.exists():
            return True  # reached_known

        # raw detail
        raw_path = self._raw_posts / f"{post_id}.json"
        if raw_path.exists():
            with raw_path.open("r", encoding="utf-8") as f:
                detail = json.load(f)
        else:
            try:
                detail = self._fetch_detail(post_id)
            except Exception as exc:  # noqa: BLE001
                print(f"  detail fetch failed for {post_id}: {exc}", file=sys.stderr)
                return False
            self._write_json(raw_path, detail)

        normalized = self._normalize(detail, post_id)
        self._write_json(normalized_path, normalized)

        # append to index
        with self._index_path.open("a", encoding="utf-8") as f:
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

        self._total_posts += 1
        self._total_comments += len(normalized.get("comments") or [])
        self._sleep()
        return False

    def _scan_pages(
        self, start_offset: int, limit: int, max_pages: int, phase: str
    ) -> None:
        """Iterate list pages, processing every post.

        Raises on list-fetch failure — caller owns error / backoff.
        Stops early when _process_post returns True (reached_known).
        """
        offset = start_offset
        pages_done = 0
        for page_num in range(1, max_pages + 1) if max_pages > 0 else iter(int, 1):
            page_label = (
                f"[{phase}] page {page_num}/{max_pages}"
                if max_pages > 0
                else f"[{phase}] page {page_num}"
            )
            print(f"{page_label}  offset={offset}", file=sys.stderr)

            # --- fetch list page (may raise) ---
            post_list = self._fetch_list(offset, limit)

            # --- persist raw list with timestamp suffix ---
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            self._write_json(self._raw_lists / f"offset_{offset}_{ts}.json", post_list)

            posts = post_list.get("posts") or []
            has_next = post_list.get("next_offset") is not None

            # --- process posts (with tqdm if available) ---
            iterable = (
                tqdm(posts, desc=page_label, unit="post")
                if tqdm is not None
                else posts
            )
            reached_known = False
            for post in iterable:
                if self._process_post(post):
                    reached_known = True
                    break

            pages_done += 1

            # --- advance offset & persist status after every page ---
            next_offset = int(post_list.get("next_offset") or (offset + limit))
            self._save_status(next_offset, phase)
            offset = next_offset

            if reached_known:
                print(
                    f"{page_label}  reached known post — stopping. "
                    f"(total_posts={self._total_posts}, total_comments={self._total_comments})",
                    file=sys.stderr,
                )
                return

            if not post_list.get("has_more"):
                print(
                    f"{page_label}  has_more=false"
                    + ("" if has_next else ", next_offset missing")
                    + f" — stopping. (total_posts={self._total_posts}, total_comments={self._total_comments})",
                    file=sys.stderr,
                )
                return

            if not has_next:
                print(
                    f"{page_label}  WARNING: next_offset missing, falling back to offset + limit",
                    file=sys.stderr,
                )

            # gap between list pages
            self._sleep()

    # ------------------------------------------------------------------
    # phases
    # ------------------------------------------------------------------

    def _cold_start(self, resume_offset: int) -> None:
        """Run cold start, catching list-fetch errors and backing off."""
        while True:
            try:
                self._scan_pages(
                    start_offset=resume_offset,
                    limit=self.cold_start_limit,
                    max_pages=self.cold_start_pages,
                    phase="cold_start",
                )
                # completed without error
                self._cold_start_complete = True
                self._save_status(0, "poll")
                print(
                    f"[cold_start] complete. total_posts={self._total_posts}, "
                    f"total_comments={self._total_comments}",
                    file=sys.stderr,
                )
                return
            except Exception as exc:  # noqa: BLE001
                _log_error_backoff("cold_start", exc)
                # reload persisted offset so we resume from last good page
                status = self._load_status()
                resume_offset = int(status.get("current_offset", resume_offset))

    def _poll_cycle(self) -> None:
        """Single poll pass from offset 0. Catches errors and backs off."""
        while True:
            try:
                self._scan_pages(
                    start_offset=0,
                    limit=self.poll_limit,
                    max_pages=0,  # no page cap; stops via reached_known or has_more
                    phase="poll",
                )
                print("[poll] cycle complete.", file=sys.stderr)
                return
            except Exception as exc:  # noqa: BLE001
                _log_error_backoff("poll", exc)
                # poll always restarts from 0 after an error

    # ------------------------------------------------------------------
    # entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        # ensure directories
        self._raw_lists.mkdir(parents=True, exist_ok=True)
        self._raw_posts.mkdir(parents=True, exist_ok=True)
        self._posts_dir.mkdir(parents=True, exist_ok=True)

        # restore state
        status = self._load_status()
        self._total_posts = int(status.get("total_posts", 0))
        self._total_comments = int(status.get("total_comments", 0))
        self._cold_start_complete = bool(status.get("cold_start_complete", False))

        # --- cold start (skipped if already done) ---
        if not self._cold_start_complete:
            resume_offset = int(status.get("current_offset", 0))
            print(
                f"[cold_start] starting from offset {resume_offset}",
                file=sys.stderr,
            )
            self._cold_start(resume_offset)

        # --- poll loop (forever) ---
        while True:
            print("[poll] starting cycle …", file=sys.stderr)
            self._poll_cycle()
            print(
                f"[poll] sleeping {self.poll_interval:.0f}s …",
                file=sys.stderr,
            )
            time.sleep(self.poll_interval)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    args = _parse_args()
    s = _resolve(args)
    crawler = NewPostCrawler(
        out_dir=s["out_dir"],
        cold_start_pages=s["cold_start_pages"],
        cold_start_limit=s["cold_start_limit"],
        poll_limit=s["poll_limit"],
        sleep_min=s["sleep_min"],
        sleep_max=s["sleep_max"],
        timeout=s["timeout"],
        retries=s["retries"],
        poll_interval=s["poll_interval"],
    )
    crawler.run()


if __name__ == "__main__":
    main()
