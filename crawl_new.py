#!/usr/bin/env python3
"""Standalone crawler for sort=new posts.

Phases
------
cold_start – pages 0…cold_start_pages at cold_start_limit posts/page.
             Persisted offset so it resumes after a crash mid-way.
             At completion the newest created_at seen becomes last_update_timestamp.
poll       – restart from offset 0; stop when a post's created_at is older
             than last_update_timestamp.  Posts already on disk are skipped
             (no detail fetch) but do NOT stop the scan — feed drift can
             surface duplicates anywhere in the page sequence.
             On completion last_update_timestamp is advanced to the newest
             created_at seen in this cycle.

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


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 created_at string into a tz-aware datetime, or None."""
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:  # noqa: BLE001
        return None


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
        self._last_update_timestamp: Optional[datetime] = None

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
        """Fetch list page with retries.

        Returns the response dict even when success=false (clean end-of-data
        signal).  Only raises on network / timeout errors after all retries
        are exhausted.
        """
        url = f"{BASE_URL}/posts?limit={limit}&sort=new&time=all&offset={offset}"
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                resp = self._session.get(url, timeout=self.timeout)
                data = resp.json()
                # success=false means "no data at this offset" — not a transient
                # error, so return immediately without raising.
                if data.get("success") is False:
                    return data
                resp.raise_for_status()
                return data
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt < self.retries:
                    time.sleep(0.5 * (2 ** (attempt - 1)) + random.uniform(0, 0.25))
                else:
                    raise
        raise RuntimeError(f"unreachable: {last_exc}")  # pragma: no cover

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
                "last_update_timestamp": (
                    self._last_update_timestamp.isoformat()
                    if self._last_update_timestamp
                    else None
                ),
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

    def _process_post(self, post: Dict[str, Any]) -> None:
        """Fetch, normalize, persist one post.  Silently skips if already on disk."""
        post_id = post.get("id")
        if not post_id:
            return

        normalized_path = self._posts_dir / f"{post_id}.json"
        if normalized_path.exists():
            return  # duplicate from feed drift — skip, do NOT stop

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
                return
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

    def _scan_pages(
        self,
        start_offset: int,
        limit: int,
        max_pages: int,
        phase: str,
        last_update_timestamp: Optional[datetime] = None,
    ) -> Optional[datetime]:
        """Iterate list pages, processing every post.

        last_update_timestamp – if set (poll phase) scanning stops when a post's
            created_at falls below this value.  None means no timestamp bound
            (cold_start — runs until max_pages or success=false).

        Returns the newest created_at seen across the entire scan (or None if
        no post had a parseable timestamp).

        Raises on list-fetch / network failure — caller owns error / backoff.
        """
        offset = start_offset
        newest_seen: Optional[datetime] = None

        for page_num in range(1, max_pages + 1) if max_pages > 0 else iter(int, 1):
            page_label = (
                f"[{phase}] page {page_num}/{max_pages}"
                if max_pages > 0
                else f"[{phase}] page {page_num}"
            )
            print(f"{page_label}  offset={offset}", file=sys.stderr)

            # --- fetch list page (may raise on network error) ---
            post_list = self._fetch_list(offset, limit)

            # --- success=false → end of data, persist and stop cleanly ---
            if post_list.get("success") is False:
                print(
                    f"{page_label}  success=false: {post_list.get('error', '?')} "
                    f"— end of data. "
                    f"(total_posts={self._total_posts}, total_comments={self._total_comments})",
                    file=sys.stderr,
                )
                self._save_status(offset, phase)
                return newest_seen

            # --- persist raw list with timestamp suffix ---
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            self._write_json(self._raw_lists / f"offset_{offset}_{ts}.json", post_list)

            posts = post_list.get("posts") or []
            print(f"{page_label}  got {len(posts)} posts", file=sys.stderr)

            # --- process posts (with tqdm if available) ---
            iterable = (
                tqdm(posts, desc=page_label, unit="post")
                if tqdm is not None
                else posts
            )
            reached_boundary = False
            for post in iterable:
                created_at = _parse_ts(post.get("created_at"))

                # track the newest timestamp we've seen in this scan
                if created_at and (newest_seen is None or created_at > newest_seen):
                    newest_seen = created_at

                # timestamp boundary check (poll only; cold_start passes None)
                if last_update_timestamp and created_at and created_at < last_update_timestamp:
                    print(
                        f"{page_label}  reached timestamp boundary "
                        f"({created_at.isoformat()} < {last_update_timestamp.isoformat()}) "
                        f"— stopping. "
                        f"(total_posts={self._total_posts}, total_comments={self._total_comments})",
                        file=sys.stderr,
                    )
                    reached_boundary = True
                    break

                self._process_post(post)

            # --- always advance by limit; never trust next_offset ---
            offset += limit
            self._save_status(offset, phase)

            if reached_boundary:
                return newest_seen

            # gap between list pages
            self._sleep()

        return newest_seen

    # ------------------------------------------------------------------
    # phases
    # ------------------------------------------------------------------

    def _cold_start(self, resume_offset: int) -> None:
        """Run cold start, catching list-fetch errors and backing off."""
        while True:
            try:
                newest_seen = self._scan_pages(
                    start_offset=resume_offset,
                    limit=self.cold_start_limit,
                    max_pages=self.cold_start_pages,
                    phase="cold_start",
                    # no timestamp bound — run the full page range
                )
                # completed without error; anchor = newest post we saw
                self._cold_start_complete = True
                if newest_seen:
                    self._last_update_timestamp = newest_seen
                self._save_status(0, "poll")
                print(
                    f"[cold_start] complete. total_posts={self._total_posts}, "
                    f"total_comments={self._total_comments}, "
                    f"last_update_timestamp={self._last_update_timestamp.isoformat() if self._last_update_timestamp else None}",
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
                newest_seen = self._scan_pages(
                    start_offset=0,
                    limit=self.poll_limit,
                    max_pages=0,  # no page cap; stops at timestamp boundary
                    phase="poll",
                    last_update_timestamp=self._last_update_timestamp,
                )
                # advance the anchor to the newest post seen in this cycle
                if newest_seen:
                    self._last_update_timestamp = newest_seen
                self._save_status(0, "poll")
                print(
                    f"[poll] cycle complete. "
                    f"total_posts={self._total_posts}, "
                    f"last_update_timestamp={self._last_update_timestamp.isoformat() if self._last_update_timestamp else None}",
                    file=sys.stderr,
                )
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
        self._last_update_timestamp = _parse_ts(status.get("last_update_timestamp"))

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
