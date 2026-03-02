"""
AI-powered website metrics estimator using Claude API.
Used when a GA4 property ID is not configured but a website URL is known.
"""

import os
import json
import re
import random
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta


class WebsiteAIEstimator:
    """
    Estimates monthly website metrics via Claude when GA4 is unavailable.
    Returns data in the same structure expected by collect_website_bulk so
    the rest of the pipeline (benchmark lookup, Firestore storage) is unchanged.
    """

    ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
    MAX_MONTHS = 3  # cap AI estimation to last 3 months

    def __init__(self, website_url: str, industry: str = "healthcare"):
        self.website_url = website_url.rstrip("/")
        self.industry = industry
        self.api_key = os.environ.get("ANTHROPIC_API_KEY")

    def estimate_monthly_metrics(self, months: int = 3) -> dict:
        """
        Returns a dict keyed by 'YYYY-MM' whose values mirror the per-month
        structure that GA4Fetcher.get_monthly_metrics_bulk() returns:

          {
            '2026-01': {
              'awareness':  { 'sessions': int, 'users': int },
              'engagement': { 'pages_per_session': float,
                              'avg_session_duration': float,
                              'engagement_rate': float },
              'conversion': { 'total_conversions': int,
                              'conversion_rate': float },
              'retention':  { 'returning_users': int,
                              'returning_user_rate': float },
              'advocacy':   { 'referral_sessions': int,
                              'social_sessions': int },
              'top_pages':  [ {'path': str, 'title': str, 'sessions': int} ]
            },
            ...
          }
        """
        months = min(months, self.MAX_MONTHS)

        # Build list of YYYY-MM strings for the last N months
        now = datetime.now()
        month_keys = [
            (now - relativedelta(months=i)).strftime("%Y-%m")
            for i in range(months - 1, -1, -1)
        ]

        if self.api_key:
            result = self._call_claude(month_keys)
            if result:
                print(f"[OK] AI estimated website metrics for {len(result)} month(s)")
                return result
            print("[WARNING] Claude response unusable, falling back to rule-based estimates")

        return self._rule_based_estimates(month_keys)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _call_claude(self, month_keys: list) -> dict:
        prompt = self._build_prompt(month_keys)
        try:
            resp = requests.post(
                self.ANTHROPIC_API_URL,
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": self.api_key,
                    "anthropic-version": "2023-06-01",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 2048,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=30,
            )
            resp.raise_for_status()
            raw = resp.json()["content"][0]["text"]
            return self._parse_json(raw)
        except Exception as exc:
            print(f"[ERROR] Claude estimation failed: {exc}")
            return {}

    def _build_prompt(self, month_keys: list) -> str:
        months_csv = ", ".join(month_keys)
        example_key = month_keys[0]
        return f"""You are a web analytics expert estimating realistic website traffic for a small {self.industry} practice.

Website: {self.website_url}
Industry: {self.industry}
Months needed: {months_csv}

Typical ranges for a small {self.industry} practice:
- sessions per month: 150–700
- users: 80% of sessions
- pages_per_session: 2–4
- avg_session_duration (seconds): 60–180
- engagement_rate (%): 40–65
- total_conversions (form submits): 3–20
- conversion_rate (%): 1–5
- returning_user_rate (%): 20–40
- returning_users: returning_user_rate * users
- referral_sessions: 5–15% of sessions
- top_pages: realistic paths for the website type

Return ONLY raw JSON with NO markdown or code fences. Vary values naturally across months.

{{
  "{example_key}": {{
    "awareness":  {{ "sessions": 320, "users": 260 }},
    "engagement": {{ "pages_per_session": 2.9, "avg_session_duration": 118, "engagement_rate": 52.0 }},
    "conversion": {{ "total_conversions": 11, "conversion_rate": 3.4 }},
    "retention":  {{ "returning_users": 78, "returning_user_rate": 30.0 }},
    "advocacy":   {{ "referral_sessions": 38, "social_sessions": 0 }},
    "top_pages":  [
      {{"path": "/", "title": "Home", "sessions": 145}},
      {{"path": "/services", "title": "Services", "sessions": 82}},
      {{"path": "/contact", "title": "Contact Us", "sessions": 64}}
    ]
  }}
}}

Provide an entry for each month: {months_csv}"""

    def _parse_json(self, text: str) -> dict:
        """Extract and validate JSON from an AI response."""
        text = text.strip()
        # Strip optional markdown fences
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # Try to find the outermost {...}
            match = re.search(r"\{[\s\S]*\}", text)
            if not match:
                return {}
            try:
                data = json.loads(match.group())
            except json.JSONDecodeError:
                return {}

        # Validate structure – must be a dict of YYYY-MM keys
        if not isinstance(data, dict):
            return {}
        validated = {}
        for k, v in data.items():
            if isinstance(v, dict) and "awareness" in v:
                validated[k] = v
        return validated

    def _rule_based_estimates(self, month_keys: list) -> dict:
        """Deterministic fallback when Claude is unavailable."""
        base = 300
        result = {}
        for mk in month_keys:
            s = int(base * random.uniform(0.85, 1.15))
            u = int(s * random.uniform(0.75, 0.90))
            rur = random.uniform(22, 38)
            result[mk] = {
                "awareness": {"sessions": s, "users": u},
                "engagement": {
                    "pages_per_session": round(random.uniform(2.2, 3.5), 1),
                    "avg_session_duration": round(random.uniform(80, 160), 0),
                    "engagement_rate": round(random.uniform(40, 62), 1),
                },
                "conversion": {
                    "total_conversions": int(s * random.uniform(0.02, 0.05)),
                    "conversion_rate": round(random.uniform(2.0, 5.0), 1),
                },
                "retention": {
                    "returning_users": int(u * rur / 100),
                    "returning_user_rate": round(rur, 1),
                },
                "advocacy": {
                    "referral_sessions": int(s * random.uniform(0.06, 0.14)),
                    "social_sessions": 0,
                },
                "top_pages": [
                    {"path": "/", "title": "Home", "sessions": int(s * 0.45)},
                    {"path": "/services", "title": "Services", "sessions": int(s * 0.25)},
                    {"path": "/contact", "title": "Contact Us", "sessions": int(s * 0.18)},
                ],
            }
        return result
