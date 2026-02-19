"""
Social Media Analytics - REAL METRICS ONLY
Separated Facebook and Instagram metrics per customer journey stage.

Per-platform metrics (all from working APIs):

FACEBOOK (post-level insights + page fields):
1. Reach      - post_impressions_unique (post insights)
2. Engagement - reactions + comments (post fields)
3. Conversion - post_clicks (post insights, closest link-click proxy)
4. Retention  - fan_count (page object field)
5. Advocacy   - shares (post fields)

INSTAGRAM (account-level insights preferred; media-level for shares):
1. Reach      - reach (account insights, period=day)
2. Engagement - accounts_engaged (account insights, metric_type=total_value)
3. Conversion - profile_links_taps (external link taps from bio, account insights)
4. Retention  - follower_count (account insights)
5. Advocacy   - shares aggregated from media insights per post

NOTE on Instagram Conversion:
  - profile_links_taps tracks external link taps from the bio/profile
  - This is the closest account-level link-click metric available
  - Falls back to 0 if not available for the account
  - Instagram deprecated website_clicks/profile_views in Jan 2025
"""

import requests
from datetime import datetime, timedelta
import json

API_VERSION = "v24.0"


# ============================================================================
# FACEBOOK - POST-LEVEL INSIGHTS
# ============================================================================

def get_facebook_metrics_bulk(page_id, page_token, days_back=365):
    """
    Collect all Facebook metrics by aggregating from individual posts.

    Returns monthly data keyed by "YYYY-MM" with these platform metrics:
      reach        - post_impressions_unique (post insights)
      engagement   - reactions + comments (post fields)
      conversion   - post_clicks (post insights)
      advocacy     - shares (post fields)

    Fan count (retention) is returned as a separate top-level key.

    All values are REAL API data, no estimates.
    """
    print(f"  [Facebook] Fetching posts for last {days_back} days...")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    since_timestamp = int(start_date.timestamp())

    # Step 1: Get posts with engagement fields
    posts_url = f"https://graph.facebook.com/{API_VERSION}/{page_id}/posts"
    posts_params = {
        'fields': 'id,message,created_time,shares,reactions.summary(true),comments.summary(true)',
        'since': since_timestamp,
        'access_token': page_token,
        'limit': 100
    }

    try:
        posts_response = requests.get(posts_url, params=posts_params, timeout=30)
        if posts_response.status_code != 200:
            print(f"  [Facebook] Posts API error {posts_response.status_code}")
            try:
                print(f"  [Facebook] {json.dumps(posts_response.json(), indent=2)}")
            except Exception:
                print(f"  [Facebook] {posts_response.text}")
        posts_response.raise_for_status()
        posts_data = posts_response.json().get('data', [])
        print(f"  [Facebook] Found {len(posts_data)} posts")
    except Exception as e:
        print(f"  [Facebook] Failed to get posts: {e}")
        return {}

    monthly_data = {}
    all_posts = []  # Per-post records for top performers

    for post in posts_data:
        try:
            post_id = post['id']
            created_time = post.get('created_time', '')
            if not created_time:
                continue

            post_date = datetime.strptime(created_time[:10], '%Y-%m-%d')
            month_key = f"{post_date.year}-{post_date.month:02d}"

            if month_key not in monthly_data:
                monthly_data[month_key] = {
                    'reach': 0,
                    'reactions': 0,
                    'comments': 0,
                    'clicks': 0,
                    'shares': 0,
                    'posts': 0
                }

            monthly_data[month_key]['posts'] += 1

            # Post fields (reactions, comments, shares) - always work
            reactions = post.get('reactions', {}).get('summary', {}).get('total_count', 0)
            comments = post.get('comments', {}).get('summary', {}).get('total_count', 0)
            shares = post.get('shares', {}).get('count', 0)

            monthly_data[month_key]['reactions'] += reactions
            monthly_data[month_key]['comments'] += comments
            monthly_data[month_key]['shares'] += shares

            # Post-level insights (reach + clicks)
            post_reach = 0
            post_clicks = 0
            insights_url = f"https://graph.facebook.com/{API_VERSION}/{post_id}/insights"
            insights_params = {
                'metric': 'post_impressions_unique,post_clicks',
                'access_token': page_token
            }

            insights_response = requests.get(insights_url, params=insights_params, timeout=10)

            if insights_response.status_code == 200:
                for insight in insights_response.json().get('data', []):
                    metric_name = insight.get('name')
                    values = insight.get('values', [{}])
                    value = values[0].get('value', 0) if values else 0

                    if metric_name == 'post_impressions_unique':
                        monthly_data[month_key]['reach'] += value
                        post_reach = value
                    elif metric_name == 'post_clicks':
                        monthly_data[month_key]['clicks'] += value
                        post_clicks = value
            else:
                print(f"    [Facebook] Post insights error {insights_response.status_code} for post {post_id}")

            # Track per-post data for top performers
            message = post.get('message', '')
            title = (message[:80] + '...') if len(message) > 80 else message
            if not title:
                title = f"Post {post_date.strftime('%b %d, %Y')}"
            engagement = reactions + comments
            all_posts.append({
                'id': post_id,
                'title': title,
                'date': created_time[:10],
                'month': month_key,
                'reach': post_reach,
                'engagement': engagement,
                'clicks': post_clicks,
                'shares': shares,
            })

        except Exception as e:
            print(f"  [Facebook] Warning: failed to process post: {e}")
            continue

    # Fan count (current, point-in-time - used for all months as retention)
    fan_count = 0
    try:
        page_url = f"https://graph.facebook.com/{API_VERSION}/{page_id}"
        page_params = {
            'fields': 'fan_count,followers_count',
            'access_token': page_token
        }
        page_response = requests.get(page_url, params=page_params, timeout=10)
        page_data = page_response.json()
        fan_count = page_data.get('fan_count', 0)
        print(f"  [Facebook] Fan count: {fan_count:,}")
    except Exception as e:
        print(f"  [Facebook] Could not retrieve fan count: {e}")

    print(f"  [Facebook] Collected data for {len(monthly_data)} months, {len(all_posts)} posts")
    return {
        'monthly_data': monthly_data,
        'fan_count': fan_count,
        'all_posts': all_posts
    }


# ============================================================================
# INSTAGRAM - ACCOUNT-LEVEL INSIGHTS (preferred source)
# ============================================================================

def get_instagram_account_metrics_bulk(instagram_id, page_token, days_back=365):
    """
    Collect Instagram account-level metrics in 30-day chunks.

    Metrics retrieved from /{ig-user-id}/insights:
      reach             - unique accounts reached (daily, summed per month)
      accounts_engaged  - unique accounts that engaged (daily, summed per month)
      follower_count    - current followers (point-in-time)

    Instagram deprecated profile_views, website_clicks, phone_call_clicks
    and text_message_clicks as of January 2025. The closest conversion proxy
    at account level is profile_link_taps (external link taps from bio).
    This function attempts to fetch it; falls back to 0 if unavailable.

    Returns:
        {
          'monthly_data': {
              'YYYY-MM': {
                  'reach': int,
                  'accounts_engaged': int,
                  'link_taps': int,   # profile_link_taps or 0
              }
          },
          'follower_count': int
        }
    """
    print(f"  [Instagram] Fetching account insights for last {days_back} days...")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    insights_url = f"https://graph.facebook.com/{API_VERSION}/{instagram_id}/insights"
    monthly_data = {}

    # --- reach and accounts_engaged (30-day chunks) ---
    current_chunk_end = end_date
    while current_chunk_end > start_date:
        current_chunk_start = max(current_chunk_end - timedelta(days=30), start_date)
        print(f"    Chunk: {current_chunk_start.strftime('%Y-%m-%d')} to {current_chunk_end.strftime('%Y-%m-%d')}")

        try:
            # --- reach: uses values[] array, no metric_type ---
            reach_params = {
                'metric': 'reach',
                'period': 'day',
                'access_token': page_token,
                'since': current_chunk_start.strftime('%Y-%m-%d'),
                'until': current_chunk_end.strftime('%Y-%m-%d'),
            }
            reach_response = requests.get(insights_url, params=reach_params, timeout=30)
            if reach_response.status_code == 200:
                for metric_obj in reach_response.json().get('data', []):
                    if metric_obj.get('name') != 'reach':
                        continue
                    for value_obj in metric_obj.get('values', []):
                        value = value_obj.get('value', 0)
                        end_time = value_obj.get('end_time', '')
                        if not end_time:
                            continue
                        date = datetime.strptime(end_time[:10], '%Y-%m-%d')
                        month_key = f"{date.year}-{date.month:02d}"
                        if month_key not in monthly_data:
                            monthly_data[month_key] = {'reach': 0, 'accounts_engaged': 0, 'link_taps': 0}
                        monthly_data[month_key]['reach'] += value
            else:
                print(f"    [Instagram] reach error {reach_response.status_code}")

            # --- accounts_engaged: metric_type=total_value returns total_value{value} not values[] ---
            engaged_params = {
                'metric': 'accounts_engaged',
                'period': 'day',
                'access_token': page_token,
                'since': current_chunk_start.strftime('%Y-%m-%d'),
                'until': current_chunk_end.strftime('%Y-%m-%d'),
                'metric_type': 'total_value'
            }
            engaged_response = requests.get(insights_url, params=engaged_params, timeout=30)
            if engaged_response.status_code == 200:
                for metric_obj in engaged_response.json().get('data', []):
                    if metric_obj.get('name') != 'accounts_engaged':
                        continue
                    value = metric_obj.get('total_value', {}).get('value', 0)
                    # Attribute the total to the chunk's end month
                    chunk_month = current_chunk_end.strftime('%Y-%m')
                    if chunk_month not in monthly_data:
                        monthly_data[chunk_month] = {'reach': 0, 'accounts_engaged': 0, 'link_taps': 0}
                    monthly_data[chunk_month]['accounts_engaged'] += value
            else:
                print(f"    [Instagram] accounts_engaged error {engaged_response.status_code}")

        except Exception as e:
            print(f"    [Instagram] Chunk error: {e}")

        current_chunk_end = current_chunk_start - timedelta(days=1)

    print(f"  [Instagram] Account insights collected for {len(monthly_data)} months")

    # --- profile_link_taps (conversion proxy, attempt) ---
    # This metric tracks external link taps from the Instagram profile/bio.
    # It is only available for some accounts and within a 90-day window.
    # We attempt a 30-day fetch; if it fails we log and continue with 0.
    try:
        link_params = {
            'metric': 'profile_links_taps',
            'period': 'day',
            'access_token': page_token,
            'since': (end_date - timedelta(days=30)).strftime('%Y-%m-%d'),
            'until': end_date.strftime('%Y-%m-%d'),
            'metric_type': 'total_value'
        }
        link_response = requests.get(insights_url, params=link_params, timeout=10)
        if link_response.status_code == 200:
            link_data = link_response.json().get('data', [])
            for metric_obj in link_data:
                if metric_obj.get('name') == 'profile_links_taps':
                    # metric_type=total_value returns total_value{value}, not values[]
                    value = metric_obj.get('total_value', {}).get('value', 0)
                    month_key = end_date.strftime('%Y-%m')
                    if month_key not in monthly_data:
                        monthly_data[month_key] = {'reach': 0, 'accounts_engaged': 0, 'link_taps': 0}
                    monthly_data[month_key]['link_taps'] += value
            print(f"  [Instagram] profile_links_taps collected")
        else:
            print(f"  [Instagram] profile_links_taps not available (status {link_response.status_code}) - using 0")
    except Exception as e:
        print(f"  [Instagram] Could not retrieve profile_link_taps: {e}")

    # --- follower_count: read from IG user object (insights metric is daily GAIN, not total) ---
    follower_count = 0
    try:
        user_url = f"https://graph.facebook.com/{API_VERSION}/{instagram_id}"
        user_params = {
            'fields': 'followers_count',
            'access_token': page_token
        }
        user_response = requests.get(user_url, params=user_params, timeout=10)
        if user_response.status_code == 200:
            follower_count = user_response.json().get('followers_count', 0)
            print(f"  [Instagram] Follower count: {follower_count:,}")
        else:
            print(f"  [Instagram] followers_count field not available (status {user_response.status_code})")
    except Exception as e:
        print(f"  [Instagram] Could not retrieve follower_count: {e}")

    return {
        'monthly_data': monthly_data,
        'follower_count': follower_count
    }


# ============================================================================
# INSTAGRAM - MEDIA INSIGHTS (for shares per post, advocacy)
# ============================================================================

def get_instagram_shares_bulk(instagram_id, page_token, days_back=365):
    """
    Aggregate Instagram shares from individual media posts.

    The 'shares' metric is not available at Instagram account level,
    so we fetch it per media item and aggregate into monthly buckets.

    Returns:
        {'monthly_data': {'YYYY-MM': {'shares': int}}}
    """
    print(f"  [Instagram] Fetching media shares...")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    since_timestamp = int(start_date.timestamp())

    media_url = f"https://graph.facebook.com/{API_VERSION}/{instagram_id}/media"
    media_params = {
        'fields': 'id,caption,timestamp,like_count,comments_count',
        'since': since_timestamp,
        'access_token': page_token,
        'limit': 100
    }

    try:
        media_response = requests.get(media_url, params=media_params, timeout=30)
        if media_response.status_code != 200:
            print(f"  [Instagram] Media API error {media_response.status_code}")
            try:
                print(f"  [Instagram] {json.dumps(media_response.json(), indent=2)}")
            except Exception:
                print(f"  [Instagram] {media_response.text}")
        media_response.raise_for_status()
        media_items = media_response.json().get('data', [])
        print(f"  [Instagram] Found {len(media_items)} media items")
    except Exception as e:
        print(f"  [Instagram] Failed to get media: {e}")
        import traceback
        traceback.print_exc()
        return {}

    monthly_data = {}
    all_posts = []  # Per-post records for top performers

    for media in media_items:
        try:
            media_id = media['id']
            timestamp = media.get('timestamp', '')
            if not timestamp:
                continue

            media_date = datetime.strptime(timestamp[:10], '%Y-%m-%d')
            month_key = f"{media_date.year}-{media_date.month:02d}"

            if month_key not in monthly_data:
                monthly_data[month_key] = {'shares': 0}

            like_count = media.get('like_count', 0)
            comments_count = media.get('comments_count', 0)

            post_shares = 0
            insights_url = f"https://graph.facebook.com/{API_VERSION}/{media_id}/insights"
            insights_params = {
                'metric': 'shares',
                'access_token': page_token
            }

            insights_response = requests.get(insights_url, params=insights_params, timeout=10)
            if insights_response.status_code == 200:
                for insight in insights_response.json().get('data', []):
                    if insight.get('name') == 'shares':
                        values = insight.get('values', [{}])
                        value = values[0].get('value', 0) if values else 0
                        monthly_data[month_key]['shares'] += value
                        post_shares = value
            else:
                # Shares may not be available for all media types; skip silently
                pass

            # Track per-post data for top performers
            caption = media.get('caption', '')
            title = (caption[:80] + '...') if len(caption) > 80 else caption
            if not title:
                title = f"Post {media_date.strftime('%b %d, %Y')}"
            engagement = like_count + comments_count
            all_posts.append({
                'id': media_id,
                'title': title,
                'date': timestamp[:10],
                'month': month_key,
                'engagement': engagement,
                'likes': like_count,
                'comments': comments_count,
                'shares': post_shares,
            })

        except Exception as e:
            print(f"    [Instagram] Warning: failed to process media {media.get('id', 'unknown')}: {e}")
            continue

    print(f"  [Instagram] Media shares collected for {len(monthly_data)} months, {len(all_posts)} posts")
    return {'monthly_data': monthly_data, 'all_posts': all_posts}


# ============================================================================
# MAIN COLLECTION FUNCTION - RETURNS PLATFORM-SEPARATED METRICS
# ============================================================================

def collect_social_media_real_metrics(page_id, page_token, instagram_id, days_back=365):
    """
    Collect all real metrics from Facebook and Instagram, separated by platform.

    Returns a dict keyed by month ("YYYY-MM") where each month has:
      'facebook': {
          'reach':      int  (post_impressions_unique)
          'engagement': int  (reactions + comments)
          'conversion': int  (post_clicks)
          'retention':  int  (fan_count - same for all months)
          'advocacy':   int  (shares from post fields)
      }
      'instagram': {
          'reach':      int  (account-level reach)
          'engagement': int  (accounts_engaged)
          'conversion': int  (profile_link_taps, or 0 if unavailable)
          'retention':  int  (follower_count - same for all months)
          'advocacy':   int  (shares aggregated from media)
      }

    Also includes top-level 'current_followers' for both platforms.
    """
    print("\n" + "=" * 70)
    print("COLLECTING REAL SOCIAL MEDIA METRICS (PLATFORM-SEPARATED)")
    print("=" * 70)

    # Collect from all sources
    fb_data = get_facebook_metrics_bulk(page_id, page_token, days_back)
    ig_account_data = get_instagram_account_metrics_bulk(instagram_id, page_token, days_back)
    ig_shares_data = get_instagram_shares_bulk(instagram_id, page_token, days_back)

    fb_monthly = fb_data.get('monthly_data', {})
    ig_account_monthly = ig_account_data.get('monthly_data', {})
    ig_shares_monthly = ig_shares_data.get('monthly_data', {})

    all_months = set()
    all_months.update(fb_monthly.keys())
    all_months.update(ig_account_monthly.keys())
    all_months.update(ig_shares_monthly.keys())

    result = {}

    for month in sorted(all_months):
        fb = fb_monthly.get(month, {})
        ig_acc = ig_account_monthly.get(month, {})
        ig_shr = ig_shares_monthly.get(month, {})

        result[month] = {
            'facebook': {
                'reach': fb.get('reach', 0),
                'engagement': fb.get('reactions', 0) + fb.get('comments', 0),
                'conversion': fb.get('clicks', 0),
                'retention': fb_data.get('fan_count', 0),
                'advocacy': fb.get('shares', 0)
            },
            'instagram': {
                'reach': ig_acc.get('reach', 0),
                'engagement': ig_acc.get('accounts_engaged', 0),
                'conversion': ig_acc.get('link_taps', 0),
                'retention': ig_account_data.get('follower_count', 0),
                'advocacy': ig_shr.get('shares', 0)
            }
        }

    result['current_followers'] = {
        'facebook_fans': fb_data.get('fan_count', 0),
        'instagram_followers': ig_account_data.get('follower_count', 0)
    }

    print("\n" + "=" * 70)
    print(f"COLLECTION COMPLETE - {len(result) - 1} months of REAL data")
    print("=" * 70)

    return result


# ============================================================================
# HELPER FUNCTIONS FOR DATA COLLECTOR
# ============================================================================

def get_all_pages_and_instagram_accounts(system_token):
    """
    Get all Facebook pages accessible by the system user token
    and their linked Instagram Business accounts.

    Returns: List of account dictionaries with page and Instagram info.
    """
    url = f"https://graph.facebook.com/{API_VERSION}/me/accounts"
    params = {'access_token': system_token}

    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            print(f"[Facebook] Pages API error {response.status_code}")
            try:
                print(f"[Facebook] {json.dumps(response.json(), indent=2)}")
            except Exception:
                print(f"[Facebook] {response.text}")
        response.raise_for_status()
        pages_data = response.json().get('data', [])
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to fetch Facebook pages: {e}")
        import traceback
        traceback.print_exc()
        return []

    accounts = []
    for page in pages_data:
        page_id = page.get('id')
        page_token = page.get('access_token')
        page_name = page.get('name')

        if not page_id or not page_token or not page_name:
            print(f"  [WARNING] Incomplete page data: {page}")
            continue

        token_preview = (page_token[:20] + '...' + page_token[-10:]
                         if len(page_token) > 30 else 'TOKEN_TOO_SHORT')
        print(f"  Page '{page_name}' (ID: {page_id})")
        print(f"    Token: {token_preview}")

        # Get linked Instagram account
        ig_url = f"https://graph.facebook.com/{API_VERSION}/{page_id}"
        ig_params = {
            'fields': 'instagram_business_account,fan_count,followers_count',
            'access_token': system_token
        }

        instagram_id = None
        fan_count = 0
        followers_count = 0

        try:
            ig_response = requests.get(ig_url, params=ig_params, timeout=30)
            if ig_response.status_code != 200:
                print(f"  [WARNING] Page details error {ig_response.status_code} for '{page_name}'")
                try:
                    print(f"  [WARNING] {json.dumps(ig_response.json(), indent=2)}")
                except Exception:
                    pass
            ig_response.raise_for_status()
            ig_data = ig_response.json()
            instagram_id = ig_data.get('instagram_business_account', {}).get('id')
            fan_count = ig_data.get('fan_count', 0)
            followers_count = ig_data.get('followers_count', 0)
        except requests.exceptions.RequestException as e:
            print(f"  [WARNING] Failed to get Instagram for '{page_name}': {e}")

        accounts.append({
            'page_name': page_name,
            'page_id': page_id,
            'page_token': page_token,
            'instagram_id': instagram_id,
            'fan_count': fan_count,
            'followers_count': followers_count
        })

    return accounts


# ============================================================================
# LEGACY FUNCTIONS - kept for backward compatibility with data_collector.py
# These are called by collect_social_bulk in data_collector.py and will be
# replaced when data_collector.py is updated to use the new separated approach.
# ============================================================================

def get_facebook_posts_engagement(page_id, page_token, days_back=365):
    """
    Legacy wrapper - returns Facebook post engagement data.
    Now internally uses get_facebook_metrics_bulk.
    """
    fb_data = get_facebook_metrics_bulk(page_id, page_token, days_back)
    # Reformat to match legacy expected structure
    legacy_monthly = {}
    for month_key, data in fb_data.get('monthly_data', {}).items():
        legacy_monthly[month_key] = {
            'reactions': data.get('reactions', 0),
            'comments': data.get('comments', 0),
            'shares': data.get('shares', 0),
            'posts_count': data.get('posts', 0)
        }
    return {'monthly_data': legacy_monthly}


def get_instagram_account_insights(instagram_id, page_token, days_back=7):
    """
    Legacy wrapper - returns Instagram account insights in the old format.
    Now internally uses get_instagram_account_metrics_bulk.
    """
    ig_data = get_instagram_account_metrics_bulk(instagram_id, page_token, days_back)
    monthly = ig_data.get('monthly_data', {})

    # Build legacy format: lists of {end_time, value} dicts
    reach_values = []
    accounts_engaged_values = []

    for month_key, m in monthly.items():
        # Use first day of month as synthetic end_time
        year, month = month_key.split('-')
        end_time = f"{year}-{month}-01T00:00:00+0000"
        reach_values.append({'end_time': end_time, 'value': m.get('reach', 0)})
        accounts_engaged_values.append({'end_time': end_time, 'value': m.get('accounts_engaged', 0)})

    result = {}
    if reach_values:
        result['reach'] = reach_values
    if accounts_engaged_values:
        result['accounts_engaged'] = accounts_engaged_values

    # Add follower count
    follower_count = ig_data.get('follower_count', 0)
    if follower_count:
        result['follower_count'] = [{'end_time': '', 'value': follower_count}]

    return result


def get_instagram_media_insights(instagram_id, page_token, limit=20):
    """
    Get Instagram media (posts) and their insights.
    Returns a list of recent media items with insight data.
    """
    media_url = f"https://graph.facebook.com/{API_VERSION}/{instagram_id}/media"
    media_params = {
        'fields': 'id,caption,media_type,timestamp,permalink',
        'limit': limit,
        'access_token': page_token
    }

    try:
        media_response = requests.get(media_url, params=media_params, timeout=30)
        if media_response.status_code != 200:
            print(f"  [Instagram] Media API error {media_response.status_code}")
            try:
                print(f"  [Instagram] {json.dumps(media_response.json(), indent=2)}")
            except Exception:
                pass
        media_response.raise_for_status()
        media_list = media_response.json().get('data', [])
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Failed to get Instagram media: {e}")
        import traceback
        traceback.print_exc()
        return []

    media_insights = []
    for media in media_list:
        media_id = media['id']
        insights_url = f"https://graph.facebook.com/{API_VERSION}/{media_id}/insights"
        metrics = ['reach', 'saved', 'likes', 'comments', 'shares', 'total_interactions']

        post_data = {
            'id': media_id,
            'caption': (media.get('caption', '')[:100] + '...'
                        if media.get('caption') and len(media.get('caption', '')) > 100
                        else media.get('caption', '')),
            'media_type': media.get('media_type'),
            'timestamp': media.get('timestamp'),
            'permalink': media.get('permalink'),
            'insights': {}
        }

        for metric in metrics:
            try:
                params = {'metric': metric, 'access_token': page_token}
                response = requests.get(insights_url, params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json().get('data', [])
                    if data:
                        values = data[0].get('values', [{}])
                        post_data['insights'][metric] = values[0].get('value') if values else 0
                else:
                    post_data['insights'][metric] = 0
            except requests.exceptions.RequestException:
                post_data['insights'][metric] = 0

        media_insights.append(post_data)

    return media_insights


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 4:
        print("Usage: python social_media_analytics.py PAGE_ID PAGE_TOKEN INSTAGRAM_ID")
        sys.exit(1)

    page_id = sys.argv[1]
    page_token = sys.argv[2]
    instagram_id = sys.argv[3]

    data = collect_social_media_real_metrics(page_id, page_token, instagram_id, days_back=30)

    print("\nSUMMARY BY MONTH:\n")
    for month in sorted([k for k in data.keys() if k != 'current_followers']):
        print(f"\n{month}:")
        for platform, metrics in data[month].items():
            print(f"  {platform}: {metrics}")
