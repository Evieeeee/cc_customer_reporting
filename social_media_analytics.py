"""
Social Media Analytics - REAL METRICS ONLY
Uses Facebook POST-level insights and Instagram account/media insights
All metrics are actual API-provided values, NO estimates

Customer Journey Stages:
1. Awareness - Reach/Impressions (from post and account insights)
2. Engagement - Engaged users, reactions, comments
3. Conversion - Profile views, website clicks, post clicks
4. Retention - Follower count, saved posts
5. Advocacy - Shares (from post fields)
"""

import requests
from datetime import datetime, timedelta
import json

API_VERSION = "v24.0"

# ============================================================================
# FACEBOOK - POST-LEVEL INSIGHTS (THESE WORK!)
# ============================================================================

def get_facebook_post_insights_bulk(page_id, page_token, days_back=365):
    """
    Get Facebook POST-LEVEL insights (not page-level, those are deprecated)
    
    Returns REAL metrics by aggregating from individual posts:
    - Reach (post_impressions_unique)
    - Impressions (post_impressions)
    - Engagement (post_engaged_users)
    - Clicks (post_clicks)
    - Reactions/Comments/Shares (from post fields)
    
    All metrics are REAL, not estimates!
    """
    print(f"  [Facebook] Fetching posts from last {days_back} days...")
    
    # Calculate timestamp
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    since_timestamp = int(start_date.timestamp())
    
    # Step 1: Get posts with engagement fields
    posts_url = f"https://graph.facebook.com/{API_VERSION}/{page_id}/posts"
    posts_params = {
        'fields': 'id,created_time,shares,reactions.summary(true),comments.summary(true)',
        'since': since_timestamp,
        'access_token': page_token,
        'limit': 100  # Max posts to analyze
    }
    
    try:
        posts_response = requests.get(posts_url, params=posts_params, timeout=30)
        posts_response.raise_for_status()
        posts_data = posts_response.json().get('data', [])
        print(f"  [Facebook] Found {len(posts_data)} posts")
    except Exception as e:
        print(f"  [Facebook] Failed to get posts: {e}")
        return {}
    
    # Step 2: Get POST-LEVEL insights for each post
    monthly_data = {}
    
    for post in posts_data:
        try:
            post_id = post['id']
            created_time = post.get('created_time', '')
            
            if not created_time:
                continue
            
            # Parse date and determine month
            post_date = datetime.strptime(created_time[:10], '%Y-%m-%d')
            month_key = f"{post_date.year}-{post_date.month:02d}"
            
            # Initialize month bucket
            if month_key not in monthly_data:
                monthly_data[month_key] = {
                    'reach': 0,              # post_impressions_unique (REAL)
                    'impressions': 0,        # post_impressions (REAL)
                    'engaged_users': 0,      # post_engaged_users (REAL)
                    'clicks': 0,             # post_clicks (REAL)
                    'reactions': 0,          # From post fields (REAL)
                    'comments': 0,           # From post fields (REAL)
                    'shares': 0,             # From post fields (REAL)
                    'posts': 0
                }
            
            monthly_data[month_key]['posts'] += 1
            
            # Get post fields (reactions, comments, shares)
            reactions = post.get('reactions', {}).get('summary', {}).get('total_count', 0)
            comments = post.get('comments', {}).get('summary', {}).get('total_count', 0)
            shares = post.get('shares', {}).get('count', 0)
            
            monthly_data[month_key]['reactions'] += reactions
            monthly_data[month_key]['comments'] += comments
            monthly_data[month_key]['shares'] += shares
            
            # Get post-level insights (THE KEY PART - THESE STILL WORK!)
            insights_url = f"https://graph.facebook.com/{API_VERSION}/{post_id}/insights"
            insights_params = {
                'metric': 'post_impressions,post_impressions_unique,post_engaged_users,post_clicks',
                'access_token': page_token
            }
            
            insights_response = requests.get(insights_url, params=insights_params, timeout=10)
            
            if insights_response.status_code == 200:
                insights_data = insights_response.json().get('data', [])
                
                for insight in insights_data:
                    metric_name = insight.get('name')
                    values = insight.get('values', [{}])
                    value = values[0].get('value', 0) if values else 0
                    
                    if metric_name == 'post_impressions':
                        monthly_data[month_key]['impressions'] += value
                    elif metric_name == 'post_impressions_unique':
                        monthly_data[month_key]['reach'] += value
                    elif metric_name == 'post_engaged_users':
                        monthly_data[month_key]['engaged_users'] += value
                    elif metric_name == 'post_clicks':
                        monthly_data[month_key]['clicks'] += value
            
        except Exception as e:
            print(f"  [Facebook] Warning: Failed to process post: {e}")
            continue
    
    # Get current fan count (still available as page field)
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
    except:
        fan_count = 0
    
    result = {
        'monthly_data': monthly_data,
        'fan_count': fan_count
    }
    
    print(f"  [Facebook] ✓ Collected data for {len(monthly_data)} months")
    return result


# ============================================================================
# INSTAGRAM - ACCOUNT INSIGHTS (THESE STILL WORK!)
# ============================================================================

def get_instagram_insights_bulk(instagram_id, page_token, days_back=365):
    """
    Get Instagram ACCOUNT insights with supported metrics only

    DEPRECATED METRICS (removed Jan 2025):
    - profile_views, website_clicks, phone_call_clicks, text_message_clicks

    SUPPORTED METRICS:
    - Reach (daily aggregation)
    - Impressions (daily aggregation)
    - Follower count (current)

    Makes multiple 30-day API calls to build historical data
    """
    print(f"  [Instagram] Fetching account insights for last {days_back} days...")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    insights_url = f"https://graph.facebook.com/{API_VERSION}/{instagram_id}/insights"

    # Supported metrics only (deprecated metrics removed)
    metrics = 'reach,impressions'

    monthly_data = {}

    # Split into 30-day chunks
    current_chunk_end = end_date
    while current_chunk_end > start_date:
        current_chunk_start = max(current_chunk_end - timedelta(days=30), start_date)

        print(f"    Fetching chunk: {current_chunk_start.strftime('%Y-%m-%d')} to {current_chunk_end.strftime('%Y-%m-%d')}")

        params = {
            'metric': metrics,
            'period': 'day',
            'access_token': page_token,
            'since': current_chunk_start.strftime('%Y-%m-%d'),
            'until': current_chunk_end.strftime('%Y-%m-%d')
        }

        try:
            response = requests.get(insights_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json().get('data', [])

            # Process each metric's daily values
            for metric_obj in data:
                metric_name = metric_obj.get('name')
                values = metric_obj.get('values', [])

                for value_obj in values:
                    value = value_obj.get('value', 0)
                    end_time = value_obj.get('end_time', '')

                    if not end_time:
                        continue

                    # Parse date and determine month
                    date = datetime.strptime(end_time[:10], '%Y-%m-%d')
                    month_key = f"{date.year}-{date.month:02d}"

                    # Initialize month bucket
                    if month_key not in monthly_data:
                        monthly_data[month_key] = {
                            'reach': 0,
                            'impressions': 0
                        }

                    # Aggregate by month
                    if metric_name in monthly_data[month_key]:
                        monthly_data[month_key][metric_name] += value

        except Exception as e:
            print(f"    Warning: Failed chunk {current_chunk_start.strftime('%Y-%m-%d')}: {e}")

        # Move to next chunk
        current_chunk_end = current_chunk_start - timedelta(days=1)

    print(f"  [Instagram] ✓ Collected account data for {len(monthly_data)} months")

    # Get current follower count
    try:
        follower_params = {
            'metric': 'follower_count',
            'period': 'day',
            'access_token': page_token
        }
        follower_response = requests.get(insights_url, params=follower_params, timeout=10)
        follower_data = follower_response.json().get('data', [])
        
        if follower_data and follower_data[0].get('values'):
            follower_count = follower_data[0]['values'][-1].get('value', 0)
            print(f"  [Instagram] Follower count: {follower_count:,}")
        else:
            follower_count = 0
    except:
        follower_count = 0
    
    return {
        'monthly_data': monthly_data,
        'follower_count': follower_count
    }


# ============================================================================
# INSTAGRAM - MEDIA INSIGHTS (SAVED/SHARES PER POST)
# ============================================================================

def get_instagram_media_insights_bulk(instagram_id, page_token, days_back=365):
    """
    Get Instagram MEDIA (post) insights for saved/shares
    These are only available at media level, not account level
    """
    print(f"  [Instagram] Fetching media insights...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    since_timestamp = int(start_date.timestamp())
    
    # Get media
    media_url = f"https://graph.facebook.com/{API_VERSION}/{instagram_id}/media"
    media_params = {
        'fields': 'id,timestamp',
        'since': since_timestamp,
        'access_token': page_token,
        'limit': 100
    }
    
    try:
        media_response = requests.get(media_url, params=media_params, timeout=30)
        media_response.raise_for_status()
        media_items = media_response.json().get('data', [])
        print(f"  [Instagram] Found {len(media_items)} media items")
    except Exception as e:
        print(f"  [Instagram] Failed to get media: {e}")
        return {}
    
    monthly_data = {}
    
    for media in media_items:
        try:
            media_id = media['id']
            timestamp = media.get('timestamp', '')
            
            if not timestamp:
                continue
            
            # Parse date
            media_date = datetime.strptime(timestamp[:10], '%Y-%m-%d')
            month_key = f"{media_date.year}-{media_date.month:02d}"
            
            # Initialize month bucket
            if month_key not in monthly_data:
                monthly_data[month_key] = {
                    'saved': 0,
                    'shares': 0
                }
            
            # Get media insights
            insights_url = f"https://graph.facebook.com/{API_VERSION}/{media_id}/insights"
            insights_params = {
                'metric': 'saved,shares',
                'access_token': page_token
            }
            
            insights_response = requests.get(insights_url, params=insights_params, timeout=10)
            
            if insights_response.status_code == 200:
                insights_data = insights_response.json().get('data', [])
                
                for insight in insights_data:
                    metric_name = insight.get('name')
                    values = insight.get('values', [{}])
                    value = values[0].get('value', 0) if values else 0
                    
                    if metric_name in monthly_data[month_key]:
                        monthly_data[month_key][metric_name] += value
        
        except Exception as e:
            continue
    
    print(f"  [Instagram] ✓ Collected media data for {len(monthly_data)} months")
    return {'monthly_data': monthly_data}


# ============================================================================
# MAIN COLLECTION FUNCTION
# ============================================================================

def collect_social_media_real_metrics(page_id, page_token, instagram_id, days_back=365):
    """
    Collect ALL real metrics from Facebook and Instagram
    Maps to customer journey stages with REAL data only
    
    Returns monthly data with these REAL metrics:
    
    Awareness:
    - Facebook reach (post_impressions_unique)
    - Facebook impressions (post_impressions)
    - Instagram reach
    - Instagram impressions
    
    Engagement:
    - Facebook engaged users (post_engaged_users)
    - Facebook reactions
    - Facebook comments
    
    Conversion:
    - Facebook clicks (post_clicks)
    # Note: Instagram profile_views and website_clicks deprecated Jan 2025
    
    Retention:
    - Facebook fan count
    - Instagram follower count
    - Instagram saved posts
    
    Advocacy:
    - Facebook shares
    - Instagram shares
    """
    print("\n" + "="*70)
    print("COLLECTING REAL SOCIAL MEDIA METRICS")
    print("="*70)
    
    # Collect from all sources
    fb_data = get_facebook_post_insights_bulk(page_id, page_token, days_back)
    ig_account_data = get_instagram_insights_bulk(instagram_id, page_token, days_back)
    ig_media_data = get_instagram_media_insights_bulk(instagram_id, page_token, days_back)
    
    # Merge by month
    all_months = set()
    all_months.update(fb_data.get('monthly_data', {}).keys())
    all_months.update(ig_account_data.get('monthly_data', {}).keys())
    all_months.update(ig_media_data.get('monthly_data', {}).keys())
    
    result = {}
    
    for month in sorted(all_months):
        fb_month = fb_data.get('monthly_data', {}).get(month, {})
        ig_account_month = ig_account_data.get('monthly_data', {}).get(month, {})
        ig_media_month = ig_media_data.get('monthly_data', {}).get(month, {})
        
        result[month] = {
            # AWARENESS
            'awareness': {
                'reach': fb_month.get('reach', 0) + ig_account_month.get('reach', 0),
                'impressions': fb_month.get('impressions', 0) + ig_account_month.get('impressions', 0)
            },
            # ENGAGEMENT
            'engagement': {
                'engaged_users': fb_month.get('engaged_users', 0),
                'reactions': fb_month.get('reactions', 0),
                'comments': fb_month.get('comments', 0)
            },
            # CONVERSION
            'conversion': {
                'clicks': fb_month.get('clicks', 0)
                # Note: Instagram profile_views and website_clicks deprecated Jan 2025
            },
            # RETENTION
            'retention': {
                'saved': ig_media_month.get('saved', 0)
            },
            # ADVOCACY
            'advocacy': {
                'shares': fb_month.get('shares', 0) + ig_media_month.get('shares', 0)
            }
        }
    
    # Add current follower/fan counts
    result['current_followers'] = {
        'facebook_fans': fb_data.get('fan_count', 0),
        'instagram_followers': ig_account_data.get('follower_count', 0)
    }
    
    print("\n" + "="*70)
    print(f"✓ COLLECTION COMPLETE - {len(result)-1} months of REAL data")
    print("="*70)
    
    return result


# ============================================================================
# HELPER FUNCTIONS FOR DATA COLLECTOR
# ============================================================================

def get_all_pages_and_instagram_accounts(system_token):
    """
    Get all Facebook pages accessible by the system user token
    and their linked Instagram Business accounts

    Returns: List of account dictionaries with page and Instagram info
    """
    url = f"https://graph.facebook.com/{API_VERSION}/me/accounts"
    params = {'access_token': system_token}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        pages_data = response.json().get('data', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching pages: {e}")
        return []

    accounts = []
    for page in pages_data:
        page_id = page['id']
        page_token = page['access_token']
        page_name = page['name']

        # Get Instagram account if linked
        ig_url = f"https://graph.facebook.com/{API_VERSION}/{page_id}"
        ig_params = {
            'fields': 'instagram_business_account,fan_count,followers_count',
            'access_token': system_token
        }

        try:
            ig_response = requests.get(ig_url, params=ig_params, timeout=30)
            ig_response.raise_for_status()
            ig_data = ig_response.json()

            instagram_id = ig_data.get('instagram_business_account', {}).get('id')
            fan_count = ig_data.get('fan_count', 0)
            followers_count = ig_data.get('followers_count', 0)
        except requests.exceptions.RequestException:
            instagram_id = None
            fan_count = 0
            followers_count = 0

        accounts.append({
            'page_name': page_name,
            'page_id': page_id,
            'page_token': page_token,
            'instagram_id': instagram_id,
            'fan_count': fan_count,
            'followers_count': followers_count
        })

    return accounts


def get_facebook_page_insights(page_id, page_token, days_back=7):
    """
    Get Facebook page-level insights with daily period granularity
    Uses period='day' for native API monthly segmentation

    UPDATED FOR 2026: Impressions metrics deprecated November 2025
    Now using Views metrics (page_media_view, post_media_view)

    Args:
        page_id: Facebook page ID
        page_token: Page access token
        days_back: Number of days of historical data to retrieve

    Returns: Dictionary of insights by metric with daily values
    """
    # Updated metrics for v24.0 (November 2025 deprecations applied)
    # Replaced impressions with views metrics
    metrics = [
        'page_post_engagements',      # Still works
        'page_media_view',             # Replaces page_impressions_unique
        'page_engaged_users',          # Still works
        'page_post_reactions_like',    # Reactions still work
        'page_post_reactions_love',
        'page_post_reactions_wow',
        'page_post_reactions_haha',
        'page_post_reactions_sorry',
        'page_post_reactions_anger',
        'page_actions_post_reactions_total'  # Total reactions
    ]

    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    insights = {}
    for metric in metrics:
        url = f"https://graph.facebook.com/{API_VERSION}/{page_id}/insights"
        params = {
            'metric': metric,
            'period': 'day',  # Native API daily granularity for monthly segmentation
            'access_token': page_token,
            'since': start_date.strftime('%Y-%m-%d'),
            'until': end_date.strftime('%Y-%m-%d')
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json().get('data', [])
            if data:
                insights[metric] = {
                    'name': data[0].get('name'),
                    'description': data[0].get('description'),
                    'values': data[0].get('values', [])
                }
        except requests.exceptions.RequestException as e:
            print(f"  Warning: Failed to fetch {metric}: {e}")
            insights[metric] = {'error': str(e)}

    return insights


def get_instagram_account_insights(instagram_id, page_token, days_back=7):
    """
    Get Instagram account-level insights with daily period granularity
    Makes multiple 30-day API calls to build historical data up to 12 months

    DEPRECATED METRICS (removed as of Jan 2025):
    - profile_views, website_clicks, phone_call_clicks, text_message_clicks

    SUPPORTED METRICS:
    - reach, impressions, follower_count

    Args:
        instagram_id: Instagram Business Account ID
        page_token: Page access token
        days_back: Number of days of historical data (will split into 30-day chunks)

    Returns: Dictionary of account insights with daily values across entire period
    """
    insights = {}

    url = f"https://graph.facebook.com/{API_VERSION}/{instagram_id}/insights"

    # Supported metrics only (deprecated metrics removed Jan 2025)
    daily_metrics = ['reach', 'impressions']

    # Calculate how many 30-day chunks we need
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    # Split into 30-day chunks (Instagram API limit)
    current_chunk_end = end_date
    all_values = {metric: [] for metric in daily_metrics}

    while current_chunk_end > start_date:
        current_chunk_start = max(current_chunk_end - timedelta(days=30), start_date)

        print(f"    Fetching Instagram {current_chunk_start.strftime('%Y-%m-%d')} to {current_chunk_end.strftime('%Y-%m-%d')}")

        for metric in daily_metrics:
            try:
                params = {
                    'metric': metric,
                    'period': 'day',
                    'since': current_chunk_start.strftime('%Y-%m-%d'),
                    'until': current_chunk_end.strftime('%Y-%m-%d'),
                    'access_token': page_token
                }
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json().get('data', [])
                if data:
                    values = data[0].get('values', [])
                    all_values[metric].extend(values)
            except requests.exceptions.RequestException as e:
                print(f"      Warning: Failed to fetch {metric}: {e}")

        # Move to next chunk (go backwards in time)
        current_chunk_end = current_chunk_start - timedelta(days=1)

    # Store aggregated values
    for metric in daily_metrics:
        if all_values[metric]:
            insights[metric] = all_values[metric]

    # Follower count (lifetime metric, returns current value only)
    try:
        params = {
            'metric': 'follower_count',
            'period': 'day',
            'access_token': page_token
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json().get('data', [])
        if data:
            insights['follower_count'] = data[0].get('values', [])
    except requests.exceptions.RequestException as e:
        print(f"  Warning: Failed to fetch follower_count: {e}")

    return insights


def get_instagram_media_insights(instagram_id, page_token, limit=20):
    """
    Get Instagram media (posts) and their insights

    Args:
        instagram_id: Instagram Business Account ID
        page_token: Page access token
        limit: Number of recent posts to retrieve

    Returns: List of media with insights
    """
    # Get recent media
    media_url = f"https://graph.facebook.com/{API_VERSION}/{instagram_id}/media"
    media_params = {
        'fields': 'id,caption,media_type,timestamp,permalink',
        'limit': limit,
        'access_token': page_token
    }

    try:
        media_response = requests.get(media_url, params=media_params, timeout=30)
        media_response.raise_for_status()
        media_list = media_response.json().get('data', [])
    except requests.exceptions.RequestException as e:
        print(f"  Warning: Failed to fetch media: {e}")
        return []

    media_insights = []
    for media in media_list:
        media_id = media['id']

        # Get insights for this media
        insights_url = f"https://graph.facebook.com/{API_VERSION}/{media_id}/insights"
        metrics = ['reach', 'saved', 'likes', 'comments', 'shares', 'total_interactions']

        post_data = {
            'id': media_id,
            'caption': media.get('caption', '')[:100] + '...' if media.get('caption') and len(media.get('caption', '')) > 100 else media.get('caption', ''),
            'media_type': media.get('media_type'),
            'timestamp': media.get('timestamp'),
            'permalink': media.get('permalink'),
            'insights': {}
        }

        for metric in metrics:
            try:
                params = {'metric': metric, 'access_token': page_token}
                response = requests.get(insights_url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json().get('data', [])
                if data:
                    values = data[0].get('values', [{}])
                    post_data['insights'][metric] = values[0].get('value') if values else 0
            except requests.exceptions.RequestException:
                post_data['insights'][metric] = 0

        media_insights.append(post_data)

    return media_insights


if __name__ == '__main__':
    # Test with your credentials
    import sys

    if len(sys.argv) < 4:
        print("Usage: python real_metrics.py PAGE_ID PAGE_TOKEN INSTAGRAM_ID")
        sys.exit(1)

    page_id = sys.argv[1]
    page_token = sys.argv[2]
    instagram_id = sys.argv[3]

    data = collect_social_media_real_metrics(page_id, page_token, instagram_id, days_back=30)

    # Print summary
    print("\nSUMMARY BY MONTH:\n")
    for month in sorted([k for k in data.keys() if k != 'current_followers']):
        print(f"{month}:")
        for stage, metrics in data[month].items():
            print(f"  {stage}: {metrics}")
