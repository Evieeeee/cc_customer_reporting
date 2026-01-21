"""
Social Media Analytics Automation Script
Pulls Facebook Page and Instagram insights for all connected accounts
"""

import requests
from datetime import datetime, timedelta
import json
import csv

# ============================================================================
# CONFIGURATION
# ============================================================================
SYSTEM_USER_TOKEN = "YOUR_SYSTEM_USER_TOKEN_HERE"  # Replace with your token
API_VERSION = "v21.0"

# ============================================================================
# ACCOUNT DISCOVERY
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

# ============================================================================
# FACEBOOK PAGE INSIGHTS
# ============================================================================

def get_facebook_page_insights(page_id, page_token, days_back=7):
    """
    Get Facebook page-level insights
    
    Args:
        page_id: Facebook page ID
        page_token: Page access token
        days_back: Number of days of historical data to retrieve
    
    Returns: Dictionary of insights by metric
    """
    metrics = [
        'page_post_engagements',
        'page_impressions_unique',
        'page_posts_impressions',
        'page_posts_impressions_unique',
        'page_actions_post_reactions_total',
        'page_video_views',
        'page_total_actions'
    ]
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    insights = {}
    for metric in metrics:
        url = f"https://graph.facebook.com/{API_VERSION}/{page_id}/insights"
        params = {
            'metric': metric,
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

# ============================================================================
# INSTAGRAM ACCOUNT INSIGHTS
# ============================================================================

def get_instagram_account_insights(instagram_id, page_token, days_back=7):
    """
    Get Instagram account-level insights
    
    Args:
        instagram_id: Instagram Business Account ID
        page_token: Page access token
        days_back: Number of days of historical data
    
    Returns: Dictionary of account insights
    """
    insights = {}
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    url = f"https://graph.facebook.com/{API_VERSION}/{instagram_id}/insights"
    
    # Reach metric
    try:
        params = {
            'metric': 'reach',
            'period': 'day',
            'access_token': page_token,
            'since': start_date.strftime('%Y-%m-%d'),
            'until': end_date.strftime('%Y-%m-%d')
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json().get('data', [])
        if data:
            insights['reach'] = data[0].get('values', [])
    except requests.exceptions.RequestException as e:
        print(f"  Warning: Failed to fetch reach: {e}")
    
    # Follower count
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
    
    # Profile views (total_value metric)
    try:
        params = {
            'metric': 'profile_views',
            'period': 'day',
            'metric_type': 'total_value',
            'access_token': page_token
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json().get('data', [])
        if data:
            insights['profile_views'] = data[0].get('total_value', {})
    except requests.exceptions.RequestException as e:
        print(f"  Warning: Failed to fetch profile_views: {e}")
    
    # Website clicks (total_value metric)
    try:
        params = {
            'metric': 'website_clicks',
            'period': 'day',
            'metric_type': 'total_value',
            'access_token': page_token
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json().get('data', [])
        if data:
            insights['website_clicks'] = data[0].get('total_value', {})
    except requests.exceptions.RequestException as e:
        print(f"  Warning: Failed to fetch website_clicks: {e}")
    
    return insights

# ============================================================================
# INSTAGRAM MEDIA INSIGHTS
# ============================================================================

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

# ============================================================================
# EXPORT FUNCTIONS
# ============================================================================

def export_to_json(data, filename):
    """Export data to JSON file"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"JSON exported to: {filename}")

def export_to_csv_summary(data, filename):
    """Export summary data to CSV"""
    rows = []
    
    for account in data:
        # Facebook summary
        fb_row = {
            'Account Name': account['account_name'],
            'Platform': 'Facebook',
            'Date': account['date'],
            'Fans': account.get('fan_count', 0),
            'Followers': account.get('followers_count', 0)
        }
        
        # Add latest Facebook metrics
        for metric, values in account.get('facebook', {}).items():
            if isinstance(values, dict) and 'values' in values:
                latest_value = values['values'][-1]['value'] if values['values'] else 0
                fb_row[metric] = latest_value
        
        rows.append(fb_row)
        
        # Instagram summary
        if account.get('instagram', {}).get('account'):
            ig_row = {
                'Account Name': account['account_name'],
                'Platform': 'Instagram',
                'Date': account['date']
            }
            
            ig_account = account['instagram']['account']
            
            # Add Instagram account metrics
            if 'follower_count' in ig_account:
                latest_followers = ig_account['follower_count'][-1]['value'] if ig_account['follower_count'] else 0
                ig_row['Followers'] = latest_followers
            
            if 'reach' in ig_account:
                latest_reach = ig_account['reach'][-1]['value'] if ig_account['reach'] else 0
                ig_row['Reach'] = latest_reach
            
            if 'profile_views' in ig_account:
                ig_row['Profile Views'] = ig_account['profile_views'].get('value', 0)
            
            if 'website_clicks' in ig_account:
                ig_row['Website Clicks'] = ig_account['website_clicks'].get('value', 0)
            
            # Add media stats
            media = account['instagram'].get('media', [])
            if media:
                total_likes = sum(m['insights'].get('likes', 0) for m in media)
                total_comments = sum(m['insights'].get('comments', 0) for m in media)
                total_shares = sum(m['insights'].get('shares', 0) for m in media)
                total_saved = sum(m['insights'].get('saved', 0) for m in media)
                
                ig_row['Total Posts'] = len(media)
                ig_row['Total Likes'] = total_likes
                ig_row['Total Comments'] = total_comments
                ig_row['Total Shares'] = total_shares
                ig_row['Total Saved'] = total_saved
            
            rows.append(ig_row)
    
    if rows:
        # Collect all unique fieldnames from all rows
        all_fieldnames = []
        for row in rows:
            for key in row.keys():
                if key not in all_fieldnames:
                    all_fieldnames.append(key)
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=all_fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"CSV summary exported to: {filename}")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 70)
    print("SOCIAL MEDIA ANALYTICS AUTOMATION")
    print("=" * 70)
    
    # Fetch all accounts
    print("\n[1/4] Discovering accounts...")
    accounts = get_all_pages_and_instagram_accounts(SYSTEM_USER_TOKEN)
    
    if not accounts:
        print("ERROR: No accounts found. Check your system user token.")
        return
    
    print(f"Found {len(accounts)} account(s)")
    
    all_data = []
    
    # Process each account
    print("\n[2/4] Fetching insights...")
    for i, account in enumerate(accounts, 1):
        print(f"\n[{i}/{len(accounts)}] Processing: {account['page_name']}")
        
        account_data = {
            'account_name': account['page_name'],
            'date': datetime.now().isoformat(),
            'fan_count': account['fan_count'],
            'followers_count': account['followers_count'],
            'facebook': {},
            'instagram': {}
        }
        
        # Get Facebook insights
        print("  - Fetching Facebook page insights...")
        account_data['facebook'] = get_facebook_page_insights(
            account['page_id'],
            account['page_token'],
            days_back=7
        )
        
        # Get Instagram insights if account exists
        if account['instagram_id']:
            print("  - Fetching Instagram account insights...")
            account_data['instagram']['account'] = get_instagram_account_insights(
                account['instagram_id'],
                account['page_token'],
                days_back=7
            )
            
            print("  - Fetching Instagram media insights...")
            account_data['instagram']['media'] = get_instagram_media_insights(
                account['instagram_id'],
                account['page_token'],
                limit=20
            )
        else:
            print("  - No Instagram account linked")
        
        all_data.append(account_data)
    
    # Export data
    print("\n[3/4] Exporting data...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # JSON export
    json_filename = f"social_media_insights_{timestamp}.json"
    export_to_json(all_data, json_filename)
    
    # CSV summary export
    csv_filename = f"social_media_summary_{timestamp}.csv"
    export_to_csv_summary(all_data, csv_filename)
    
    print("\n[4/4] Complete!")
    print("=" * 70)
    print(f"Processed {len(accounts)} account(s)")
    print(f"Files created:")
    print(f"  - {json_filename}")
    print(f"  - {csv_filename}")
    print("=" * 70)

if __name__ == "__main__":
    main()
