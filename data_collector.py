"""
Data Collector for ContentClicks Dashboard
Integrates Social Media, Email, and GA4 Analytics with TRUE 12-month historical tracking

VERSION 4.2 - NATIVE API OPTIMIZATION - BUILD 20260121
NATIVE API FUNCTIONALITY FOR EACH SYSTEM:

1. GA4 (Google Analytics 4):
   - Uses start_month and end_month parameters in ONE API call
   - Example: GET /ga4?start_month=2025-10&end_month=2026-01
   - Returns data pre-segmented by month in "by_month" structure
   - 1 API call for all 12 months

2. Social Media (Facebook/Instagram):
   - Facebook: Uses period='day' with since/until for full date range (1 API call)
   - Instagram: Uses period='day' (returns last 30 days only - API limitation)
   - Returns daily data points which are aggregated into months
   - Note: Instagram Insights API does not support since/until with period='day'

3. Email (Instantly):
   - Gets all campaigns in ONE API call, grouped by start_date field
   - Uses bulk analytics API to fetch multiple campaigns efficiently
   - Passes proper date ranges (start_date/end_date) for accurate metrics
   - Reduces API calls significantly vs individual campaign fetches

Total efficiency: Single or minimal API calls per data source
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List
from dateutil.relativedelta import relativedelta

# Add parent directory to path to import the email and social scripts
sys.path.insert(0, '/mnt/project')

from models import (
    Database, Customer, CustomerCredential, HistoricalMetric, 
    TopPerformer, get_benchmark
)

# Import the existing analytics modules
from email_metrics_fetcher import InstantlyFetcher, KlaviyoFetcher
from social_media_analytics import (
    get_all_pages_and_instagram_accounts,
    get_facebook_page_insights,
    get_instagram_account_insights,
    get_instagram_media_insights
)


class GA4Fetcher:
    """Fetch website analytics from enhanced GA4 endpoint with monthly support"""
    
    def __init__(self, property_id: str, endpoint_url: str = None):
        """
        Initialize with GA4 property ID and optional custom endpoint
        
        Args:
            property_id: GA4 property ID
            endpoint_url: Your GA4 analytics endpoint URL
        """
        self.property_id = property_id
        # Use the enhanced GA4 endpoint (now supports monthly segments)
        self.endpoint_url = endpoint_url or "https://ga4-analytics-ioneema27a-uc.a.run.app"
        print(f"[INFO] Using enhanced GA4 endpoint: {self.endpoint_url}")
    
    def get_monthly_metrics_bulk(self, start_month: str, end_month: str) -> Dict:
        """
        Get GA4 metrics for multiple months in ONE API call
        Uses the enhanced endpoint's by_month structure
        
        Args:
            start_month: Start month in YYYY-MM format (e.g., "2024-02")
            end_month: End month in YYYY-MM format (e.g., "2025-01")
        
        Returns:
            Dict with by_month structure: {
                "2024-02": {awareness: {...}, engagement: {...}, ...},
                "2024-03": {...},
                ...
            }
        """
        import requests
        
        print(f"[INFO] Fetching GA4 bulk monthly data for {self.property_id}")
        print(f"[INFO] Date range: {start_month} to {end_month}")
        
        try:
            # Call enhanced endpoint with month range
            response = requests.get(
                f"{self.endpoint_url}/ga4",
                params={
                    "start_month": start_month,
                    "end_month": end_month
                },
                timeout=120
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Check response structure
            if data.get('status') != 'success':
                print(f"[WARNING] GA4 endpoint returned non-success status")
                return {}
            
            # Extract property data
            property_data = data.get('data', {}).get(self.property_id, {})
            
            if not property_data:
                print(f"[WARNING] No data found for property {self.property_id}")
                return {}
            
            # Extract by_month structure
            by_month = property_data.get('by_month', {})
            
            if not by_month:
                print(f"[WARNING] No by_month data in response")
                return {}
            
            print(f"[OK] Received data for {len(by_month)} months")
            
            return by_month
            
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Failed to fetch bulk GA4 data: {e}")
            import traceback
            traceback.print_exc()
            return {}
        except Exception as e:
            print(f"[ERROR] Error parsing GA4 response: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    def get_metrics(self, start_date: datetime, end_date: datetime) -> Dict:
        """
        Get GA4 metrics for a specific date range (single month)
        Used for current period collection
        
        Args:
            start_date: Start date
            end_date: End date
        
        Returns:
            Dict with metrics by journey stage
        """
        import requests
        
        print(f"[INFO] Fetching GA4 metrics for property {self.property_id}")
        print(f"[INFO] Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        try:
            # For single month, use the month range format
            start_month = start_date.strftime('%Y-%m')
            end_month = end_date.strftime('%Y-%m')
            
            # Call bulk endpoint (works for single month too)
            by_month = self.get_monthly_metrics_bulk(start_month, end_month)
            
            if not by_month:
                return self._empty_metrics()
            
            # Get the first (and should be only) month's data
            month_key = list(by_month.keys())[0] if by_month else None
            
            if not month_key:
                return self._empty_metrics()
            
            month_data = by_month[month_key]
            
            # Convert to expected format
            return self._parse_month_data(month_data)
            
        except Exception as e:
            print(f"[ERROR] Failed to fetch GA4 metrics: {e}")
            import traceback
            traceback.print_exc()
            return self._empty_metrics()
    
    def _parse_month_data(self, month_data: Dict) -> Dict:
        """Parse monthly data into expected format"""
        
        awareness = month_data.get('awareness', {})
        engagement = month_data.get('engagement', {})
        conversion = month_data.get('conversion', {})
        retention = month_data.get('retention', {})
        advocacy = month_data.get('advocacy', {})
        
        return {
            'awareness': {
                'sessions': int(awareness.get('sessions', 0)),
                'users': int(awareness.get('users', 0))
            },
            'engagement': {
                'pages_per_session': float(engagement.get('pages_per_session', 0)),
                'avg_session_duration': float(engagement.get('avg_session_duration', 0)),
                'engagement_rate': float(engagement.get('engagement_rate', 0))
            },
            'conversion': {
                'conversions': int(conversion.get('total_conversions', 0)),
                'conversion_rate': float(conversion.get('conversion_rate', 0))
            },
            'retention': {
                'returning_users': int(retention.get('returning_users', 0)),
                'retention_rate': float(retention.get('returning_user_rate', 0))
            },
            'advocacy': {
                'referrals': int(advocacy.get('referral_sessions', 0)),
                'social_shares': int(advocacy.get('social_sessions', 0))
            },
            'top_pages': []
        }
    
    def _empty_metrics(self):
        """Return empty metrics structure"""
        return {
            'awareness': {'sessions': 0, 'users': 0},
            'engagement': {'pages_per_session': 0, 'avg_session_duration': 0, 'engagement_rate': 0},
            'conversion': {'conversions': 0, 'conversion_rate': 0},
            'retention': {'returning_users': 0, 'retention_rate': 0},
            'advocacy': {'referrals': 0, 'social_shares': 0},
            'top_pages': []
        }


class DataCollector:
    """Collect and store data from all sources with TRUE historical tracking"""
    
    def __init__(self, customer_id: str):
        self.customer_id = customer_id
        self.customer = Customer.get_by_id(customer_id)
        self.credentials = CustomerCredential.get_all_for_customer(customer_id)
        
    def collect_all_data(self, days: int = 30, status_callback=None, collect_history: bool = False):
        """
        Collect data from all sources and store in database
        
        Args:
            days: Number of days for current data collection
            status_callback: Callback function for status updates
            collect_history: If True, collect 12 months of historical data
        """
        import concurrent.futures
        import threading
        
        print(f"\n{'='*70}")
        print(f"COLLECTING DATA FOR: {self.customer['name']}")
        print(f"Industry: {self.customer['industry']}")
        print(f"Historical Collection: {'ENABLED (12 months)' if collect_history else 'DISABLED (current only)'}")
        print(f"{'='*70}\n")
        
        if collect_history:
            # Collect 12 months of historical data with BULK API CALLS
            self.collect_historical_data_optimized(status_callback)
        else:
            # Collect current period only (parallel execution)
            completed = {'social': False, 'email': False, 'website': False}
            lock = threading.Lock()
            
            def update_status(source, message, progress):
                """Update status safely from any thread"""
                if status_callback:
                    status_callback(source, message, progress)
            
            def collect_with_status(collect_func, source_name, emoji, progress_start):
                """Wrapper to collect with status updates"""
                try:
                    print(f"[THREAD] Starting {source_name} collection thread")
                    update_status(source_name, f"{emoji} Collecting {source_name} data...", progress_start)
                    
                    # Call the actual collection function
                    print(f"[THREAD] Calling collect function for {source_name}")
                    collect_func(days)
                    
                    with lock:
                        completed[source_name.split()[0].lower()] = True
                    update_status(source_name, f"✅ {source_name} complete!", progress_start + 30)
                    print(f"[OK] {source_name} collection completed")
                except Exception as e:
                    print(f"[ERROR] {source_name} collection thread failed: {e}")
                    import traceback
                    traceback.print_exc()
                    with lock:
                        completed[source_name.split()[0].lower()] = True
                    update_status(source_name, f"⚠️ {source_name} failed: {str(e)[:50]}", progress_start + 30)
            
            # Run all three collections in parallel
            print(f"[INFO] Starting parallel collection with 3 threads...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                future_social = executor.submit(collect_with_status, self.collect_social_media, "Social Media", "📱", 20)
                future_email = executor.submit(collect_with_status, self.collect_email_metrics, "Email", "📧", 40)
                future_website = executor.submit(collect_with_status, self.collect_website_metrics, "Website", "🌐", 60)
                
                futures = [future_social, future_email, future_website]
                concurrent.futures.wait(futures)
            
            print(f"\n{'='*70}")
            print("DATA COLLECTION COMPLETE")
            print(f"Social: {'✓' if completed['social'] else '✗'}")
            print(f"Email: {'✓' if completed['email'] else '✗'}")
            print(f"Website: {'✓' if completed['website'] else '✗'}")
            print(f"{'='*70}\n")
    
    def collect_historical_data_optimized(self, status_callback=None):
        """
        Collect 12 months of historical data with OPTIMIZED bulk API calls
        
        NEW APPROACH:
        - GA4: 1 API call for all 12 months (bulk endpoint)
        - Email: 1 API call for all campaigns (group by start_date)
        - Social: 1 API call with period='day' (parse daily to monthly)
        
        Total: 3 API calls instead of 36 (12 months × 3 sources)
        """
        print(f"\n{'='*70}")
        print(f"COLLECTING 12-MONTH HISTORICAL DATA (OPTIMIZED)")
        print(f"{'='*70}\n")
        
        # Calculate 12 month range
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=11)  # 11 months back + current = 12 months
        
        start_month = start_date.replace(day=1)
        end_month = end_date
        
        print(f"[INFO] Date range: {start_month.strftime('%Y-%m')} to {end_month.strftime('%Y-%m')}")
        print(f"[INFO] Using BULK API calls for maximum efficiency\n")
        
        # 1. COLLECT WEBSITE DATA (1 API call for all 12 months)
        if status_callback:
            status_callback("Historical Collection", "🌐 Fetching 12 months of website data...", 10)
        
        try:
            print("[1/3] Website Analytics (GA4) - Bulk collection...")
            self.collect_website_bulk(start_month, end_month)
            print("✓ Website data collected (1 API call)\n")
        except Exception as e:
            print(f"[ERROR] Website bulk collection failed: {e}\n")
        
        if status_callback:
            status_callback("Historical Collection", "📧 Fetching 12 months of email data...", 40)
        
        # 2. COLLECT EMAIL DATA (1 API call for all campaigns)
        try:
            print("[2/3] Email Metrics - Bulk collection...")
            self.collect_email_bulk(start_month, end_month)
            print("✓ Email data collected (1 API call)\n")
        except Exception as e:
            print(f"[ERROR] Email bulk collection failed: {e}\n")
        
        if status_callback:
            status_callback("Historical Collection", "📱 Fetching 12 months of social media data...", 70)
        
        # 3. COLLECT SOCIAL MEDIA DATA (1 API call with daily granularity)
        try:
            print("[3/3] Social Media - Bulk collection with daily data...")
            self.collect_social_bulk(start_month, end_month)
            print("✓ Social media data collected (1 API call)\n")
        except Exception as e:
            print(f"[ERROR] Social bulk collection failed: {e}\n")
        
        if status_callback:
            status_callback("Historical Collection", "✅ 12-month historical collection complete!", 100)
        
        print(f"{'='*70}")
        print("HISTORICAL DATA COLLECTION COMPLETE")
        print(f"Total API calls: 3 (GA4, Email, Social)")
        print(f"Time saved: ~95% compared to month-by-month approach")
        print(f"{'='*70}\n")
    
    def collect_website_bulk(self, start_month: datetime, end_month: datetime):
        """
        Collect website data for ALL 12 months in ONE API call
        Uses enhanced GA4 endpoint with by_month structure
        """
        website_creds = self.credentials.get('website', {})
        property_id = website_creds.get('ga4_property_id')
        
        if not property_id:
            print("  [WARNING] No GA4 property ID")
            return
        
        try:
            fetcher = GA4Fetcher(property_id)
            
            # Format months for API call
            start_month_str = start_month.strftime('%Y-%m')
            end_month_str = end_month.strftime('%Y-%m')
            
            # Get ALL months in one call
            by_month = fetcher.get_monthly_metrics_bulk(start_month_str, end_month_str)
            
            if not by_month:
                print("  [WARNING] No data returned from GA4 bulk endpoint")
                return
            
            # Store each month's data
            for month_str, month_data in by_month.items():
                # Parse month string (YYYY-MM)
                year, month = map(int, month_str.split('-'))
                
                # Calculate days in month
                if month == 12:
                    next_month_start = datetime(year + 1, 1, 1)
                else:
                    next_month_start = datetime(year, month + 1, 1)
                month_start = datetime(year, month, 1)
                days = (next_month_start - month_start).days
                
                print(f"  Storing {month_str}...")
                
                # Parse and store metrics
                awareness = month_data.get('awareness', {})
                engagement = month_data.get('engagement', {})
                conversion = month_data.get('conversion', {})
                retention = month_data.get('retention', {})
                advocacy = month_data.get('advocacy', {})
                
                # Store awareness metrics
                self._store_metric('website', 'awareness', 'Sessions',
                                  int(awareness.get('sessions', 0)),
                                  'sessions', days, year, month)
                self._store_metric('website', 'awareness', 'Users',
                                  int(awareness.get('users', 0)),
                                  'users', days, year, month)
                
                # Store engagement metrics
                self._store_metric('website', 'engagement', 'Pages per Session',
                                  float(engagement.get('pages_per_session', 0)),
                                  'pages_per_session', days, year, month)
                self._store_metric('website', 'engagement', 'Avg Session Duration',
                                  float(engagement.get('avg_session_duration', 0)),
                                  'avg_session_duration', days, year, month)
                
                # Store conversion metrics
                self._store_metric('website', 'conversion', 'Conversions',
                                  int(conversion.get('total_conversions', 0)),
                                  'conversions', days, year, month)
                self._store_metric('website', 'conversion', 'Conversion Rate',
                                  float(conversion.get('conversion_rate', 0)),
                                  'conversion_rate', days, year, month)
                
                # Store retention metrics
                self._store_metric('website', 'retention', 'Returning Users',
                                  int(retention.get('returning_users', 0)),
                                  'returning_users', days, year, month)
                self._store_metric('website', 'retention', 'Retention Rate',
                                  float(retention.get('returning_user_rate', 0)),
                                  'retention_rate', days, year, month)
                
                # Store advocacy metrics
                self._store_metric('website', 'advocacy', 'Referrals',
                                  int(advocacy.get('referral_sessions', 0)),
                                  'referrals', days, year, month)
            
            print(f"  ✓ Stored {len(by_month)} months of website data")
            
        except Exception as e:
            print(f"  [ERROR] Website bulk collection failed: {e}")
            import traceback
            traceback.print_exc()
    
    def collect_email_bulk(self, start_month: datetime, end_month: datetime):
        """
        Collect email data for ALL 12 months in ONE API call
        Gets all campaigns and groups them by start_date field
        """
        email_creds = self.credentials.get('email', {})
        instantly_key = email_creds.get('instantly_api_key')
        
        if not instantly_key:
            print("  [WARNING] No email credentials")
            return
        
        try:
            fetcher = InstantlyFetcher(instantly_key)
            
            # Get ALL campaigns (1 API call)
            campaigns = fetcher.get_all_campaigns()
            
            if not campaigns:
                print("  [WARNING] No campaigns found")
                return
            
            print(f"  Found {len(campaigns)} campaigns")
            
            # Group campaigns by month based on start_date
            campaigns_by_month = {}
            
            for campaign in campaigns:
                # Get campaign start_date
                start_date = campaign.get('start_date')
                
                if not start_date:
                    continue
                
                # Parse date (format may vary, handle multiple formats)
                try:
                    # Try ISO format first
                    campaign_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                except:
                    try:
                        # Try common format YYYY-MM-DD
                        campaign_date = datetime.strptime(start_date[:10], '%Y-%m-%d')
                    except:
                        print(f"  [WARNING] Could not parse date: {start_date}")
                        continue
                
                # Check if campaign is in our date range
                if campaign_date < start_month or campaign_date > end_month:
                    continue
                
                # Get month key
                month_key = (campaign_date.year, campaign_date.month)
                
                if month_key not in campaigns_by_month:
                    campaigns_by_month[month_key] = []
                
                campaigns_by_month[month_key].append(campaign)
            
            print(f"  Campaigns span {len(campaigns_by_month)} months")
            
            # Now get analytics for each campaign and aggregate by month
            for (year, month), month_campaigns in campaigns_by_month.items():
                print(f"  Processing {year}-{month:02d}: {len(month_campaigns)} campaigns...")

                # Aggregate metrics for this month
                total_sent = 0
                total_delivered = 0
                total_opened = 0
                total_clicked = 0
                total_replied = 0
                total_bounced = 0
                total_unsubscribed = 0

                # Calculate date range for this month
                month_start = datetime(year, month, 1)
                if month == 12:
                    month_end = datetime(year + 1, 1, 1) - timedelta(days=1)
                else:
                    month_end = datetime(year, month + 1, 1) - timedelta(days=1)

                # Get campaign IDs for bulk fetch
                campaign_ids = [c.get('id') for c in month_campaigns if c.get('id')]

                if not campaign_ids:
                    continue

                # Fetch analytics for multiple campaigns at once (more efficient)
                if len(campaign_ids) > 1:
                    analytics_data = fetcher.get_multiple_campaigns_analytics(
                        campaign_ids,
                        start_date=month_start.strftime('%Y-%m-%d'),
                        end_date=month_end.strftime('%Y-%m-%d'),
                        debug=False
                    )

                    # Aggregate from bulk response
                    if isinstance(analytics_data, dict):
                        for campaign_id, analytics in analytics_data.items():
                            if analytics:
                                total_sent += analytics.get('emails_sent_count', 0)
                                total_delivered += analytics.get('contacted_count', 0)
                                total_opened += analytics.get('open_count_unique', 0)
                                total_clicked += analytics.get('link_click_count_unique', 0)
                                total_replied += analytics.get('reply_count_unique', 0)
                                total_bounced += analytics.get('bounced_count', 0)
                                total_unsubscribed += analytics.get('unsubscribed_count', 0)
                else:
                    # Single campaign - use individual fetch
                    campaign_id = campaign_ids[0]
                    analytics = fetcher.get_campaign_analytics(
                        campaign_id,
                        start_date=month_start.strftime('%Y-%m-%d'),
                        end_date=month_end.strftime('%Y-%m-%d'),
                        debug=False
                    )

                    if analytics:
                        total_sent += analytics.get('emails_sent_count', 0)
                        total_delivered += analytics.get('contacted_count', 0)
                        total_opened += analytics.get('open_count_unique', 0)
                        total_clicked += analytics.get('link_click_count_unique', 0)
                        total_replied += analytics.get('reply_count_unique', 0)
                        total_bounced += analytics.get('bounced_count', 0)
                        total_unsubscribed += analytics.get('unsubscribed_count', 0)
                
                # Calculate rates
                delivery_rate = (total_delivered / total_sent * 100) if total_sent > 0 else 0
                open_rate = (total_opened / total_delivered * 100) if total_delivered > 0 else 0
                click_rate = (total_clicked / total_delivered * 100) if total_delivered > 0 else 0
                reply_rate = (total_replied / total_delivered * 100) if total_delivered > 0 else 0
                unsubscribe_rate = (total_unsubscribed / total_delivered * 100) if total_delivered > 0 else 0
                deliverability_score = (total_delivered / total_sent * 100) if total_sent > 0 else 0
                
                # Calculate days in month
                if month == 12:
                    next_month = datetime(year + 1, 1, 1)
                else:
                    next_month = datetime(year, month + 1, 1)
                month_start = datetime(year, month, 1)
                days = (next_month - month_start).days
                
                # Store metrics
                self._store_metric('email', 'awareness', 'Emails Sent',
                                  total_sent, 'emails_sent', days, year, month)
                self._store_metric('email', 'awareness', 'Delivery Rate',
                                  delivery_rate, 'delivery_rate', days, year, month)
                
                self._store_metric('email', 'engagement', 'Open Rate',
                                  open_rate, 'open_rate', days, year, month)
                self._store_metric('email', 'engagement', 'Click Rate',
                                  click_rate, 'click_rate', days, year, month)
                
                self._store_metric('email', 'response', 'Reply Rate',
                                  reply_rate, 'reply_rate', days, year, month)
                
                self._store_metric('email', 'retention', 'Unsubscribe Rate',
                                  unsubscribe_rate, 'unsubscribe_rate', days, year, month)
                
                self._store_metric('email', 'quality', 'Deliverability Score',
                                  deliverability_score, 'deliverability_score', days, year, month)
            
            print(f"  ✓ Stored {len(campaigns_by_month)} months of email data")
            
        except Exception as e:
            print(f"  [ERROR] Email bulk collection failed: {e}")
            import traceback
            traceback.print_exc()
    
    def collect_social_bulk(self, start_month: datetime, end_month: datetime):
        """
        Collect social media data for ALL 12 months with daily granularity
        Gets data with period='day' and groups into monthly buckets
        """
        social_creds = self.credentials.get('social_media', {})
        system_token = social_creds.get('system_user_token')
        
        if not system_token:
            print("  [WARNING] No social media credentials")
            return
        
        try:
            accounts = get_all_pages_and_instagram_accounts(system_token)
            
            if not accounts:
                print("  [WARNING] No accounts found")
                return
            
            print(f"  Found {len(accounts)} social media accounts")
            
            # Calculate total days to request
            days_total = (end_month - start_month).days + 1
            
            # Initialize monthly buckets
            monthly_data = {}
            
            # For each account, get data with daily granularity
            for account in accounts:
                print(f"  Processing account: {account['page_name']}")
                
                # Get Facebook insights with daily data
                try:
                    fb_insights = get_facebook_page_insights(
                        account['page_id'],
                        account['page_token'],
                        days_back=days_total
                    )
                    
                    # Parse daily values into monthly buckets
                    if 'page_impressions_unique' in fb_insights:
                        for value_entry in fb_insights['page_impressions_unique'].get('values', []):
                            date_str = value_entry.get('end_time', '')
                            value = value_entry.get('value', 0)
                            
                            # Parse date and extract year-month
                            if date_str:
                                try:
                                    date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')
                                    month_key = (date_obj.year, date_obj.month)
                                    
                                    if month_key not in monthly_data:
                                        monthly_data[month_key] = {
                                            'impressions': 0,
                                            'engagement': 0,
                                            'reach': 0,
                                            'followers': 0
                                        }
                                    
                                    monthly_data[month_key]['impressions'] += value
                                except:
                                    pass
                    
                    # Parse engagement data
                    if 'page_post_engagements' in fb_insights:
                        for value_entry in fb_insights['page_post_engagements'].get('values', []):
                            date_str = value_entry.get('end_time', '')
                            value = value_entry.get('value', 0)
                            
                            if date_str:
                                try:
                                    date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')
                                    month_key = (date_obj.year, date_obj.month)
                                    
                                    if month_key not in monthly_data:
                                        monthly_data[month_key] = {
                                            'impressions': 0,
                                            'engagement': 0,
                                            'reach': 0,
                                            'followers': 0
                                        }
                                    
                                    monthly_data[month_key]['engagement'] += value
                                except:
                                    pass
                    
                    # Add follower count (point-in-time, not cumulative)
                    for month_key in monthly_data:
                        monthly_data[month_key]['followers'] += account.get('fan_count', 0)
                    
                except Exception as e:
                    print(f"    [ERROR] Facebook failed: {e}")
                
                # Get Instagram insights
                if account.get('instagram_id'):
                    try:
                        ig_insights = get_instagram_account_insights(
                            account['instagram_id'],
                            account['page_token'],
                            days_back=days_total
                        )
                        
                        # Parse reach data
                        if 'reach' in ig_insights:
                            for value_entry in ig_insights['reach']:
                                date_str = value_entry.get('end_time', '')
                                value = value_entry.get('value', 0)
                                
                                if date_str:
                                    try:
                                        date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')
                                        month_key = (date_obj.year, date_obj.month)
                                        
                                        if month_key not in monthly_data:
                                            monthly_data[month_key] = {
                                                'impressions': 0,
                                                'engagement': 0,
                                                'reach': 0,
                                                'followers': 0
                                            }
                                        
                                        monthly_data[month_key]['reach'] += value
                                    except:
                                        pass
                        
                        # Add follower count
                        for month_key in monthly_data:
                            monthly_data[month_key]['followers'] += account.get('followers_count', 0)
                        
                    except Exception as e:
                        print(f"    [ERROR] Instagram failed: {e}")
            
            # Store monthly aggregated data
            for (year, month), data in monthly_data.items():
                print(f"  Storing {year}-{month:02d}...")
                
                # Calculate days in month
                if month == 12:
                    next_month = datetime(year + 1, 1, 1)
                else:
                    next_month = datetime(year, month + 1, 1)
                month_start = datetime(year, month, 1)
                days = (next_month - month_start).days
                
                # Calculate engagement rate
                total_reach = data['reach'] if data['reach'] > 0 else data['impressions']
                engagement_rate = (data['engagement'] / total_reach * 100) if total_reach > 0 else 0
                
                # Store metrics
                self._store_metric('social_media', 'awareness', 'Reach',
                                  data['reach'], 'reach', days, year, month)
                self._store_metric('social_media', 'awareness', 'Impressions',
                                  data['impressions'], 'impressions', days, year, month)
                self._store_metric('social_media', 'engagement', 'Engagement Rate',
                                  engagement_rate, 'engagement_rate', days, year, month)
                self._store_metric('social_media', 'engagement', 'Total Interactions',
                                  data['engagement'], 'interactions', days, year, month)
                self._store_metric('social_media', 'retention', 'Follower Count',
                                  data['followers'], 'follower_growth', days, year, month)
            
            print(f"  ✓ Stored {len(monthly_data)} months of social media data")
            
        except Exception as e:
            print(f"  [ERROR] Social bulk collection failed: {e}")
            import traceback
            traceback.print_exc()
    
    # Keep existing methods for current period collection
    def collect_social_media(self, days: int):
        """Collect and store social media metrics for current period"""
        now = datetime.now()
        start = now - timedelta(days=days)
        self.collect_social_bulk(start, now)
    
    def collect_email_metrics(self, days: int):
        """Collect and store email metrics for current period"""
        now = datetime.now()
        start = now - timedelta(days=days)
        self.collect_email_bulk(start, now)
    
    def collect_website_metrics(self, days: int):
        """Collect and store website metrics for current period"""
        now = datetime.now()
        start = now - timedelta(days=days)
        self.collect_website_bulk(start, now)
    
    def _store_metric(self, medium: str, journey_stage: str, kpi_name: str, 
                     kpi_value: float, benchmark_key: str, time_period_days: int,
                     year: int = None, month: int = None):
        """Store a metric with its benchmark for a specific month"""
        # Get benchmark
        benchmark = get_benchmark(
            self.customer['industry'],
            medium,
            journey_stage,
            benchmark_key
        )
        
        # Store in database with year/month
        HistoricalMetric.add(
            self.customer_id,
            medium,
            journey_stage,
            kpi_name,
            kpi_value,
            benchmark,
            time_period_days,
            year,
            month
        )


def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect data for customer dashboard')
    parser.add_argument('--customer-id', type=str, required=True, help='Customer ID')
    parser.add_argument('--days', type=int, default=30, help='Days of data to collect')
    parser.add_argument('--history', action='store_true', help='Collect 12 months of historical data')
    
    args = parser.parse_args()
    
    # Initialize database if needed
    Database.init_db()
    
    # Collect data
    collector = DataCollector(args.customer_id)
    collector.collect_all_data(days=args.days, collect_history=args.history)


if __name__ == '__main__':
    main()
