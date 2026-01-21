"""
Data Collector for ContentClicks Dashboard
Integrates Social Media, Email, and GA4 Analytics with 12-month historical tracking

VERSION 2.0 - SYNTAX FIXED - BUILD 20260118
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
    """Fetch website analytics from existing GA4 endpoint"""
    
    def __init__(self, property_id: str, endpoint_url: str = None):
        """
        Initialize with GA4 property ID and optional custom endpoint
        
        Args:
            property_id: GA4 property ID
            endpoint_url: Your GA4 analytics endpoint URL
        """
        self.property_id = property_id
        # Default to the deployed GA4 analytics service
        self.endpoint_url = endpoint_url or "https://ga4-analytics-713170455475.us-central1.run.app"
        print(f"[INFO] Using GA4 endpoint: {self.endpoint_url}")
        
    def get_metrics(self, start_date: datetime, end_date: datetime) -> Dict:
        """Get GA4 metrics for a specific date range with retry logic"""
        import requests
        import time
        
        max_attempts = 5  # Increased from 3 for rate-limited endpoints
        attempt = 0
        
        while attempt < max_attempts:
            try:
                attempt += 1
                
                if attempt > 1:
                    # Longer delays for rate limiting (15s, 30s)
                    wait_time = 15 * attempt  # 15s, 30s, 45s
                    print(f"[INFO] Retry {attempt}/{max_attempts} after {wait_time}s delay (rate limiting)...")
                    time.sleep(wait_time)
                
                print(f"[INFO] Fetching GA4 metrics for property {self.property_id}")
                print(f"[INFO] Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                
                # CRITICAL FIX: Force fresh SSL context per request
                # This prevents SSL connection pool corruption
                from requests.adapters import HTTPAdapter
                
                session = requests.Session()
                
                # Disable connection pooling completely
                adapter = HTTPAdapter(
                    pool_connections=1,
                    pool_maxsize=1,
                    max_retries=0
                )
                session.mount('https://', adapter)
                session.mount('http://', adapter)
                
                # Headers to force connection close
                headers = {
                    'Connection': 'close',
                    'User-Agent': 'ContentClicksDashboard/2.0'
                }
                
                # Call your existing GA4 endpoint with fresh SSL context
                response = session.get(
                    f"{self.endpoint_url}/ga4/{self.property_id}",
                    params={
                        "start_date": start_date.strftime('%Y-%m-%d'),
                        "end_date": end_date.strftime('%Y-%m-%d')
                    },
                    headers=headers,
                    timeout=90,
                    verify=True
                )
                
                # Extract data before closing
                response.raise_for_status()
                raw_data = response.json()
                
                # Aggressively cleanup connections
                response.close()
                session.close()
                del session
                
                print(f"[OK] Received GA4 data from endpoint")
                
                # Your endpoint returns data in format: {"data": {...}, "status": "success"}
                if raw_data.get('status') != 'success':
                    print(f"[WARNING] GA4 endpoint returned non-success status")
                    return self._empty_metrics()
                
                data = raw_data.get('data', {})
                
                # Extract data sections
                awareness = data.get('awareness', {})
                engagement = data.get('engagement', {})
                conversion = data.get('conversion', {})
                retention = data.get('retention', {})
                advocacy = data.get('advocacy', {})
                
                # Calculate total sessions and new users from traffic_by_channel
                total_sessions = 0
                total_new_users = 0
                for channel in awareness.get('traffic_by_channel', []):
                    total_sessions += int(channel.get('sessions', 0))
                    total_new_users += int(channel.get('newUsers', 0))
                
                # Get new users directly from awareness if available
                new_users_from_awareness = int(awareness.get('new_users', 0))
                if new_users_from_awareness > 0:
                    new_users = new_users_from_awareness
                else:
                    new_users = total_new_users
                
                # Calculate total users: new users + returning users
                returning_users_count = int(retention.get('returning_users', 0))
                total_users = new_users + returning_users_count
                
                # If total is still 0, try to estimate from user_type_breakdown sessions
                if total_users == 0:
                    for user_type in retention.get('user_type_breakdown', []):
                        if user_type.get('newVsReturning') == 'new':
                            total_users += int(user_type.get('sessions', 0) * 0.7)
                        elif user_type.get('newVsReturning') == 'returning':
                            total_users += int(user_type.get('sessions', 0) * 0.3)
                    total_users = int(total_users)
                
                # Calculate total page views from top_content_pages
                total_page_views = 0
                for page in engagement.get('top_content_pages', []):
                    total_page_views += int(page.get('screenPageViews', 0))
                
                # Calculate engaged sessions from user_type_breakdown
                engaged_sessions = 0
                for user_type in retention.get('user_type_breakdown', []):
                    engaged_sessions += int(user_type.get('engagedSessions', 0))
                
                # Calculate metrics
                pages_per_session = round(total_page_views / total_sessions, 2) if total_sessions > 0 else 0
                
                # Get average session duration from top landing pages
                total_duration = 0
                landing_sessions = 0
                for landing in engagement.get('top_landing_pages', []):
                    duration = landing.get('averageSessionDuration', 0)
                    sessions = landing.get('sessions', 0)
                    total_duration += duration * sessions
                    landing_sessions += sessions
                
                avg_duration = round(total_duration / landing_sessions, 0) if landing_sessions > 0 else 0
                
                # Engagement rate
                engagement_rate = round((engaged_sessions / total_sessions * 100), 2) if total_sessions > 0 else 0
                
                # Extract top pages
                top_pages = []
                for page in engagement.get('top_content_pages', [])[:3]:
                    top_pages.append({
                        'page': page.get('pagePath', 'Unknown'),
                        'title': page.get('pageTitle', 'Untitled'),
                        'views': int(page.get('screenPageViews', 0)),
                        'users': 0,
                        'avg_duration': round(page.get('averageSessionDuration', 0), 0)
                    })
                
                # Get conversions
                total_conversions = 0
                for event in conversion.get('key_conversion_events', []):
                    total_conversions += int(event.get('eventCount', 0))
                
                conversion_rate = round((total_conversions / total_sessions * 100), 2) if total_sessions > 0 else 0
                
                # Retention metrics
                returning_users = int(retention.get('returning_users', 0))
                retention_rate = round(retention.get('returning_user_rate', 0), 2)
                
                # Advocacy metrics  
                referral_sessions = int(advocacy.get('referral_sessions', 0))
                social_sessions = int(advocacy.get('social_sessions', 0))
                
                # Build response matching dashboard expectations
                result = {
                    'awareness': {
                        'sessions': total_sessions,
                        'users': total_users
                    },
                    'engagement': {
                        'pages_per_session': pages_per_session,
                        'avg_session_duration': avg_duration,
                        'engagement_rate': engagement_rate
                    },
                    'conversion': {
                        'conversions': total_conversions,
                        'conversion_rate': conversion_rate
                    },
                    'retention': {
                        'returning_users': returning_users,
                        'retention_rate': retention_rate
                    },
                    'advocacy': {
                        'referrals': referral_sessions,
                        'social_shares': social_sessions
                    },
                    'top_pages': top_pages
                }
                
                print(f"[OK] Processed GA4 metrics: {result['awareness']['sessions']} sessions")
                return result
                
            except requests.exceptions.SSLError as ssl_err:
                print(f"[WARNING] SSL error on attempt {attempt}/{max_attempts}: {ssl_err}")
                if attempt < max_attempts:
                    continue  # Retry
                else:
                    print(f"[ERROR] Max retries exceeded due to SSL errors")
                    return self._empty_metrics()
            
            except requests.exceptions.Timeout as timeout_err:
                print(f"[WARNING] Timeout on attempt {attempt}/{max_attempts}: {timeout_err}")
                if attempt < max_attempts:
                    continue  # Retry
                else:
                    print(f"[ERROR] Max retries exceeded due to timeouts")
                    return self._empty_metrics()
            
            except Exception as e:
                print(f"[ERROR] Failed to fetch from GA4 endpoint: {e}")
                import traceback
                traceback.print_exc()
                return self._empty_metrics()
        
        # If we get here, all retries failed
        print(f"[ERROR] All {max_attempts} attempts failed")
        return self._empty_metrics()
    
    def _empty_metrics(self):
        """Return empty metrics structure"""
        return {
            'awareness': {'sessions': 0, 'users': 0},
            'engagement': {'pages_per_session': 0, 'avg_session_duration': 0},
            'conversion': {'conversions': 0, 'conversion_rate': 0},
            'retention': {'returning_users': 0, 'retention_rate': 0},
            'advocacy': {'referrals': 0, 'social_shares': 0},
            'top_pages': []
        }


class DataCollector:
    """Collect and store data from all sources with historical tracking"""
    
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
            # Collect 12 months of historical data
            self.collect_historical_data(status_callback)
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
    
    def collect_historical_data(self, status_callback=None):
        """Collect 12 months of historical data for all mediums"""
        print(f"\n{'='*70}")
        print(f"COLLECTING 12-MONTH HISTORICAL DATA")
        print(f"{'='*70}\n")
        
        # Calculate 12 month periods
        end_date = datetime.now()
        months = []
        
        for i in range(12):
            month_end = end_date - relativedelta(months=i)
            month_start = month_end.replace(day=1)
            
            # Calculate last day of month
            if month_end.month == 12:
                next_month = month_end.replace(year=month_end.year + 1, month=1, day=1)
            else:
                next_month = month_end.replace(month=month_end.month + 1, day=1)
            month_end = next_month - timedelta(days=1)
            
            months.append({
                'start': month_start,
                'end': month_end,
                'year': month_start.year,
                'month': month_start.month,
                'days': (month_end - month_start).days + 1
            })
        
        # Reverse to go chronologically (oldest to newest)
        months.reverse()
        
        print(f"[INFO] Collecting data for {len(months)} months:")
        for m in months:
            print(f"  - {m['start'].strftime('%Y-%m')} ({m['days']} days)")
        print()
        
        # Collect data for each month
        for idx, month_info in enumerate(months):
            progress = int((idx / len(months)) * 100)
            month_label = month_info['start'].strftime('%Y-%m')
            
            if status_callback:
                status_callback("Historical Collection", 
                              f"📅 Collecting {month_label} ({idx+1}/{len(months)})", 
                              progress)
            
            print(f"\n[{idx+1}/{len(months)}] Collecting data for {month_label}...")
            
            # Collect each medium for this month
            try:
                self.collect_social_media_monthly(month_info)
            except Exception as e:
                print(f"[ERROR] Social media failed for {month_label}: {e}")
            
            try:
                self.collect_email_metrics_monthly(month_info)
            except Exception as e:
                print(f"[ERROR] Email failed for {month_label}: {e}")
            
            try:
                self.collect_website_metrics_monthly(month_info)
            except Exception as e:
                print(f"[ERROR] Website failed for {month_label}: {e}")
            
            # Add delay between months to prevent overwhelming APIs
            # Especially important for GA4 endpoint rate limiting
            # Increased to 30 seconds based on SSL error patterns
            if idx < len(months) - 1:  # Don't delay after last month
                import time
                delay = 30  # 30 seconds between months (increased from 10)
                print(f"[INFO] Pausing {delay}s before next month to avoid rate limiting...")
                time.sleep(delay)
        
        if status_callback:
            status_callback("Historical Collection", 
                          "✅ 12-month historical collection complete!", 
                          100)
        
        print(f"\n{'='*70}")
        print("HISTORICAL DATA COLLECTION COMPLETE")
        print(f"{'='*70}\n")
    
    def collect_social_media_monthly(self, month_info: dict):
        """Collect social media data for a specific month"""
        days = month_info['days']
        year = month_info['year']
        month = month_info['month']
        
        print(f"  [Social Media] {year}-{month:02d}")
        
        # Get credentials
        social_creds = self.credentials.get('social_media', {})
        system_token = social_creds.get('system_user_token')
        
        if not system_token:
            print(f"  [WARNING] No social media credentials")
            return
        
        try:
            accounts = get_all_pages_and_instagram_accounts(system_token)
            
            if not accounts:
                print(f"  [WARNING] No accounts found")
                return
            
            # Aggregate metrics
            total_reach = 0
            total_impressions = 0
            total_engagement = 0
            total_followers = 0
            
            for account in accounts:
                # Facebook metrics
                try:
                    fb_insights = get_facebook_page_insights(
                        account['page_id'],
                        account['page_token'],
                        days_back=days
                    )
                    
                    if 'page_impressions_unique' in fb_insights:
                        values = fb_insights['page_impressions_unique'].get('values', [])
                        total_impressions += sum(v.get('value', 0) for v in values)
                    
                    if 'page_post_engagements' in fb_insights:
                        values = fb_insights['page_post_engagements'].get('values', [])
                        total_engagement += sum(v.get('value', 0) for v in values)
                    
                    total_followers += account.get('fan_count', 0)
                except Exception as e:
                    print(f"    [ERROR] Facebook failed: {e}")
                
                # Instagram metrics
                if account.get('instagram_id'):
                    try:
                        ig_insights = get_instagram_account_insights(
                            account['instagram_id'],
                            account['page_token'],
                            days_back=days
                        )
                        
                        if 'reach' in ig_insights:
                            total_reach += sum(v.get('value', 0) for v in ig_insights['reach'])
                        
                        total_followers += account.get('followers_count', 0)
                    except Exception as e:
                        print(f"    [ERROR] Instagram failed: {e}")
            
            # Calculate metrics
            engagement_rate = (total_engagement / total_reach * 100) if total_reach > 0 else 0
            
            # Store metrics with year/month
            industry = self.customer['industry']
            
            self._store_metric('social_media', 'awareness', 'Reach', 
                              total_reach, 'reach', days, year, month)
            self._store_metric('social_media', 'awareness', 'Impressions', 
                              total_impressions, 'impressions', days, year, month)
            self._store_metric('social_media', 'engagement', 'Engagement Rate', 
                              engagement_rate, 'engagement_rate', days, year, month)
            self._store_metric('social_media', 'engagement', 'Total Interactions', 
                              total_engagement, 'interactions', days, year, month)
            self._store_metric('social_media', 'retention', 'Follower Count', 
                              total_followers, 'follower_growth', days, year, month)
            
            print(f"  ✓ Social media metrics stored")
            
        except Exception as e:
            print(f"  [ERROR] Social media collection failed: {e}")
            import traceback
            traceback.print_exc()
    
    def collect_email_metrics_monthly(self, month_info: dict):
        """Collect email metrics for a specific month"""
        days = month_info['days']
        year = month_info['year']
        month = month_info['month']
        
        print(f"  [Email] {year}-{month:02d}")
        
        email_creds = self.credentials.get('email', {})
        instantly_key = email_creds.get('instantly_api_key')
        
        if not instantly_key:
            print(f"  [WARNING] No email credentials")
            return
        
        try:
            fetcher = InstantlyFetcher(instantly_key)
            metrics = fetcher.calculate_customer_journey_metrics(days=days, debug=False)
            
            if metrics:
                if 'awareness' in metrics:
                    self._store_metric('email', 'awareness', 'Emails Sent',
                                      metrics['awareness'].get('emails_sent', 0),
                                      'emails_sent', days, year, month)
                    self._store_metric('email', 'awareness', 'Delivery Rate',
                                      metrics['awareness'].get('delivery_rate', 0),
                                      'delivery_rate', days, year, month)
                
                if 'engagement' in metrics:
                    self._store_metric('email', 'engagement', 'Open Rate',
                                      metrics['engagement'].get('open_rate', 0),
                                      'open_rate', days, year, month)
                    self._store_metric('email', 'engagement', 'Click Rate',
                                      metrics['engagement'].get('click_rate', 0),
                                      'click_rate', days, year, month)
                
                if 'response' in metrics:
                    self._store_metric('email', 'response', 'Reply Rate',
                                      metrics['response'].get('reply_rate', 0),
                                      'reply_rate', days, year, month)
                
                if 'retention' in metrics:
                    self._store_metric('email', 'retention', 'Unsubscribe Rate',
                                      metrics['retention'].get('unsubscribe_rate', 0),
                                      'unsubscribe_rate', days, year, month)
                
                if 'quality' in metrics:
                    self._store_metric('email', 'quality', 'Deliverability Score',
                                      metrics['quality'].get('deliverability_score', 0),
                                      'deliverability_score', days, year, month)
                
                print(f"  ✓ Email metrics stored")
        except Exception as e:
            print(f"  [ERROR] Email collection failed: {e}")
    
    def collect_website_metrics_monthly(self, month_info: dict):
        """Collect website metrics for a specific month"""
        year = month_info['year']
        month = month_info['month']
        start_date = month_info['start']
        end_date = month_info['end']
        days = month_info['days']
        
        print(f"  [Website] {year}-{month:02d}")
        
        website_creds = self.credentials.get('website', {})
        property_id = website_creds.get('ga4_property_id')
        
        if not property_id:
            print(f"  [WARNING] No GA4 property ID")
            return
        
        try:
            fetcher = GA4Fetcher(property_id)
            metrics = fetcher.get_metrics(start_date, end_date)
            
            if 'awareness' in metrics:
                self._store_metric('website', 'awareness', 'Sessions',
                                  metrics['awareness'].get('sessions', 0),
                                  'sessions', days, year, month)
                self._store_metric('website', 'awareness', 'Users',
                                  metrics['awareness'].get('users', 0),
                                  'users', days, year, month)
            
            if 'engagement' in metrics:
                self._store_metric('website', 'engagement', 'Pages per Session',
                                  metrics['engagement'].get('pages_per_session', 0),
                                  'pages_per_session', days, year, month)
                self._store_metric('website', 'engagement', 'Avg Session Duration',
                                  metrics['engagement'].get('avg_session_duration', 0),
                                  'avg_session_duration', days, year, month)
            
            if 'conversion' in metrics:
                self._store_metric('website', 'conversion', 'Conversions',
                                  metrics['conversion'].get('conversions', 0),
                                  'conversions', days, year, month)
                self._store_metric('website', 'conversion', 'Conversion Rate',
                                  metrics['conversion'].get('conversion_rate', 0),
                                  'conversion_rate', days, year, month)
            
            if 'retention' in metrics:
                self._store_metric('website', 'retention', 'Returning Users',
                                  metrics['retention'].get('returning_users', 0),
                                  'returning_users', days, year, month)
                self._store_metric('website', 'retention', 'Retention Rate',
                                  metrics['retention'].get('retention_rate', 0),
                                  'retention_rate', days, year, month)
            
            if 'advocacy' in metrics:
                self._store_metric('website', 'advocacy', 'Referrals',
                                  metrics['advocacy'].get('referrals', 0),
                                  'referrals', days, year, month)
            
            print(f"  ✓ Website metrics stored")
            
        except Exception as e:
            print(f"  [ERROR] Website collection failed: {e}")
            import traceback
            traceback.print_exc()
    
    # Keep existing methods for current period collection
    def collect_social_media(self, days: int):
        """Collect and store social media metrics for current period"""
        now = datetime.now()
        self.collect_social_media_monthly({
            'days': days,
            'year': now.year,
            'month': now.month,
            'start': now - timedelta(days=days),
            'end': now
        })
    
    def collect_email_metrics(self, days: int):
        """Collect and store email metrics for current period"""
        now = datetime.now()
        self.collect_email_metrics_monthly({
            'days': days,
            'year': now.year,
            'month': now.month,
            'start': now - timedelta(days=days),
            'end': now
        })
    
    def collect_website_metrics(self, days: int):
        """Collect and store website metrics for current period"""
        now = datetime.now()
        self.collect_website_metrics_monthly({
            'days': days,
            'year': now.year,
            'month': now.month,
            'start': now - timedelta(days=days),
            'end': now
        })
    
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
