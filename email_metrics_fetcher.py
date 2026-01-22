"""
Email Metrics Fetcher - Customer Journey Edition
Tracks email performance across the complete customer lifecycle
Supports both Instantly.ai and Klaviyo platforms

Customer Journey Stages Tracked:
1. Awareness - Emails sent, delivered, reach
2. Engagement - Opens, clicks, engagement rate
3. Response - Replies, conversions, response rate
4. Retention - Unsubscribes, list growth
5. Quality - Bounces, spam complaints, deliverability
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os


class InstantlyFetcher:
    """Fetch email metrics from Instantly.ai API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.instantly.ai/api/v2"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def get_all_campaigns(self) -> List[Dict]:
        """Get all campaigns with complete metrics"""
        print("[INFO] Fetching Instantly campaigns...")
        
        try:
            response = requests.get(
                f"{self.base_url}/campaigns",
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            
            # Debug: Show response structure
            print(f"[DEBUG] API response type: {type(data)}")
            if isinstance(data, dict):
                print(f"[DEBUG] Response keys: {list(data.keys())}")
            
            # Handle different response formats
            if isinstance(data, list):
                campaigns = data
            elif isinstance(data, dict) and 'items' in data:
                campaigns = data['items']  # Instantly.ai uses 'items' key
            elif isinstance(data, dict) and 'campaigns' in data:
                campaigns = data['campaigns']
            elif isinstance(data, dict) and 'data' in data:
                campaigns = data['data']
            else:
                print(f"[WARNING] Unexpected API response format: {type(data)}")
                print(f"[DEBUG] Available keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
                print(f"[DEBUG] First item sample: {str(data)[:200]}")
                campaigns = []
            
            print(f"[OK] Found {len(campaigns)} campaigns")
            
            # Debug: Show first campaign structure if available
            if campaigns and len(campaigns) > 0:
                print(f"[DEBUG] First campaign type: {type(campaigns[0])}")
                if isinstance(campaigns[0], dict):
                    print(f"[DEBUG] First campaign keys: {list(campaigns[0].keys())}")
            
            return campaigns
            
        except Exception as e:
            print(f"[ERROR] Failed to fetch campaigns: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_aggregate_analytics(self, start_date: str, end_date: str, debug: bool = False) -> Dict:
        """
        Get aggregate analytics for ALL campaigns in a date range
        This is much more efficient than fetching per-campaign data

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            debug: Enable debug logging

        Returns: Dictionary with aggregated metrics across all campaigns
        """
        endpoint = f"{self.base_url}/campaigns/analytics"
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'exclude_total_leads_count': 'true'
        }

        try:
            if debug:
                print(f"[DEBUG] Fetching aggregate analytics")
                print(f"[DEBUG] Endpoint: {endpoint}")
                print(f"[DEBUG] Params: {params}")

            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()

            if debug:
                print(f"[DEBUG] Success!")
                print(f"[DEBUG] Response type: {type(data)}")
                if isinstance(data, dict):
                    print(f"[DEBUG] Response keys: {list(data.keys())}")

            return data

        except Exception as e:
            print(f"[ERROR] Failed to fetch aggregate analytics: {e}")
            if debug:
                import traceback
                traceback.print_exc()
            return {}

    def get_campaign_analytics(self, campaign_id: str, start_date: str = None, end_date: str = None, debug: bool = False) -> Dict:
        """Get detailed analytics for a specific campaign"""
        from datetime import datetime, timedelta
        
        # Default to last 30 days if not specified
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        endpoint = f"{self.base_url}/campaigns/analytics/overview"
        params = {
            'id': campaign_id,
            'start_date': start_date,
            'end_date': end_date,
            'expand_crm_events': 'true'
        }
        
        try:
            if debug:
                print(f"[DEBUG] Endpoint: {endpoint}")
                print(f"[DEBUG] Params: {params}")
            
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            if debug:
                print(f"[DEBUG] Success!")
                print(f"[DEBUG] Response type: {type(data)}")
                if isinstance(data, dict):
                    print(f"[DEBUG] Response keys: {list(data.keys())}")
            
            return data
            
        except Exception as e:
            print(f"[ERROR] Failed to fetch campaign {campaign_id} analytics: {e}")
            if debug:
                import traceback
                traceback.print_exc()
            return {}
    
    def get_multiple_campaigns_analytics(self, campaign_ids: list, start_date: str = None, end_date: str = None, debug: bool = False) -> Dict:
        """Get analytics for multiple campaigns at once (more efficient)"""
        from datetime import datetime, timedelta
        
        # Default to last 30 days if not specified
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        endpoint = f"{self.base_url}/campaigns/analytics/overview"
        params = {
            'ids': ','.join(campaign_ids),  # Multiple IDs comma-separated
            'start_date': start_date,
            'end_date': end_date,
            'expand_crm_events': 'true'
        }
        
        try:
            if debug:
                print(f"[DEBUG] Fetching analytics for {len(campaign_ids)} campaigns in one call")
                print(f"[DEBUG] Endpoint: {endpoint}")
                print(f"[DEBUG] Params: {params}")
            
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            if debug:
                print(f"[DEBUG] Success!")
                print(f"[DEBUG] Response type: {type(data)}")
                if isinstance(data, dict):
                    print(f"[DEBUG] Response keys: {list(data.keys())}")
            
            return data
            
        except Exception as e:
            print(f"[ERROR] Failed to fetch multiple campaigns analytics: {e}")
            if debug:
                import traceback
                traceback.print_exc()
            return {}
    
    def get_account_analytics(self, debug: bool = False) -> Dict:
        """Get account-level email statistics"""
        # Try multiple possible endpoints
        possible_endpoints = [
            f"{self.base_url}/analytics/account",
            f"{self.base_url}/analytics",
            f"{self.base_url}/account/analytics",
            f"{self.base_url}/analytics/summary"
        ]
        
        for endpoint in possible_endpoints:
            try:
                if debug:
                    print(f"[DEBUG] Trying endpoint: {endpoint}")
                
                response = requests.get(endpoint, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                
                if debug:
                    print(f"[DEBUG] Success! Endpoint works: {endpoint}")
                    print(f"[DEBUG] Response type: {type(data)}")
                    if isinstance(data, dict):
                        print(f"[DEBUG] Response keys: {list(data.keys())}")
                
                return data
                
            except Exception as e:
                if debug:
                    print(f"[DEBUG] Failed: {endpoint} - {e}")
                continue
        
        print(f"[ERROR] All analytics endpoints failed")
        return {}
    
    def calculate_customer_journey_metrics(self, days: int = 30, debug: bool = False) -> Dict:
        """
        Calculate comprehensive customer journey metrics from Instantly data
        Returns metrics organized by journey stage
        """
        from datetime import datetime, timedelta
        
        print("\n" + "="*70)
        print("INSTANTLY.AI - CUSTOMER JOURNEY METRICS")
        print("="*70)
        
        # Calculate date range
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        print(f"[INFO] Date range: {start_date} to {end_date}")
        
        campaigns = self.get_all_campaigns()
        
        if not campaigns:
            print("[WARNING] No campaign data available")
            return {}
        
        print(f"[INFO] Fetching analytics for {len(campaigns)} campaigns...")
        
        # Initialize totals
        totals = {
            'total_sent': 0,
            'total_delivered': 0,
            'total_opened': 0,
            'total_clicked': 0,
            'total_replied': 0,
            'total_bounced': 0,
            'total_unsubscribed': 0,
            'total_leads': 0,
            'total_completed': 0,
            # Instantly-specific sales pipeline metrics
            'total_opportunities': 0,
            'total_interested': 0,
            'total_meetings_booked': 0,
            'total_meetings_completed': 0,
            'total_closed': 0
        }
        
        campaign_list = []
        
        # Fetch analytics for each campaign
        for idx, campaign in enumerate(campaigns):
            # Skip if campaign is not a dict
            if not isinstance(campaign, dict):
                print(f"[WARNING] Skipping campaign {idx}: not a dictionary")
                continue
            
            campaign_id = campaign.get('id')
            campaign_name = campaign.get('name', 'Unknown')
            
            if not campaign_id:
                print(f"[WARNING] Skipping campaign without ID: {campaign_name}")
                continue
            
            print(f"[{idx+1}/{len(campaigns)}] Fetching analytics for: {campaign_name}")
            
            # Get campaign analytics with date range
            analytics = self.get_campaign_analytics(
                campaign_id, 
                start_date=start_date, 
                end_date=end_date,
                debug=(debug and idx == 0)
            )
            
            if not analytics:
                if debug:
                    print(f"[WARNING] No analytics data for campaign: {campaign_name}")
                continue
            
            # Extract metrics from analytics response using Instantly's actual field names
            campaign_data = {
                'id': campaign_id,
                'name': campaign_name,
                'status': campaign.get('status', 'unknown'),
                'sent': analytics.get('emails_sent_count', 0),
                'delivered': analytics.get('contacted_count', analytics.get('emails_sent_count', 0)),
                'opened': analytics.get('open_count_unique', 0),  # Use unique opens
                'clicked': analytics.get('link_click_count_unique', 0),  # Use unique clicks
                'replied': analytics.get('reply_count_unique', 0),  # Use unique replies
                'bounced': analytics.get('bounced_count', 0),
                'unsubscribed': analytics.get('unsubscribed_count', 0),
                'leads': analytics.get('new_leads_contacted_count', 0),
                # Bonus Instantly-specific metrics
                'opportunities': analytics.get('total_opportunities', 0),
                'interested': analytics.get('total_interested', 0),
                'meetings_booked': analytics.get('total_meeting_booked', 0),
                'meetings_completed': analytics.get('total_meeting_completed', 0),
                'closed': analytics.get('total_closed', 0)
            }
            
            if debug and idx == 0:
                print(f"[DEBUG] Extracted campaign data: {campaign_data}")
            
            # Add to totals
            totals['total_sent'] += campaign_data['sent']
            totals['total_delivered'] += campaign_data['delivered']
            totals['total_opened'] += campaign_data['opened']
            totals['total_clicked'] += campaign_data['clicked']
            totals['total_replied'] += campaign_data['replied']
            totals['total_bounced'] += campaign_data['bounced']
            totals['total_unsubscribed'] += campaign_data['unsubscribed']
            totals['total_leads'] += campaign_data['leads']
            totals['total_opportunities'] += campaign_data.get('opportunities', 0)
            totals['total_interested'] += campaign_data.get('interested', 0)
            totals['total_meetings_booked'] += campaign_data.get('meetings_booked', 0)
            totals['total_meetings_completed'] += campaign_data.get('meetings_completed', 0)
            totals['total_closed'] += campaign_data.get('closed', 0)
            
            if campaign.get('status') == 'completed':
                totals['total_completed'] += 1
            
            campaign_list.append(campaign_data)
        
        # Calculate rates
        sent = totals['total_sent']
        delivered = totals['total_delivered']
        
        metrics = {
            # STAGE 1: AWARENESS (How many people did we reach?)
            'awareness': {
                'emails_sent': totals['total_sent'],
                'emails_delivered': totals['total_delivered'],
                'delivery_rate': round((delivered / sent * 100) if sent > 0 else 0, 2),
                'bounce_rate': round((totals['total_bounced'] / sent * 100) if sent > 0 else 0, 2),
                'net_reach': totals['total_delivered']  # Actual people reached
            },
            
            # STAGE 2: ENGAGEMENT (How many people engaged?)
            'engagement': {
                'total_opened': totals['total_opened'],
                'total_clicked': totals['total_clicked'],
                'open_rate': round((totals['total_opened'] / delivered * 100) if delivered > 0 else 0, 2),
                'click_rate': round((totals['total_clicked'] / delivered * 100) if delivered > 0 else 0, 2),
                'click_to_open_rate': round((totals['total_clicked'] / totals['total_opened'] * 100) if totals['total_opened'] > 0 else 0, 2)
            },
            
            # STAGE 3: RESPONSE (How many people responded?)
            'response': {
                'total_replied': totals['total_replied'],
                'total_leads': totals['total_leads'],
                'reply_rate': round((totals['total_replied'] / delivered * 100) if delivered > 0 else 0, 2),
                'lead_conversion_rate': round((totals['total_leads'] / delivered * 100) if delivered > 0 else 0, 2),
                'reply_to_open_rate': round((totals['total_replied'] / totals['total_opened'] * 100) if totals['total_opened'] > 0 else 0, 2)
            },
            
            # STAGE 4: RETENTION (Are people staying on our list?)
            'retention': {
                'total_unsubscribed': totals['total_unsubscribed'],
                'unsubscribe_rate': round((totals['total_unsubscribed'] / delivered * 100) if delivered > 0 else 0, 2),
                'active_list_size': delivered - totals['total_unsubscribed'] - totals['total_bounced']
            },
            
            # STAGE 5: QUALITY (How healthy is our email program?)
            'quality': {
                'total_bounced': totals['total_bounced'],
                'bounce_rate': round((totals['total_bounced'] / sent * 100) if sent > 0 else 0, 2),
                'deliverability_score': round((delivered / sent * 100) if sent > 0 else 0, 2),
                'engagement_quality': round(((totals['total_opened'] + totals['total_clicked']) / delivered * 100) if delivered > 0 else 0, 2)
            },
            
            # CAMPAIGN PERFORMANCE
            'campaigns': {
                'total_campaigns': len(campaigns),
                'active_campaigns': len([c for c in campaigns if c.get('status') == 'active']),
                'completed_campaigns': totals['total_completed'],
                'top_campaigns': sorted(campaign_list, key=lambda x: x['replied'], reverse=True)[:5]
            },
            
            # SALES PIPELINE (Instantly-specific)
            'sales_pipeline': {
                'opportunities': totals['total_opportunities'],
                'interested': totals['total_interested'],
                'meetings_booked': totals['total_meetings_booked'],
                'meetings_completed': totals['total_meetings_completed'],
                'deals_closed': totals['total_closed'],
                'meeting_show_rate': round((totals['total_meetings_completed'] / totals['total_meetings_booked'] * 100) if totals['total_meetings_booked'] > 0 else 0, 2),
                'close_rate': round((totals['total_closed'] / totals['total_leads'] * 100) if totals['total_leads'] > 0 else 0, 2)
            }
        }
        
        return metrics


class KlaviyoFetcher:
    """Fetch email metrics from Klaviyo API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://a.klaviyo.com/api"
        self.headers = {
            "Authorization": f"Klaviyo-API-Key {api_key}",
            "Accept": "application/json",
            "revision": "2025-10-15"
        }
    
    def get_metrics_list(self) -> List[Dict]:
        """Get all available metrics in the account"""
        try:
            # Don't use page[size] - it's not valid for metrics endpoint
            response = requests.get(
                f"{self.base_url}/metrics",
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            return data.get('data', [])
            
        except Exception as e:
            print(f"[ERROR] Failed to fetch metrics list: {e}")
            return []
    
    def find_metric_id(self, metric_name: str, debug: bool = False) -> Optional[str]:
        """Find metric ID by name"""
        metrics = self.get_metrics_list()
        
        if debug:
            print(f"[DEBUG] Searching for metric: '{metric_name}'")
            print(f"[DEBUG] Total metrics available: {len(metrics)}")
        
        for metric in metrics:
            if metric.get('attributes', {}).get('name') == metric_name:
                metric_id = metric.get('id')
                if debug:
                    print(f"[DEBUG] Found match! ID: {metric_id}")
                return metric_id
        
        if debug:
            print(f"[DEBUG] No exact match found. Available email metrics:")
            email_metrics = [m for m in metrics if 'email' in m.get('attributes', {}).get('name', '').lower()]
            for m in email_metrics[:10]:
                print(f"  - {m.get('attributes', {}).get('name')}: {m.get('id')}")
        
        return None
    
    def get_metric_aggregate(self, metric_id: str, start_date: str, end_date: str, debug: bool = False) -> Dict:
        """Get aggregated data for a specific metric"""
        try:
            response = requests.post(
                f"{self.base_url}/metric-aggregates",
                headers=self.headers,
                json={
                    "data": {
                        "type": "metric-aggregate",
                        "attributes": {
                            "metric_id": metric_id,
                            "measurements": ["count", "unique"],
                            "interval": "day",
                            "filter": [
                                f"greater-or-equal(datetime,{start_date})",
                                f"less-than(datetime,{end_date})"
                            ]
                        }
                    }
                }
            )
            response.raise_for_status()
            data = response.json()
            
            if debug:
                print(f"[DEBUG] Metric aggregate response type: {type(data)}")
                if isinstance(data, dict):
                    print(f"[DEBUG] Response keys: {list(data.keys())}")
            
            # Sum all values
            total = 0
            unique_total = 0
            
            if 'data' in data and 'attributes' in data['data']:
                measurements = data['data']['attributes'].get('data', [])
                for measurement in measurements:
                    measure_data = measurement.get('measurements', {})
                    
                    # Handle both single values and lists
                    count_val = measure_data.get('count', 0)
                    unique_val = measure_data.get('unique', 0)
                    
                    # If it's a list, sum it; otherwise just add the value
                    if isinstance(count_val, list):
                        total += sum(count_val)
                    else:
                        total += count_val
                    
                    if isinstance(unique_val, list):
                        unique_total += sum(unique_val)
                    else:
                        unique_total += unique_val
            
            if debug:
                print(f"[DEBUG] Total: {total}, Unique: {unique_total}")
            
            return {'total': total, 'unique': unique_total}
            
        except Exception as e:
            print(f"[ERROR] Failed to fetch metric aggregate: {e}")
            if debug:
                import traceback
                traceback.print_exc()
            return {'total': 0, 'unique': 0}
    
    def calculate_customer_journey_metrics(self, days: int = 30, debug: bool = False) -> Dict:
        """
        Calculate comprehensive customer journey metrics from Klaviyo data
        Returns metrics organized by journey stage
        """
        print("\n" + "="*70)
        print("KLAVIYO - CUSTOMER JOURNEY METRICS")
        print("="*70)
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")
        
        print(f"[INFO] Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Find metric IDs
        metric_mapping = {
            'Received Email': 'received',
            'Opened Email': 'opened',
            'Clicked Email': 'clicked',
            'Bounced Email': 'bounced',
            'Unsubscribed from Email Marketing': 'unsubscribed',  # Fixed: use full name
            'Marked Email as Spam': 'spam'
        }
        
        metric_ids = {}
        print("\n[INFO] Finding metric IDs...")
        
        for metric_name, key in metric_mapping.items():
            metric_id = self.find_metric_id(metric_name, debug=debug)
            if metric_id:
                metric_ids[key] = metric_id
                print(f"[OK] Found '{metric_name}': {metric_id}")
            else:
                print(f"[WARNING] Could not find '{metric_name}'")
        
        # Fetch all metrics
        data = {}
        print("\n[INFO] Fetching metrics data...")
        
        for key, metric_id in metric_ids.items():
            print(f"[{list(metric_ids.keys()).index(key)+1}/{len(metric_ids)}] Fetching {key}...")
            result = self.get_metric_aggregate(metric_id, start_str, end_str, debug=(debug and list(metric_ids.keys()).index(key) == 0))
            data[key] = result
        
        # Calculate totals
        received_total = data.get('received', {}).get('total', 0)
        received_unique = data.get('received', {}).get('unique', 0)
        opened_total = data.get('opened', {}).get('total', 0)
        opened_unique = data.get('opened', {}).get('unique', 0)
        clicked_total = data.get('clicked', {}).get('total', 0)
        clicked_unique = data.get('clicked', {}).get('unique', 0)
        bounced_total = data.get('bounced', {}).get('total', 0)
        unsubscribed_total = data.get('unsubscribed', {}).get('total', 0)
        spam_total = data.get('spam', {}).get('total', 0)
        
        metrics = {
            # STAGE 1: AWARENESS (How many people did we reach?)
            'awareness': {
                'emails_sent': received_total,
                'unique_recipients': received_unique,
                'emails_delivered': received_total - bounced_total,
                'delivery_rate': round(((received_total - bounced_total) / received_total * 100) if received_total > 0 else 0, 2),
                'bounce_rate': round((bounced_total / received_total * 100) if received_total > 0 else 0, 2),
                'net_reach': received_total - bounced_total
            },
            
            # STAGE 2: ENGAGEMENT (How many people engaged?)
            'engagement': {
                'total_opens': opened_total,
                'unique_opens': opened_unique,
                'total_clicks': clicked_total,
                'unique_clicks': clicked_unique,
                'open_rate': round((opened_unique / received_unique * 100) if received_unique > 0 else 0, 2),
                'click_rate': round((clicked_unique / received_unique * 100) if received_unique > 0 else 0, 2),
                'click_to_open_rate': round((clicked_unique / opened_unique * 100) if opened_unique > 0 else 0, 2),
                'avg_opens_per_recipient': round(opened_total / opened_unique, 2) if opened_unique > 0 else 0
            },
            
            # STAGE 3: RESPONSE (Conversions tracked separately via conversion metrics)
            'response': {
                'note': 'Add conversion tracking via specific metric IDs',
                'engaged_recipients': opened_unique,
                'highly_engaged': clicked_unique,
                'engagement_depth': round((clicked_unique / opened_unique * 100) if opened_unique > 0 else 0, 2)
            },
            
            # STAGE 4: RETENTION (Are people staying on our list?)
            'retention': {
                'total_unsubscribed': unsubscribed_total,
                'total_spam_complaints': spam_total,
                'unsubscribe_rate': round((unsubscribed_total / received_total * 100) if received_total > 0 else 0, 2),
                'spam_complaint_rate': round((spam_total / received_total * 100) if received_total > 0 else 0, 2),
                'list_health_score': round(100 - ((unsubscribed_total + spam_total) / received_total * 100) if received_total > 0 else 100, 2)
            },
            
            # STAGE 5: QUALITY (How healthy is our email program?)
            'quality': {
                'total_bounced': bounced_total,
                'bounce_rate': round((bounced_total / received_total * 100) if received_total > 0 else 0, 2),
                'deliverability_score': round(((received_total - bounced_total) / received_total * 100) if received_total > 0 else 0, 2),
                'engagement_quality': round(((opened_unique + clicked_unique) / received_unique * 100) if received_unique > 0 else 0, 2),
                'spam_score': spam_total
            },
            
            # OVERALL PERFORMANCE
            'summary': {
                'time_period': f"{days} days",
                'total_sent': received_total,
                'unique_recipients': received_unique,
                'overall_engagement_rate': round(((opened_unique / received_unique * 100)) if received_unique > 0 else 0, 2)
            }
        }
        
        return metrics


def print_customer_journey_report(metrics: Dict, platform: str):
    """Print a formatted customer journey report"""
    print("\n" + "="*70)
    print(f"{platform.upper()} - CUSTOMER JOURNEY ANALYSIS")
    print("="*70)
    
    # STAGE 1: AWARENESS
    if 'awareness' in metrics:
        awareness = metrics['awareness']
        print("\n1. AWARENESS - How many people did we reach?")
        print("-" * 70)
        print(f"  Emails Sent:        {awareness.get('emails_sent', 0):,}")
        print(f"  Emails Delivered:   {awareness.get('emails_delivered', 0):,}")
        print(f"  Delivery Rate:      {awareness.get('delivery_rate', 0)}%")
        print(f"  Bounce Rate:        {awareness.get('bounce_rate', 0)}%")
        print(f"  Net Reach:          {awareness.get('net_reach', 0):,} people")
    
    # STAGE 2: ENGAGEMENT
    if 'engagement' in metrics:
        engagement = metrics['engagement']
        print("\n2. ENGAGEMENT - How many people engaged with our emails?")
        print("-" * 70)
        print(f"  Total Opens:        {engagement.get('total_opened', engagement.get('total_opens', 0)):,}")
        print(f"  Total Clicks:       {engagement.get('total_clicked', engagement.get('total_clicks', 0)):,}")
        print(f"  Open Rate:          {engagement.get('open_rate', 0)}%")
        print(f"  Click Rate:         {engagement.get('click_rate', 0)}%")
        print(f"  Click-to-Open:      {engagement.get('click_to_open_rate', 0)}%")
    
    # STAGE 3: RESPONSE
    if 'response' in metrics:
        response = metrics['response']
        print("\n3. RESPONSE - How many people took action?")
        print("-" * 70)
        
        if 'total_replied' in response:
            print(f"  Total Replies:      {response.get('total_replied', 0):,}")
            print(f"  Total Leads:        {response.get('total_leads', 0):,}")
            print(f"  Reply Rate:         {response.get('reply_rate', 0)}%")
            print(f"  Lead Conv. Rate:    {response.get('lead_conversion_rate', 0)}%")
        else:
            print(f"  Engaged Recipients: {response.get('engaged_recipients', 0):,}")
            print(f"  Highly Engaged:     {response.get('highly_engaged', 0):,}")
            print(f"  Engagement Depth:   {response.get('engagement_depth', 0)}%")
    
    # STAGE 4: RETENTION
    if 'retention' in metrics:
        retention = metrics['retention']
        print("\n4. RETENTION - Are people staying on our list?")
        print("-" * 70)
        print(f"  Unsubscribes:       {retention.get('total_unsubscribed', 0):,}")
        print(f"  Unsubscribe Rate:   {retention.get('unsubscribe_rate', 0)}%")
        
        if 'active_list_size' in retention:
            print(f"  Active List Size:   {retention.get('active_list_size', 0):,}")
        if 'list_health_score' in retention:
            print(f"  List Health Score:  {retention.get('list_health_score', 0)}%")
    
    # STAGE 5: QUALITY
    if 'quality' in metrics:
        quality = metrics['quality']
        print("\n5. QUALITY - How healthy is our email program?")
        print("-" * 70)
        print(f"  Total Bounces:      {quality.get('total_bounced', 0):,}")
        print(f"  Bounce Rate:        {quality.get('bounce_rate', 0)}%")
        print(f"  Deliverability:     {quality.get('deliverability_score', 0)}%")
        print(f"  Engagement Quality: {quality.get('engagement_quality', 0)}%")
    
    # SALES PIPELINE (Instantly only)
    if 'sales_pipeline' in metrics:
        pipeline = metrics['sales_pipeline']
        print("\n6. SALES PIPELINE - Business outcomes (Instantly-specific)")
        print("-" * 70)
        print(f"  Opportunities:      {pipeline.get('opportunities', 0):,}")
        print(f"  Interested:         {pipeline.get('interested', 0):,}")
        print(f"  Meetings Booked:    {pipeline.get('meetings_booked', 0):,}")
        print(f"  Meetings Completed: {pipeline.get('meetings_completed', 0):,}")
        print(f"  Deals Closed:       {pipeline.get('deals_closed', 0):,}")
        print(f"  Meeting Show Rate:  {pipeline.get('meeting_show_rate', 0)}%")
        print(f"  Close Rate:         {pipeline.get('close_rate', 0)}%")
    
    print("\n" + "="*70)


def export_to_json(metrics: Dict, filename: str):
    """Export metrics to JSON file"""
    with open(filename, 'w') as f:
        json.dump(metrics, f, indent=2)
    print(f"\n[OK] Metrics exported to: {filename}")


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Fetch email metrics across the complete customer journey'
    )
    parser.add_argument(
        '--platform',
        choices=['instantly', 'klaviyo', 'both'],
        default='both',
        help='Email platform to fetch from'
    )
    parser.add_argument(
        '--instantly-key',
        help='Instantly.ai API key'
    )
    parser.add_argument(
        '--klaviyo-key',
        help='Klaviyo API key'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=30,
        help='Number of days for Klaviyo metrics (default: 30)'
    )
    parser.add_argument(
        '--export',
        action='store_true',
        help='Export results to JSON files'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Show debug output for API responses'
    )
    
    args = parser.parse_args()
    
    # Get API keys from args or environment
    instantly_key = args.instantly_key or os.getenv('INSTANTLY_API_KEY')
    klaviyo_key = args.klaviyo_key or os.getenv('KLAVIYO_API_KEY')
    
    results = {}
    
    # Fetch Instantly metrics
    if args.platform in ['instantly', 'both']:
        if not instantly_key:
            print("[ERROR] Instantly API key required. Use --instantly-key or set INSTANTLY_API_KEY")
        else:
            fetcher = InstantlyFetcher(instantly_key)
            metrics = fetcher.calculate_customer_journey_metrics(days=args.days, debug=args.debug)
            results['instantly'] = metrics
            print_customer_journey_report(metrics, 'Instantly')
            
            if args.export:
                export_to_json(metrics, 'instantly_customer_journey.json')
    
    # Fetch Klaviyo metrics
    if args.platform in ['klaviyo', 'both']:
        if not klaviyo_key:
            print("[ERROR] Klaviyo API key required. Use --klaviyo-key or set KLAVIYO_API_KEY")
        else:
            fetcher = KlaviyoFetcher(klaviyo_key)
            metrics = fetcher.calculate_customer_journey_metrics(days=args.days, debug=args.debug)
            results['klaviyo'] = metrics
            print_customer_journey_report(metrics, 'Klaviyo')
            
            if args.export:
                export_to_json(metrics, 'klaviyo_customer_journey.json')
    
    # Export combined results
    if args.export and len(results) > 1:
        export_to_json(results, 'email_customer_journey_combined.json')
    
    return results


if __name__ == '__main__':
    main()
