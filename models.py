"""
Database Models for ContentClicks Customer Journey Dashboard
Uses Firestore for cloud-native storage with historical tracking
"""

from datetime import datetime
from typing import Dict, List, Optional
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
import os

# Initialize Firestore client with error handling
try:
    db = firestore.Client()
    print("[OK] Firestore client initialized")
except Exception as e:
    print(f"[ERROR] Failed to initialize Firestore: {e}")
    print("[INFO] Make sure Firestore database is created and permissions are granted")
    # Create a dummy client for import to work, actual operations will fail gracefully
    db = None

# Collection references
CUSTOMERS_COLLECTION = 'customers'
CREDENTIALS_COLLECTION = 'credentials'
HISTORICAL_METRICS_COLLECTION = 'historical_metrics'
TOP_PERFORMERS_COLLECTION = 'top_performers'


class Database:
    """Database connection manager for Firestore"""
    
    @staticmethod
    def get_connection():
        """Get Firestore client"""
        if db is None:
            raise RuntimeError("Firestore client not initialized. Check logs for initialization errors.")
        return db
    
    @staticmethod
    def init_db():
        """Initialize Firestore collections (no-op for Firestore, collections are created on first write)"""
        if db is None:
            print("[ERROR] Firestore client not initialized")
            return False
        print("[OK] Firestore initialized successfully")
        return True


class Customer:
    """Customer profile model"""
    
    @staticmethod
    def create(name: str, industry: str) -> str:
        """Create a new customer profile"""
        customer_ref = db.collection(CUSTOMERS_COLLECTION).document()
        customer_id = customer_ref.id
        
        customer_data = {
            'name': name,
            'industry': industry,
            'created_at': firestore.SERVER_TIMESTAMP,
            'updated_at': firestore.SERVER_TIMESTAMP
        }
        
        customer_ref.set(customer_data)
        
        return customer_id
    
    @staticmethod
    def get_all() -> List[Dict]:
        """Get all customer profiles"""
        customers = []
        customers_ref = db.collection(CUSTOMERS_COLLECTION).order_by('name').stream()
        
        for doc in customers_ref:
            customer_data = doc.to_dict()
            customer_data['id'] = doc.id
            customers.append(customer_data)
        
        return customers
    
    @staticmethod
    def get_by_id(customer_id: str) -> Optional[Dict]:
        """Get customer profile by ID"""
        doc_ref = db.collection(CUSTOMERS_COLLECTION).document(customer_id)
        doc = doc_ref.get()
        
        if doc.exists:
            customer_data = doc.to_dict()
            customer_data['id'] = doc.id
            return customer_data
        
        return None
    
    @staticmethod
    def update(customer_id: str, name: str = None, industry: str = None):
        """Update customer profile"""
        doc_ref = db.collection(CUSTOMERS_COLLECTION).document(customer_id)
        
        updates = {'updated_at': firestore.SERVER_TIMESTAMP}
        
        if name:
            updates['name'] = name
        
        if industry:
            updates['industry'] = industry
        
        doc_ref.update(updates)
    
    @staticmethod
    def delete(customer_id: str):
        """Delete customer profile and all associated data"""
        # Delete customer document
        db.collection(CUSTOMERS_COLLECTION).document(customer_id).delete()
        
        # Delete credentials
        credentials_ref = db.collection(CREDENTIALS_COLLECTION).document(customer_id)
        credentials_ref.delete()
        
        # Delete historical metrics (batch delete)
        metrics_ref = db.collection(HISTORICAL_METRICS_COLLECTION).document(customer_id)
        metrics_ref.delete()
        
        # Delete top performers
        performers_ref = db.collection(TOP_PERFORMERS_COLLECTION).document(customer_id)
        performers_ref.delete()


class CustomerCredential:
    """Customer API credentials model"""
    
    @staticmethod
    def set(customer_id: str, platform: str, credential_key: str, credential_value: str):
        """Set or update a customer credential"""
        doc_ref = db.collection(CREDENTIALS_COLLECTION).document(customer_id)
        
        # Get existing credentials or create new
        doc = doc_ref.get()
        if doc.exists:
            credentials = doc.to_dict()
        else:
            credentials = {}
        
        # Create nested structure: platform -> credential_key -> value
        if platform not in credentials:
            credentials[platform] = {}
        
        credentials[platform][credential_key] = credential_value
        
        doc_ref.set(credentials, merge=True)
    
    @staticmethod
    def get(customer_id: str, platform: str, credential_key: str) -> Optional[str]:
        """Get a specific credential"""
        doc_ref = db.collection(CREDENTIALS_COLLECTION).document(customer_id)
        doc = doc_ref.get()
        
        if doc.exists:
            credentials = doc.to_dict()
            return credentials.get(platform, {}).get(credential_key)
        
        return None
    
    @staticmethod
    def get_all_for_customer(customer_id: str) -> Dict[str, Dict[str, str]]:
        """Get all credentials for a customer, organized by platform"""
        doc_ref = db.collection(CREDENTIALS_COLLECTION).document(customer_id)
        doc = doc_ref.get()
        
        if doc.exists:
            return doc.to_dict()
        
        return {}
    
    @staticmethod
    def delete(customer_id: str, platform: str, credential_key: str):
        """Delete a specific credential"""
        doc_ref = db.collection(CREDENTIALS_COLLECTION).document(customer_id)
        doc = doc_ref.get()
        
        if doc.exists:
            credentials = doc.to_dict()
            if platform in credentials and credential_key in credentials[platform]:
                del credentials[platform][credential_key]
                
                # Remove platform if empty
                if not credentials[platform]:
                    del credentials[platform]
                
                doc_ref.set(credentials)


class HistoricalMetric:
    """Historical KPI metrics model with monthly snapshots"""
    
    @staticmethod
    def add(customer_id: str, medium: str, journey_stage: str, kpi_name: str, 
            kpi_value: float, benchmark_value: float, time_period_days: int,
            year: int = None, month: int = None):
        """Add a historical metric record with monthly snapshot"""
        
        # Use provided year/month or current
        if year is None or month is None:
            now = datetime.now()
            year = now.year
            month = now.month
        
        # Structure: historical_metrics/{customer_id}/{medium}/{journey_stage}/{year}/{month}/{kpi_name}
        path = f"{HISTORICAL_METRICS_COLLECTION}/{customer_id}/{medium}/{journey_stage}/{year}/{month}/kpis/{kpi_name}"
        print(f"        [FIRESTORE] Writing to: {path}")
        print(f"        [FIRESTORE] Value: {kpi_value}")

        doc_ref = (db.collection(HISTORICAL_METRICS_COLLECTION)
                   .document(customer_id)
                   .collection(medium)
                   .document(journey_stage)
                   .collection(str(year))
                   .document(str(month))
                   .collection('kpis')
                   .document(kpi_name))

        metric_data = {
            'kpi_value': kpi_value,
            'benchmark_value': benchmark_value,
            'time_period_days': time_period_days,
            'recorded_at': firestore.SERVER_TIMESTAMP,
            'year': year,
            'month': month
        }

        doc_ref.set(metric_data)
        print(f"        [FIRESTORE] ✓ Written successfully")
    
    @staticmethod
    def get_history(customer_id: str, medium: str, journey_stage: str,
                    kpi_name: str, months: int = 12) -> List[Dict]:
        """Get historical data for a specific KPI (last N months)"""

        print(f"[FIRESTORE READ] Getting history for {customer_id}/{medium}/{journey_stage}/{kpi_name}")

        # Get current year and month
        now = datetime.now()
        current_year = now.year
        current_month = now.month

        results = []

        # Iterate through last N months
        for i in range(months):
            # Calculate year and month for this iteration
            month = current_month - i
            year = current_year

            while month <= 0:
                month += 12
                year -= 1

            # Query this month's data
            try:
                path = f"{HISTORICAL_METRICS_COLLECTION}/{customer_id}/{medium}/{journey_stage}/{year}/{month}/kpis/{kpi_name}"

                doc_ref = (db.collection(HISTORICAL_METRICS_COLLECTION)
                          .document(customer_id)
                          .collection(medium)
                          .document(journey_stage)
                          .collection(str(year))
                          .document(str(month))
                          .collection('kpis')
                          .document(kpi_name))

                doc = doc_ref.get()

                if doc.exists:
                    data = doc.to_dict()
                    data['date'] = f"{year}-{month:02d}"
                    results.append(data)
                    print(f"[FIRESTORE READ] ✓ Found {year}-{month:02d}: value={data.get('kpi_value')}")
                else:
                    print(f"[FIRESTORE READ] ✗ No data for {year}-{month:02d} at {path}")
            except Exception as e:
                print(f"[FIRESTORE READ ERROR] Could not fetch data for {year}-{month:02d}: {e}")
                continue

        # Reverse to get chronological order (oldest to newest)
        results.reverse()

        print(f"[FIRESTORE READ] Returning {len(results)} data points")
        return results
    
    @staticmethod
    def get_latest_for_customer(customer_id: str) -> Dict:
        """Get the latest metrics for all KPIs for a customer"""

        print(f"[FIRESTORE READ] Getting latest metrics for customer {customer_id}")

        now = datetime.now()
        current_year = now.year
        current_month = now.month

        print(f"[FIRESTORE READ] Looking for data from {current_year}-{current_month:02d}")

        metrics = {}

        # Define known mediums and stages
        mediums_and_stages = {
            'social_media': ['awareness', 'engagement', 'conversion', 'retention', 'advocacy'],
            'social_media_facebook': ['awareness', 'engagement', 'conversion', 'retention', 'advocacy'],
            'social_media_instagram': ['awareness', 'engagement', 'conversion', 'retention', 'advocacy'],
            'email': ['awareness', 'engagement', 'response', 'retention', 'quality'],
            'website': ['awareness', 'engagement', 'conversion', 'retention', 'advocacy']
        }

        for medium, stages in mediums_and_stages.items():
            metrics[medium] = {}

            for journey_stage in stages:
                # Get current month's data for this medium/stage
                try:
                    kpis_ref = (db.collection(HISTORICAL_METRICS_COLLECTION)
                               .document(customer_id)
                               .collection(medium)
                               .document(journey_stage)
                               .collection(str(current_year))
                               .document(str(current_month))
                               .collection('kpis'))

                    kpis = kpis_ref.stream()

                    # Get all KPIs for this stage (there may be multiple)
                    kpi_list = []
                    for kpi_doc in kpis:
                        kpi_data = kpi_doc.to_dict()
                        kpi_data['kpi_name'] = kpi_doc.id
                        kpi_list.append(kpi_data)
                        print(f"[FIRESTORE READ] ✓ Found {medium}/{journey_stage}/{kpi_doc.id} = {kpi_data.get('kpi_value')}")

                    # If we have KPIs, use the first one for the old structure
                    # but this should really return all KPIs
                    if kpi_list:
                        metrics[medium][journey_stage] = kpi_list[0]  # For backwards compatibility
                        # TODO: Should return all KPIs, not just first one
                    else:
                        print(f"[FIRESTORE READ] ✗ No KPIs found for {medium}/{journey_stage}")

                except Exception as e:
                    print(f"[FIRESTORE READ ERROR] Could not fetch latest for {medium}/{journey_stage}: {e}")
        
        return metrics


class TopPerformer:
    """Top performing content model"""
    
    @staticmethod
    def add(customer_id: str, medium: str, item_id: str, item_title: str, 
            metric_name: str, metric_value: float):
        """Add a top performer record"""
        
        now = datetime.now()
        date_key = now.strftime('%Y-%m-%d')
        
        # Structure: top_performers/{customer_id}/{medium}/{date}/{item_id}
        doc_ref = (db.collection(TOP_PERFORMERS_COLLECTION)
                   .document(customer_id)
                   .collection(medium)
                   .document(date_key)
                   .collection('items')
                   .document(item_id))
        
        performer_data = {
            'item_title': item_title,
            'metric_name': metric_name,
            'metric_value': metric_value,
            'recorded_at': firestore.SERVER_TIMESTAMP
        }
        
        doc_ref.set(performer_data)
    
    @staticmethod
    def get_latest_for_customer(customer_id: str, medium: str, limit: int = 10) -> List[Dict]:
        """Get latest top performers for a customer and medium"""
        
        now = datetime.now()
        date_key = now.strftime('%Y-%m-%d')
        
        try:
            # Get today's top performers
            items_ref = (db.collection(TOP_PERFORMERS_COLLECTION)
                        .document(customer_id)
                        .collection(medium)
                        .document(date_key)
                        .collection('items')
                        .order_by('metric_value', direction=firestore.Query.DESCENDING)
                        .limit(limit))
            
            items = items_ref.stream()
            
            results = []
            for item_doc in items:
                item_data = item_doc.to_dict()
                item_data['id'] = item_doc.id
                item_data['item_id'] = item_doc.id
                results.append(item_data)
            
            return results
        except Exception as e:
            print(f"[WARNING] Could not fetch top performers: {e}")
            return []


# ============================================================================
# Industry benchmarks — per 30-day month unless noted as lifetime
#
# Social Media:
#   reach, interactions, link_clicks, shares  → monthly totals
#   followers                                 → lifetime total (point-in-time)
#
# Website (all monthly):
#   sessions, users, returning_users, referrals, form_submits → counts/month
#   pages_per_session, avg_session_duration (seconds)         → averages
#   form_submit_rate, retention_rate                          → percentages
#
# Email (all monthly):
#   emails_sent/delivered/opens/clicks/replies/unsubscribes   → counts/month
#   deliverability_score                                      → percentage
# ============================================================================
INDUSTRY_BENCHMARKS = {
    # ------------------------------------------------------------------ #
    #  Healthcare — general practice, small local clinic (50–200 patients)
    #  Sources: Mailchimp 2024, Hootsuite Healthcare Report, SEMrush SMB data
    # ------------------------------------------------------------------ #
    'healthcare': {
        'social_media': {
            # Organic reach across all posts, combined FB + IG (~650 total followers)
            'awareness':  {'reach': 780, 'impressions': 1400},
            # Reactions + comments across all posts (~2% engagement on reach)
            'engagement': {'engagement_rate': 2.0, 'interactions': 40},
            # Outbound link clicks (CTA, website link in bio/post)
            'conversion': {'link_clicks': 20, 'cta_clicks': 8},
            # Lifetime follower count (combined) — realistic starting benchmark
            'retention':  {'followers': 650, 'follower_growth': 18},
            'advocacy':   {'shares': 5, 'mentions': 2}
        },
        'social_media_facebook': {
            'awareness':  {'reach': 470},
            'engagement': {'interactions': 24},
            'conversion': {'link_clicks': 14},
            'retention':  {'followers': 390},   # lifetime
            'advocacy':   {'shares': 3}
        },
        'social_media_instagram': {
            'awareness':  {'reach': 310},
            'engagement': {'interactions': 16},
            'conversion': {'link_clicks': 6},
            'retention':  {'followers': 260},   # lifetime
            'advocacy':   {'shares': 2}
        },
        'website': {
            # Small local healthcare site, primarily organic + local search
            'awareness':  {'sessions': 480, 'users': 380},
            # avg_session_duration in seconds; 2 min is solid for info-seeking visitors
            'engagement': {'pages_per_session': 2.5, 'avg_session_duration': 115},
            # ~2% of sessions convert to a contact/booking form submission
            'conversion': {'form_submits': 9, 'form_submit_rate': 1.9},
            # ~25% of users are returning; count = users × retention_rate / 100
            'retention':  {'returning_users': 95, 'retention_rate': 25},
            # ~12% of sessions arrive via referral (partner sites, directories)
            'advocacy':   {'referrals': 58}
        },
        'email': {
            # Assumes ~1000-contact list, weekly or bi-weekly sends
            'awareness':  {'emails_sent': 1000, 'emails_delivered': 960},
            # 22% open rate, 3% click rate — healthcare avg (Mailchimp 2024)
            'engagement': {'email_opens': 211, 'email_clicks': 29},
            'response':   {'email_replies': 50},
            # <0.5% unsubscribe per send is healthy
            'retention':  {'unsubscribes': 4},
            'quality':    {'deliverability_score': 94}
        }
    },

    # ------------------------------------------------------------------ #
    #  Dental — local dental practice
    #  Dental tends to skew slightly higher on social engagement & form submits
    #  Sources: Dental Economics, Hootsuite SMB, Mailchimp Industry Report
    # ------------------------------------------------------------------ #
    'dental': {
        'social_media': {
            'awareness':  {'reach': 940, 'impressions': 1750},
            'engagement': {'engagement_rate': 2.4, 'interactions': 50},
            'conversion': {'link_clicks': 26, 'cta_clicks': 10},
            'retention':  {'followers': 760, 'follower_growth': 22},
            'advocacy':   {'shares': 7, 'mentions': 3}
        },
        'social_media_facebook': {
            'awareness':  {'reach': 560},
            'engagement': {'interactions': 30},
            'conversion': {'link_clicks': 18},
            'retention':  {'followers': 455},   # lifetime
            'advocacy':   {'shares': 4}
        },
        'social_media_instagram': {
            'awareness':  {'reach': 380},
            'engagement': {'interactions': 20},
            'conversion': {'link_clicks': 8},
            'retention':  {'followers': 305},   # lifetime
            'advocacy':   {'shares': 3}
        },
        'website': {
            # Dental sites get strong local-search intent traffic
            'awareness':  {'sessions': 560, 'users': 440},
            'engagement': {'pages_per_session': 2.8, 'avg_session_duration': 130},
            # Dental appointment bookings: ~2.3% conversion is solid
            'conversion': {'form_submits': 13, 'form_submit_rate': 2.3},
            # ~27% returning users (check-up reminders drive repeat visits)
            'retention':  {'returning_users': 119, 'retention_rate': 27},
            # ~13% referral traffic (Healthgrades, Zocdoc, local directories)
            'advocacy':   {'referrals': 73}
        },
        'email': {
            'awareness':  {'emails_sent': 800, 'emails_delivered': 768},
            # 23% open rate, 3.5% click rate for dental (strong appointment CTAs)
            'engagement': {'email_opens': 177, 'email_clicks': 27},
            'response':   {'email_replies': 44},
            'retention':  {'unsubscribes': 3},
            'quality':    {'deliverability_score': 95}
        }
    },

    # ------------------------------------------------------------------ #
    #  Medical — specialist clinic / GP practice
    #  Slightly lower social engagement than dental; higher email reliability
    # ------------------------------------------------------------------ #
    'medical': {
        'social_media': {
            'awareness':  {'reach': 840, 'impressions': 1600},
            'engagement': {'engagement_rate': 1.8, 'interactions': 38},
            'conversion': {'link_clicks': 18, 'cta_clicks': 7},
            'retention':  {'followers': 640, 'follower_growth': 15},
            'advocacy':   {'shares': 5, 'mentions': 2}
        },
        'social_media_facebook': {
            'awareness':  {'reach': 500},
            'engagement': {'interactions': 23},
            'conversion': {'link_clicks': 13},
            'retention':  {'followers': 385},   # lifetime
            'advocacy':   {'shares': 3}
        },
        'social_media_instagram': {
            'awareness':  {'reach': 340},
            'engagement': {'interactions': 15},
            'conversion': {'link_clicks': 5},
            'retention':  {'followers': 255},   # lifetime
            'advocacy':   {'shares': 2}
        },
        'website': {
            'awareness':  {'sessions': 520, 'users': 410},
            'engagement': {'pages_per_session': 2.7, 'avg_session_duration': 125},
            'conversion': {'form_submits': 11, 'form_submit_rate': 2.1},
            # ~25% returning; patients returning for appointment info
            'retention':  {'returning_users': 103, 'retention_rate': 25},
            # ~12% referral (specialist directories, GP referral links)
            'advocacy':   {'referrals': 62}
        },
        'email': {
            'awareness':  {'emails_sent': 900, 'emails_delivered': 864},
            # 21% open rate, 3% click — medical slightly lower than dental
            'engagement': {'email_opens': 182, 'email_clicks': 26},
            'response':   {'email_replies': 48},
            'retention':  {'unsubscribes': 4},
            'quality':    {'deliverability_score': 94}
        }
    },

    # ------------------------------------------------------------------ #
    #  Other — generic small local business
    #  Used when a customer selects "Other" as their industry
    #  Slightly lower benchmarks as a conservative generic baseline
    # ------------------------------------------------------------------ #
    'other': {
        'social_media': {
            'awareness':  {'reach': 620, 'impressions': 1150},
            'engagement': {'engagement_rate': 1.8, 'interactions': 28},
            'conversion': {'link_clicks': 14, 'cta_clicks': 5},
            'retention':  {'followers': 520, 'follower_growth': 12},
            'advocacy':   {'shares': 4, 'mentions': 1}
        },
        'social_media_facebook': {
            'awareness':  {'reach': 370},
            'engagement': {'interactions': 17},
            'conversion': {'link_clicks': 10},
            'retention':  {'followers': 310},   # lifetime
            'advocacy':   {'shares': 2}
        },
        'social_media_instagram': {
            'awareness':  {'reach': 250},
            'engagement': {'interactions': 11},
            'conversion': {'link_clicks': 4},
            'retention':  {'followers': 210},   # lifetime
            'advocacy':   {'shares': 2}
        },
        'website': {
            'awareness':  {'sessions': 400, 'users': 315},
            'engagement': {'pages_per_session': 2.2, 'avg_session_duration': 100},
            'conversion': {'form_submits': 7, 'form_submit_rate': 1.8},
            'retention':  {'returning_users': 79, 'retention_rate': 25},
            'advocacy':   {'referrals': 48}
        },
        'email': {
            'awareness':  {'emails_sent': 750, 'emails_delivered': 720},
            # 21% open rate, 3% click — generic SMB (Mailchimp all-industry avg)
            'engagement': {'email_opens': 151, 'email_clicks': 22},
            'response':   {'email_replies': 30},
            'retention':  {'unsubscribes': 4},
            'quality':    {'deliverability_score': 92}
        }
    },

    # ------------------------------------------------------------------ #
    #  Default — fallback for unknown/unset industry (mirrors 'other')
    # ------------------------------------------------------------------ #
    'default': {
        'social_media': {
            'awareness':  {'reach': 620, 'impressions': 1150},
            'engagement': {'engagement_rate': 1.8, 'interactions': 28},
            'conversion': {'link_clicks': 14, 'cta_clicks': 5},
            'retention':  {'followers': 520, 'follower_growth': 12},
            'advocacy':   {'shares': 4, 'mentions': 1}
        },
        'social_media_facebook': {
            'awareness':  {'reach': 370},
            'engagement': {'interactions': 17},
            'conversion': {'link_clicks': 10},
            'retention':  {'followers': 310},
            'advocacy':   {'shares': 2}
        },
        'social_media_instagram': {
            'awareness':  {'reach': 250},
            'engagement': {'interactions': 11},
            'conversion': {'link_clicks': 4},
            'retention':  {'followers': 210},
            'advocacy':   {'shares': 2}
        },
        'website': {
            'awareness':  {'sessions': 400, 'users': 315},
            'engagement': {'pages_per_session': 2.2, 'avg_session_duration': 100},
            'conversion': {'form_submits': 7, 'form_submit_rate': 1.8},
            'retention':  {'returning_users': 79, 'retention_rate': 25},
            'advocacy':   {'referrals': 48}
        },
        'email': {
            'awareness':  {'emails_sent': 750, 'emails_delivered': 720},
            'engagement': {'email_opens': 151, 'email_clicks': 22},
            'response':   {'email_replies': 30},
            'retention':  {'unsubscribes': 4},
            'quality':    {'deliverability_score': 92}
        }
    }
}


def get_benchmark(industry: str, medium: str, journey_stage: str, kpi_name: str) -> float:
    """Get benchmark value for a specific KPI"""
    industry_key = industry.lower()
    
    # Use industry-specific benchmarks or fall back to default
    benchmarks = INDUSTRY_BENCHMARKS.get(industry_key, INDUSTRY_BENCHMARKS['default'])
    
    if medium in benchmarks and journey_stage in benchmarks[medium]:
        stage_benchmarks = benchmarks[medium][journey_stage]
        
        # Try to find matching benchmark
        for key, value in stage_benchmarks.items():
            if key.lower() in kpi_name.lower() or kpi_name.lower() in key.lower():
                return value
    
    # Default fallback
    return 0


if __name__ == '__main__':
    # Initialize database
    Database.init_db()
    print("Firestore initialized successfully!")
