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
# Industry benchmarks — per 30-day month unless noted as LIFETIME
#
# Basis for social media numbers (Hootsuite 2025, Socialinsider 2024,
# Social Status 2024):
#   Nano-pages (<1 K followers) FB reach ≈ 5–7% per post × 8 posts/month
#   Nano-pages IG reach ≈ 8–15% per post × 8 posts/month
#   FB engagement ≈ 1.9% of reached accounts  (Hootsuite healthcare 2025)
#   IG engagement ≈ 3.7% of reached accounts  (Hootsuite healthcare 2025)
#
# Basis for email numbers (Mailchimp 2024, Paubox 2024, MailerLite 2025):
#   Healthcare open rate ≈ 34–38%, click rate ≈ 1.8–2.5%
#   Dental    open rate ≈ 35–40%, click rate ≈ 2.0–2.8%
#   Medical   open rate ≈ 33–37%, click rate ≈ 1.7–2.3%
#   All-industry open rate ≈ 21%, click rate ≈ 2.6%  (Mailchimp)
#   Unsubscribe rate ≈ 0.06–0.20% per send  (Paubox healthcare empirical)
#   Email replies: ~0.3–0.8% of opens  (practitioner-reported)
#
# Basis for website numbers (9Clouds, FirstPageSage 2024):
#   Small local practice organic sessions ≈ 250–600/month
#   Returning user rate ≈ 18–25%
#   Referral sessions ≈ 5–12% of total sessions
#   Form submit rate: healthcare 2.0–2.5%, dental 2.5–3.5%, medical 1.8–2.4%
#
# Social Media:
#   reach, interactions, link_clicks, shares  → monthly totals
#   followers                                 → LIFETIME total (point-in-time)
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
    #  Healthcare — general practice / clinic  (50–200 patients)
    #  ~500 FB followers, ~400 IG followers assumed for raw-number calc
    # ------------------------------------------------------------------ #
    'healthcare': {
        'social_media': {
            # Combined FB + IG organic reach across all posts (~8 posts each/month)
            # FB: 500 followers × 6% reach × 8 posts de-duped ≈ 275
            # IG: 400 followers × 10% reach × 8 posts de-duped ≈ 395
            'awareness':  {'reach': 670, 'impressions': 1250},
            # FB: 275 reached × 1.9% ≈ 5   IG: 395 reached × 3.7% ≈ 15
            'engagement': {'engagement_rate': 1.9, 'interactions': 20},
            # Organic post link clicks (FB) + bio/Stories link taps (IG)
            'conversion': {'link_clicks': 16, 'cta_clicks': 6},
            # LIFETIME follower count (combined FB + IG)
            'retention':  {'followers': 650, 'follower_growth': 15},
            # FB shares ≈ 4, IG sends/reposts ≈ 6
            'advocacy':   {'shares': 10, 'mentions': 2}
        },
        'social_media_facebook': {
            # 500 followers × 6% reach × 8 posts de-duped
            'awareness':  {'reach': 275},
            # 275 reached × 1.9%
            'engagement': {'interactions': 5},
            'conversion': {'link_clicks': 10},
            'retention':  {'followers': 390},   # LIFETIME
            'advocacy':   {'shares': 4}
        },
        'social_media_instagram': {
            # 400 followers × 10% reach × 8 posts de-duped
            'awareness':  {'reach': 395},
            # 395 reached × 3.7%
            'engagement': {'interactions': 15},
            'conversion': {'link_clicks': 6},
            'retention':  {'followers': 260},   # LIFETIME
            'advocacy':   {'shares': 6}
        },
        'website': {
            # Organic-only, basic local SEO  (9Clouds: 250–600 is "good")
            'awareness':  {'sessions': 420, 'users': 335},
            # pages/session 2.0–3.2; avg duration 90–180 s  (9Clouds)
            'engagement': {'pages_per_session': 2.5, 'avg_session_duration': 115},
            # 2.1% form submit rate  (FirstPageSage: general practice 2.0–2.5%)
            'conversion': {'form_submits': 9, 'form_submit_rate': 2.1},
            # 22% returning user rate  (18–25% per Littledata/9Clouds)
            # 335 users × 22% ≈ 74 returning users
            'retention':  {'returning_users': 74, 'retention_rate': 22},
            # ~8% referral sessions (Healthgrades, Yelp, insurance directories)
            'advocacy':   {'referrals': 34}
        },
        'email': {
            # ~1 000-contact list, 2 campaigns/month
            'awareness':  {'emails_sent': 1000, 'emails_delivered': 960},
            # 34% open rate, 1.9% click  (Mailchimp 2024: healthcare 34.6%, CTR 1.87%)
            'engagement': {'email_opens': 326, 'email_clicks': 18},
            # ~0.5% of opens reply  (practitioner-reported estimate)
            'response':   {'email_replies': 5},
            # 0.06–0.20% unsubscribe rate  (Paubox empirical healthcare data)
            'retention':  {'unsubscribes': 2},
            'quality':    {'deliverability_score': 95}
        }
    },

    # ------------------------------------------------------------------ #
    #  Dental — local dental practice  (50–200 patients)
    #  Dental skews higher: visual content, stronger appointment intent
    #  ~540 FB followers, ~440 IG followers assumed
    # ------------------------------------------------------------------ #
    'dental': {
        'social_media': {
            # FB: 540 × 6% × 8 de-duped ≈ 300   IG: 440 × 10% × 8 de-duped ≈ 430
            'awareness':  {'reach': 730, 'impressions': 1380},
            # FB: 300 × 1.9% ≈ 6   IG: 430 × 3.7% ≈ 16   (dental visual content lifts IG)
            'engagement': {'engagement_rate': 2.1, 'interactions': 22},
            'conversion': {'link_clicks': 20, 'cta_clicks': 8},
            'retention':  {'followers': 780, 'follower_growth': 20},
            # FB shares ≈ 5, IG sends ≈ 8  (before/after content is highly shared)
            'advocacy':   {'shares': 13, 'mentions': 3}
        },
        'social_media_facebook': {
            'awareness':  {'reach': 300},
            'engagement': {'interactions': 6},
            'conversion': {'link_clicks': 12},
            'retention':  {'followers': 470},   # LIFETIME
            'advocacy':   {'shares': 5}
        },
        'social_media_instagram': {
            'awareness':  {'reach': 430},
            'engagement': {'interactions': 16},
            'conversion': {'link_clicks': 8},
            'retention':  {'followers': 310},   # LIFETIME
            'advocacy':   {'shares': 8}
        },
        'website': {
            # Dental gets stronger local-search intent traffic
            'awareness':  {'sessions': 500, 'users': 395},
            'engagement': {'pages_per_session': 2.8, 'avg_session_duration': 130},
            # 2.8% form submit rate  (FirstPageSage: dental 2.5–3.5%)
            'conversion': {'form_submits': 14, 'form_submit_rate': 2.8},
            # 395 users × 22% ≈ 87
            'retention':  {'returning_users': 87, 'retention_rate': 22},
            # ~8.4% referral  (Healthgrades, ZocDoc, dental directories)
            'advocacy':   {'referrals': 42}
        },
        'email': {
            'awareness':  {'emails_sent': 1000, 'emails_delivered': 970},
            # 37% open rate, 2.4% click  (MailerLite Medical/Dental: 43.75%; Mailchimp blended ~35–38%)
            'engagement': {'email_opens': 359, 'email_clicks': 23},
            'response':   {'email_replies': 5},
            'retention':  {'unsubscribes': 2},
            'quality':    {'deliverability_score': 96}
        }
    },

    # ------------------------------------------------------------------ #
    #  Medical — specialist clinic / GP practice  (50–200 patients)
    #  Lower session volume, slightly lower form rate than dental
    #  ~470 FB followers, ~370 IG followers assumed
    # ------------------------------------------------------------------ #
    'medical': {
        'social_media': {
            # FB: 470 × 6% × 8 de-duped ≈ 240   IG: 370 × 10% × 8 de-duped ≈ 345
            'awareness':  {'reach': 585, 'impressions': 1100},
            # FB: 240 × 1.9% ≈ 5   IG: 345 × 3.7% ≈ 13
            'engagement': {'engagement_rate': 1.7, 'interactions': 18},
            'conversion': {'link_clicks': 13, 'cta_clicks': 5},
            'retention':  {'followers': 630, 'follower_growth': 12},
            # FB shares ≈ 3, IG sends ≈ 5
            'advocacy':   {'shares': 8, 'mentions': 2}
        },
        'social_media_facebook': {
            'awareness':  {'reach': 240},
            'engagement': {'interactions': 5},
            'conversion': {'link_clicks': 8},
            'retention':  {'followers': 375},   # LIFETIME
            'advocacy':   {'shares': 3}
        },
        'social_media_instagram': {
            'awareness':  {'reach': 345},
            'engagement': {'interactions': 13},
            'conversion': {'link_clicks': 5},
            'retention':  {'followers': 255},   # LIFETIME
            'advocacy':   {'shares': 5}
        },
        'website': {
            'awareness':  {'sessions': 350, 'users': 280},
            'engagement': {'pages_per_session': 2.5, 'avg_session_duration': 120},
            # 2.3% form submit rate  (FirstPageSage: medical specialty 1.8–2.4%)
            'conversion': {'form_submits': 8, 'form_submit_rate': 2.3},
            # 280 users × 19% ≈ 53  (medical repeat-visit rate 16–22%)
            'retention':  {'returning_users': 53, 'retention_rate': 19},
            # ~8% referral  (specialist directories, GP referral sites)
            'advocacy':   {'referrals': 28}
        },
        'email': {
            'awareness':  {'emails_sent': 800, 'emails_delivered': 776},
            # 35% open rate, 2.1% click  (Mailchimp healthcare blended)
            'engagement': {'email_opens': 272, 'email_clicks': 16},
            'response':   {'email_replies': 4},
            'retention':  {'unsubscribes': 2},
            'quality':    {'deliverability_score': 95}
        }
    },

    # ------------------------------------------------------------------ #
    #  Other — generic small local business
    #  Uses Mailchimp all-industry averages for email (21.3% open, 2.6% click)
    #  ~450 FB followers, ~350 IG followers assumed
    # ------------------------------------------------------------------ #
    'other': {
        'social_media': {
            # FB: 450 × 5% × 8 de-duped ≈ 200   IG: 350 × 9% × 8 de-duped ≈ 300
            'awareness':  {'reach': 500, 'impressions': 950},
            'engagement': {'engagement_rate': 1.5, 'interactions': 14},
            'conversion': {'link_clicks': 11, 'cta_clicks': 4},
            'retention':  {'followers': 450, 'follower_growth': 10},
            # FB shares ≈ 2, IG sends ≈ 4
            'advocacy':   {'shares': 6, 'mentions': 1}
        },
        'social_media_facebook': {
            'awareness':  {'reach': 200},
            'engagement': {'interactions': 4},
            'conversion': {'link_clicks': 7},
            'retention':  {'followers': 270},   # LIFETIME
            'advocacy':   {'shares': 2}
        },
        'social_media_instagram': {
            'awareness':  {'reach': 300},
            'engagement': {'interactions': 10},
            'conversion': {'link_clicks': 4},
            'retention':  {'followers': 180},   # LIFETIME
            'advocacy':   {'shares': 4}
        },
        'website': {
            'awareness':  {'sessions': 350, 'users': 275},
            'engagement': {'pages_per_session': 2.2, 'avg_session_duration': 100},
            'conversion': {'form_submits': 7, 'form_submit_rate': 2.0},
            # 275 users × 22% ≈ 60
            'retention':  {'returning_users': 60, 'retention_rate': 22},
            'advocacy':   {'referrals': 32}
        },
        'email': {
            'awareness':  {'emails_sent': 700, 'emails_delivered': 680},
            # 21.3% open, 2.6% click  (Mailchimp all-industry 2024)
            'engagement': {'email_opens': 145, 'email_clicks': 18},
            'response':   {'email_replies': 3},
            'retention':  {'unsubscribes': 2},
            'quality':    {'deliverability_score': 92}
        }
    },

    # ------------------------------------------------------------------ #
    #  Default — fallback for unknown/unset industry (mirrors 'other')
    # ------------------------------------------------------------------ #
    'default': {
        'social_media': {
            'awareness':  {'reach': 500, 'impressions': 950},
            'engagement': {'engagement_rate': 1.5, 'interactions': 14},
            'conversion': {'link_clicks': 11, 'cta_clicks': 4},
            'retention':  {'followers': 450, 'follower_growth': 10},
            'advocacy':   {'shares': 6, 'mentions': 1}
        },
        'social_media_facebook': {
            'awareness':  {'reach': 200},
            'engagement': {'interactions': 4},
            'conversion': {'link_clicks': 7},
            'retention':  {'followers': 270},
            'advocacy':   {'shares': 2}
        },
        'social_media_instagram': {
            'awareness':  {'reach': 300},
            'engagement': {'interactions': 10},
            'conversion': {'link_clicks': 4},
            'retention':  {'followers': 180},
            'advocacy':   {'shares': 4}
        },
        'website': {
            'awareness':  {'sessions': 350, 'users': 275},
            'engagement': {'pages_per_session': 2.2, 'avg_session_duration': 100},
            'conversion': {'form_submits': 7, 'form_submit_rate': 2.0},
            'retention':  {'returning_users': 60, 'retention_rate': 22},
            'advocacy':   {'referrals': 32}
        },
        'email': {
            'awareness':  {'emails_sent': 700, 'emails_delivered': 680},
            'engagement': {'email_opens': 145, 'email_clicks': 18},
            'response':   {'email_replies': 3},
            'retention':  {'unsubscribes': 2},
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
