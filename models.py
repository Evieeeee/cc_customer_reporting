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
    
    @staticmethod
    def get_history(customer_id: str, medium: str, journey_stage: str, 
                    kpi_name: str, months: int = 12) -> List[Dict]:
        """Get historical data for a specific KPI (last N months)"""
        
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
            except Exception as e:
                print(f"[WARNING] Could not fetch data for {year}-{month:02d}: {e}")
                continue
        
        # Reverse to get chronological order (oldest to newest)
        results.reverse()
        
        return results
    
    @staticmethod
    def get_latest_for_customer(customer_id: str) -> Dict:
        """Get the latest metrics for all KPIs for a customer"""
        
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        
        metrics = {}
        
        # Define known mediums and stages
        mediums_and_stages = {
            'social_media': ['awareness', 'engagement', 'conversion', 'retention', 'advocacy'],
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

                    # If we have KPIs, use the first one for the old structure
                    # but this should really return all KPIs
                    if kpi_list:
                        metrics[medium][journey_stage] = kpi_list[0]  # For backwards compatibility
                        # TODO: Should return all KPIs, not just first one
                        
                except Exception as e:
                    print(f"[DEBUG] Could not fetch latest for {medium}/{journey_stage}: {e}")
        
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


# Industry benchmarks for startup companies
INDUSTRY_BENCHMARKS = {
    'healthcare': {
        'social_media': {
            'awareness': {'reach': 1000, 'impressions': 2000},
            'engagement': {'engagement_rate': 2.5, 'interactions': 50},
            'conversion': {'link_clicks': 20, 'cta_clicks': 10},
            'retention': {'follower_growth': 5, 'repeat_engagement': 15},
            'advocacy': {'shares': 5, 'mentions': 3}
        },
        'website': {
            'awareness': {'sessions': 500, 'users': 400},
            'engagement': {'pages_per_session': 2.5, 'avg_session_duration': 120},
            'conversion': {'conversions': 10, 'conversion_rate': 2.0},
            'retention': {'returning_users': 30, 'retention_rate': 20},
            'advocacy': {'referrals': 5, 'social_shares': 10}
        },
        'email': {
            'awareness': {'emails_sent': 1000, 'emails_delivered': 950},
            'engagement': {'email_opens': 200, 'email_clicks': 25},
            'response': {'email_replies': 50},
            'retention': {'unsubscribes': 5},
            'quality': {'deliverability_score': 95}
        }
    },
    'dental': {
        'social_media': {
            'awareness': {'reach': 800, 'impressions': 1500},
            'engagement': {'engagement_rate': 3.0, 'interactions': 60},
            'conversion': {'link_clicks': 25, 'cta_clicks': 12},
            'retention': {'follower_growth': 6, 'repeat_engagement': 18},
            'advocacy': {'shares': 6, 'mentions': 4}
        },
        'website': {
            'awareness': {'sessions': 600, 'users': 450},
            'engagement': {'pages_per_session': 3.0, 'avg_session_duration': 150},
            'conversion': {'conversions': 15, 'conversion_rate': 2.5},
            'retention': {'returning_users': 35, 'retention_rate': 25},
            'advocacy': {'referrals': 8, 'social_shares': 12}
        },
        'email': {
            'awareness': {'emails_sent': 800, 'emails_delivered': 768},
            'engagement': {'email_opens': 176, 'email_clicks': 24},
            'response': {'email_replies': 48},
            'retention': {'unsubscribes': 3},
            'quality': {'deliverability_score': 96}
        }
    },
    'medical': {
        'social_media': {
            'awareness': {'reach': 900, 'impressions': 1800},
            'engagement': {'engagement_rate': 2.2, 'interactions': 45},
            'conversion': {'link_clicks': 18, 'cta_clicks': 9},
            'retention': {'follower_growth': 4, 'repeat_engagement': 12},
            'advocacy': {'shares': 4, 'mentions': 2}
        },
        'website': {
            'awareness': {'sessions': 550, 'users': 420},
            'engagement': {'pages_per_session': 2.8, 'avg_session_duration': 140},
            'conversion': {'conversions': 12, 'conversion_rate': 2.2},
            'retention': {'returning_users': 32, 'retention_rate': 22},
            'advocacy': {'referrals': 6, 'social_shares': 8}
        },
        'email': {
            'awareness': {'emails_sent': 900, 'emails_delivered': 855},
            'engagement': {'email_opens': 189, 'email_clicks': 25},
            'response': {'email_replies': 50},
            'retention': {'unsubscribes': 5},
            'quality': {'deliverability_score': 95}
        }
    },
    'default': {
        'social_media': {
            'awareness': {'reach': 1000, 'impressions': 2000},
            'engagement': {'engagement_rate': 2.5, 'interactions': 50},
            'conversion': {'link_clicks': 20, 'cta_clicks': 10},
            'retention': {'follower_growth': 5, 'repeat_engagement': 15},
            'advocacy': {'shares': 5, 'mentions': 3}
        },
        'website': {
            'awareness': {'sessions': 500, 'users': 400},
            'engagement': {'pages_per_session': 2.5, 'avg_session_duration': 120},
            'conversion': {'conversions': 10, 'conversion_rate': 2.0},
            'retention': {'returning_users': 30, 'retention_rate': 20},
            'advocacy': {'referrals': 5, 'social_shares': 10}
        },
        'email': {
            'awareness': {'emails_sent': 1000, 'emails_delivered': 950},
            'engagement': {'email_opens': 200, 'email_clicks': 25},
            'response': {'email_replies': 50},
            'retention': {'unsubscribes': 5},
            'quality': {'deliverability_score': 95}
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
