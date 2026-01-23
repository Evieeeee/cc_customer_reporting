"""
Flask Backend API for ContentClicks Dashboard
Handles customer management, data collection, and metrics retrieval
Now with Firestore support and 12-month historical tracking
"""

from flask import Flask, request, jsonify, send_file, render_template
from flask_cors import CORS
import os
import sys
from datetime import datetime
import threading

# Add project paths
sys.path.insert(0, '/mnt/project')

from models import (
    Database, Customer, CustomerCredential, HistoricalMetric,
    TopPerformer, get_benchmark
)
from data_collector import DataCollector
from trendline_analyzer import TrendlineAnalyzer

app = Flask(__name__, static_folder='static', template_folder='templates')
CORS(app)

# Initialize database
Database.init_db()


# ============================================================================
# CUSTOMER MANAGEMENT ENDPOINTS
# ============================================================================

@app.route('/api/customers', methods=['GET'])
def get_customers():
    """Get all customers"""
    try:
        customers = Customer.get_all()
        return jsonify({'success': True, 'customers': customers})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/customers', methods=['POST'])
def create_customer():
    """Create a new customer"""
    try:
        data = request.json
        name = data.get('name')
        industry = data.get('industry')
        
        if not name or not industry:
            return jsonify({'success': False, 'error': 'Name and industry required'}), 400
        
        customer_id = Customer.create(name, industry)
        
        # Store credentials if provided
        credentials = data.get('credentials', {})
        for platform, creds in credentials.items():
            for key, value in creds.items():
                if value:  # Only store non-empty credentials
                    CustomerCredential.set(customer_id, platform, key, value)
        
        customer = Customer.get_by_id(customer_id)
        return jsonify({'success': True, 'customer': customer})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/customers/<customer_id>', methods=['GET'])
def get_customer(customer_id):
    """Get a specific customer with their credentials"""
    try:
        customer = Customer.get_by_id(customer_id)
        if not customer:
            return jsonify({'success': False, 'error': 'Customer not found'}), 404
        
        credentials = CustomerCredential.get_all_for_customer(customer_id)
        
        return jsonify({
            'success': True,
            'customer': customer,
            'credentials': credentials
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/customers/<customer_id>', methods=['PUT'])
def update_customer(customer_id):
    """Update a customer"""
    try:
        data = request.json
        name = data.get('name')
        industry = data.get('industry')
        
        Customer.update(customer_id, name, industry)
        
        # Update credentials if provided
        credentials = data.get('credentials', {})
        for platform, creds in credentials.items():
            for key, value in creds.items():
                if value:
                    CustomerCredential.set(customer_id, platform, key, value)
        
        customer = Customer.get_by_id(customer_id)
        return jsonify({'success': True, 'customer': customer})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/customers/<customer_id>', methods=['DELETE'])
def delete_customer(customer_id):
    """Delete a customer"""
    try:
        Customer.delete(customer_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# METRICS ENDPOINTS
# ============================================================================

@app.route('/api/customers/<customer_id>/metrics', methods=['GET'])
def get_customer_metrics(customer_id):
    """Get latest metrics for a customer"""
    try:
        metrics = HistoricalMetric.get_latest_for_customer(customer_id)
        
        return jsonify({
            'success': True,
            'metrics': metrics
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/customers/<customer_id>/metrics/history', methods=['GET'])
def get_metric_history(customer_id):
    """Get historical data for a specific KPI"""
    try:
        medium = request.args.get('medium')
        journey_stage = request.args.get('journey_stage')
        kpi_name = request.args.get('kpi_name')
        months = int(request.args.get('months', 12))  # Default 12 months
        
        if not all([medium, journey_stage, kpi_name]):
            return jsonify({
                'success': False,
                'error': 'medium, journey_stage, and kpi_name required'
            }), 400
        
        history = HistoricalMetric.get_history(
            customer_id, medium, journey_stage, kpi_name, months
        )

        # Add AI-powered trendline analysis
        trendline_analysis = TrendlineAnalyzer.analyze_metric_history(history)

        return jsonify({
            'success': True,
            'history': history,
            'trendline': trendline_analysis
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/customers/<customer_id>/top-performers', methods=['GET'])
def get_top_performers(customer_id):
    """Get top performers for a customer"""
    try:
        medium = request.args.get('medium')
        limit = int(request.args.get('limit', 10))
        
        if not medium:
            return jsonify({'success': False, 'error': 'medium required'}), 400
        
        performers = TopPerformer.get_latest_for_customer(customer_id, medium, limit)
        
        return jsonify({
            'success': True,
            'top_performers': performers
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# PAGE DISCOVERY ENDPOINT
# ============================================================================

@app.route('/api/discover-pages', methods=['POST'])
def discover_pages():
    """Discover Facebook pages and Instagram accounts from a System User Token"""
    try:
        data = request.json
        system_token = data.get('system_user_token')
        
        if not system_token:
            return jsonify({'success': False, 'error': 'System token required'}), 400
        
        # Import the function
        from social_media_analytics import get_all_pages_and_instagram_accounts
        
        # Get all accessible pages
        accounts = get_all_pages_and_instagram_accounts(system_token)
        
        # Format for frontend
        pages = []
        for account in accounts:
            page_info = {
                'page_id': account['page_id'],
                'page_name': account['page_name'],
                'instagram_id': account.get('instagram_id'),
                'has_instagram': bool(account.get('instagram_id')),
                'fan_count': account.get('fan_count', 0),
                'followers_count': account.get('followers_count', 0)
            }
            pages.append(page_info)
        
        return jsonify({
            'success': True,
            'pages': pages,
            'total': len(pages)
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# DATA COLLECTION ENDPOINTS
# ============================================================================

# Store collection status in memory
collection_status = {}

@app.route('/api/customers/<customer_id>/collect', methods=['POST'])
def collect_customer_data(customer_id):
    """Trigger data collection for a customer"""
    try:
        data = request.json or {}
        days = data.get('days', 30)
        collect_history = data.get('collect_history', False)  # New: collect 12 months
        
        # Initialize status
        collection_status[customer_id] = {
            'status': 'starting',
            'progress': 0,
            'message': 'Initializing data collection...',
            'completed': False,
            'error': None,
            'started_at': datetime.now().isoformat(),
            'sources': {
                'social_media': {'status': 'pending', 'message': 'Waiting...'},
                'email': {'status': 'pending', 'message': 'Waiting...'},
                'website': {'status': 'pending', 'message': 'Waiting...'}
            }
        }
        
        # Status update callback
        def update_status(source, message, progress):
            """Update status from data collector threads"""
            print(f"[STATUS UPDATE] Source: {source}, Message: {message}, Progress: {progress}")
            
            if customer_id in collection_status:
                collection_status[customer_id]['message'] = message
                collection_status[customer_id]['progress'] = progress
                
                # Update source-specific status
                source_key = source.lower().replace(' ', '_')
                print(f"[STATUS UPDATE] Source key: {source_key}")
                
                # Handle historical collection
                if source_key == 'historical_collection':
                    # Historical collection is a special case
                    collection_status[customer_id]['message'] = message
                    return
                
                if source_key in collection_status[customer_id]['sources']:
                    if '✅' in message:
                        collection_status[customer_id]['sources'][source_key]['status'] = 'completed'
                        print(f"[STATUS UPDATE] Marked {source_key} as COMPLETED")
                    elif '⚠️' in message or 'failed' in message.lower():
                        collection_status[customer_id]['sources'][source_key]['status'] = 'failed'
                        print(f"[STATUS UPDATE] Marked {source_key} as FAILED")
                    else:
                        collection_status[customer_id]['sources'][source_key]['status'] = 'collecting'
                        print(f"[STATUS UPDATE] Marked {source_key} as COLLECTING")
                    collection_status[customer_id]['sources'][source_key]['message'] = message
                else:
                    print(f"[STATUS UPDATE] WARNING: source_key '{source_key}' not found in sources dict!")
                    print(f"[STATUS UPDATE] Available sources: {list(collection_status[customer_id]['sources'].keys())}")
            else:
                print(f"[STATUS UPDATE] WARNING: customer_id {customer_id} not in collection_status!")
        
        # Run collection in background thread
        def run_collection():
            try:
                collection_status[customer_id]['status'] = 'collecting'
                collection_status[customer_id]['progress'] = 10
                
                if collect_history:
                    collection_status[customer_id]['message'] = '📅 Starting 12-month historical data collection...'
                else:
                    collection_status[customer_id]['message'] = '🚀 Starting parallel data collection...'
                
                collector = DataCollector(customer_id)
                
                print(f"[INFO] Starting data collection for customer {customer_id}")
                print(f"[INFO] Historical mode: {collect_history}")
                
                # Pass status callback to collector
                collector.collect_all_data(days=days, status_callback=update_status, collect_history=collect_history)
                
                # If we get here, collection completed
                collection_status[customer_id]['status'] = 'completed'
                collection_status[customer_id]['progress'] = 100
                collection_status[customer_id]['message'] = '✅ All data collection complete! Refresh to see results.'
                collection_status[customer_id]['completed'] = True
                collection_status[customer_id]['completed_at'] = datetime.now().isoformat()
                
                print(f"[OK] Data collection completed for customer {customer_id}")
                print(f"[DEBUG] Final sources status: {collection_status[customer_id]['sources']}")
                
            except Exception as e:
                collection_status[customer_id]['status'] = 'error'
                collection_status[customer_id]['error'] = str(e)
                collection_status[customer_id]['message'] = f'❌ Error: {str(e)}'
                print(f"[ERROR] Collection failed for customer {customer_id}: {e}")
                import traceback
                traceback.print_exc()
        
        thread = threading.Thread(target=run_collection)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'message': 'Data collection started',
            'status': collection_status[customer_id]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/customers/<customer_id>/collect/status', methods=['GET'])
def get_collection_status(customer_id):
    """Get the current status of data collection"""
    try:
        if customer_id in collection_status:
            status = collection_status[customer_id]
            
            # Calculate elapsed time
            if 'started_at' in status:
                from datetime import datetime
                started = datetime.fromisoformat(status['started_at'])
                elapsed = (datetime.now() - started).total_seconds()
                status['elapsed_seconds'] = int(elapsed)
            
            # Check if any source has completed (partial data available)
            any_completed = False
            if 'sources' in status:
                for source_data in status['sources'].values():
                    if source_data.get('status') == 'completed':
                        any_completed = True
                        break
            status['partial_data_available'] = any_completed
            
            return jsonify({
                'success': True,
                'status': status
            })
        else:
            return jsonify({
                'success': True,
                'status': {
                    'status': 'idle',
                    'progress': 0,
                    'message': 'No collection in progress',
                    'completed': False
                }
            })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# FRONTEND ROUTE
# ============================================================================

@app.route('/')
def index():
    """Serve the dashboard"""
    return render_template('dashboard.html')


# ============================================================================
# PDF EXPORT ENDPOINT
# ============================================================================

@app.route('/api/customers/<customer_id>/export/pdf', methods=['POST'])
def export_pdf(customer_id):
    """Export dashboard to PDF"""
    try:
        from weasyprint import HTML, CSS
        import base64
        
        # Get customer data
        customer = Customer.get_by_id(customer_id)
        if not customer:
            return jsonify({'success': False, 'error': 'Customer not found'}), 404
        
        metrics = HistoricalMetric.get_latest_for_customer(customer_id)
        
        # Get chart data from request
        data = request.json or {}
        charts = data.get('charts', {})
        
        # Render HTML with data
        html_content = render_template(
            'pdf_export.html',
            customer=customer,
            metrics=metrics,
            charts=charts,
            generated_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        
        # Generate PDF
        pdf_filename = f"contentclicks_{customer['name'].replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.pdf"
        pdf_path = os.path.join('/tmp', pdf_filename)
        
        HTML(string=html_content).write_pdf(pdf_path)
        
        return send_file(
            pdf_path,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=pdf_filename
        )
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=True)
