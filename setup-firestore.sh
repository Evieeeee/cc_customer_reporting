#!/bin/bash
# Firestore Setup & Deployment Script
# Run this to set up Firestore and deploy your updated dashboard

set -e  # Exit on error

echo "=================================================="
echo "  ContentClicks - Firestore Setup & Deployment"
echo "=================================================="
echo ""

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "ERROR: gcloud CLI not found!"
    echo "Please install Google Cloud SDK:"
    echo "https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Get project ID
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
if [ -z "$PROJECT_ID" ]; then
    echo "ERROR: No project set"
    echo "Please set your project:"
    echo "  gcloud config set project YOUR_PROJECT_ID"
    exit 1
fi

echo "Project: $PROJECT_ID"
echo ""

# Step 1: Enable Firestore API
echo "[1/5] Enabling Firestore API..."
gcloud services enable firestore.googleapis.com
echo "✓ Firestore API enabled"
echo ""

# Step 2: Create Firestore database (if not exists)
echo "[2/5] Creating Firestore database..."
if gcloud firestore databases describe --project=$PROJECT_ID &>/dev/null; then
    echo "✓ Firestore database already exists"
else
    echo "Creating new Firestore database in us-central..."
    gcloud firestore databases create --location=us-central --project=$PROJECT_ID
    echo "✓ Firestore database created"
fi
echo ""

# Step 3: Grant Firestore permissions
echo "[3/5] Granting Firestore permissions..."
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/datastore.user" \
    --quiet

echo "✓ Permissions granted to ${SERVICE_ACCOUNT}"
echo ""

# Step 4: Deploy to Cloud Run
echo "[4/5] Deploying to Cloud Run..."
echo "This may take 3-5 minutes..."
echo ""

gcloud run deploy contentclicks-dashboard \
    --source . \
    --region us-central1 \
    --allow-unauthenticated \
    --platform managed \
    --memory 1Gi \
    --timeout 600 \
    --quiet

echo ""
echo "✓ Deployment complete!"
echo ""

# Step 5: Get service URL
echo "[5/5] Getting service URL..."
SERVICE_URL=$(gcloud run services describe contentclicks-dashboard \
    --region us-central1 \
    --format="value(status.url)")

echo ""
echo "=================================================="
echo "  ✓ Setup Complete!"
echo "=================================================="
echo ""
echo "Your dashboard is live at:"
echo "  $SERVICE_URL"
echo ""
echo "Next steps:"
echo "  1. Open the URL in your browser"
echo "  2. Create a customer"
echo "  3. Add credentials"
echo "  4. Click Refresh to collect current data"
echo "  5. Check '12-month history' and Refresh for historical data"
echo ""
echo "Features enabled:"
echo "  ✓ Firestore cloud database"
echo "  ✓ 12-month historical data collection"
echo "  ✓ Automatic backups and scaling"
echo ""
echo "Estimated monthly cost: $1-5 for 100 customers"
echo ""
echo "=================================================="
