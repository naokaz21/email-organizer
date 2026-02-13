#!/bin/bash
set -e

PROJECT_ID="project-3255e657-b52f-4d63-ae7"
REGION="us-central1"
SERVICE_NAME="email-organizer"

echo "🚀 Email Organizer 再デプロイ開始..."

# Docker ビルド & プッシュ
echo "📦 Dockerイメージをビルド中..."
gcloud builds submit --tag ${REGION}-docker.pkg.dev/${PROJECT_ID}/email-organizer/${SERVICE_NAME}:latest

# Cloud Run デプロイ
echo "☁️  Cloud Runにデプロイ中..."
gcloud run deploy ${SERVICE_NAME} \
  --image ${REGION}-docker.pkg.dev/${PROJECT_ID}/email-organizer/${SERVICE_NAME}:latest \
  --platform managed \
  --region ${REGION} \
  --set-env-vars GCP_PROJECT_ID=${PROJECT_ID} \
  --set-secrets GMAIL_CLIENT_ID=GMAIL_CLIENT_ID:latest,GMAIL_CLIENT_SECRET=GMAIL_CLIENT_SECRET:latest,GMAIL_REFRESH_TOKEN=GMAIL_REFRESH_TOKEN:latest,INVESTMENT_FOLDER_ID=INVESTMENT_FOLDER_ID:latest,PROCESSED_LABEL_NAME=PROCESSED_LABEL_NAME:latest,GOOGLE_MAPS_API_KEY=GOOGLE_MAPS_API_KEY:latest,GEMINI_API_KEY=GEMINI_API_KEY:latest \
  --service-account email-organizer-sa@${PROJECT_ID}.iam.gserviceaccount.com

echo "✅ デプロイ完了！"
