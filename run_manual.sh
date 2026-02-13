#!/bin/bash
set -e

PROJECT_ID="project-3255e657-b52f-4d63-ae7"
REGION="us-central1"
SERVICE_NAME="email-organizer"

echo "📧 Email Organizer - 手動実行"
echo "================================"
echo ""

# Cloud Run サービスURLを取得
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} \
  --region ${REGION} \
  --format='value(status.url)')

echo "🚀 メール整理を実行中..."
echo "URL: ${SERVICE_URL}/process"
echo ""

# Identity Tokenを取得してリクエスト送信
RESPONSE=$(curl -s -X POST "${SERVICE_URL}/process" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json")

echo "📊 実行結果:"
echo "${RESPONSE}" | python3 -m json.tool

echo ""
echo "✅ 完了"
