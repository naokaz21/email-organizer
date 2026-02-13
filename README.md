# Email Organizer - Cloud Run

GmailからGoogle Driveへのメール添付ファイル自動整理システム（Cloud Run版）

## 概要

Google Apps Script (GAS) から Cloud Run への完全移行プロジェクト。販売図面・住宅地図メールの添付ファイルを自動的にGoogle Driveに整理します。

## 機能

- ✅ Gmail検索（販売図面・住宅地図メール）
- ✅ 添付ファイル自動保存
- ✅ 物件番号・駅名の自動抽出
- ✅ Driveフォルダ自動作成（`YYYYMMDD_駅名_物件番号`）
- ✅ 重複チェック
- ✅ 処理済みラベル自動付与
- ✅ Cloud Scheduler自動実行（毎時0分、直近2時間のメール検索）
- ✅ 手動実行（WebUI / ローカルスクリプト）
- ✅ 物件評価レポート自動生成（PDF解析、住所抽出、相場調査、Google Docs作成）

## 技術スタック

- **言語**: Python 3.11
- **フレームワーク**: Flask
- **実行環境**: Cloud Run
- **認証**: OAuth2.0
- **API**: Gmail API v1, Google Drive API v3, Google Docs API v1, Google Maps Geocoding API, Gemini API
- **スケジューラ**: Cloud Scheduler
- **シークレット管理**: Secret Manager

## デプロイ

```bash
bash deploy.sh
```

## 物件評価レポート自動生成

販売図面PDFを受信すると、自動的に以下の処理を実行します：

1. **PDF解析**: 販売図面からテキスト抽出
2. **住所抽出**: 正規表現 + Gemini AIで物件住所を特定
3. **位置情報取得**: Google Maps Geocoding APIで緯度経度を取得
4. **相場調査**: Gemini AIで周辺の類似物件の家賃相場を調査
5. **レポート作成**: Google Docsで評価レポートを作成し、物件フォルダに格納

### 必要なAPI設定

#### 1. Google Maps API Key

1. [Google Cloud Console](https://console.cloud.google.com/) → APIs & Services → Credentials
2. "Create Credentials" → "API Key"
3. API Keyを制限: Geocoding APIのみ許可
4. Secret Managerに `GOOGLE_MAPS_API_KEY` として保存

#### 2. Gemini API Key

1. [Google AI Studio](https://aistudio.google.com/apikey) にアクセス
2. "Create API Key" をクリック
3. Secret Managerに `GEMINI_API_KEY` として保存

#### 3. OAuth スコープ追加

Google Docs API使用には、refresh tokenの再取得が必要です：

1. OAuth Consent Screenでスコープ追加: `https://www.googleapis.com/auth/documents`
2. 既存の認証フローで再認証
3. 新しい refresh token を Secret Manager の `GMAIL_REFRESH_TOKEN` に保存

## 手動実行

### 方法1: WebUIから実行

ブラウザでCloud RunのURLにアクセス:
```
https://email-organizer-3kx6vtr4ha-uc.a.run.app
```

「🚀 メール整理を実行」ボタンをクリックして実行

### 方法2: ローカルスクリプトから実行

```bash
bash run_manual.sh
```

### 方法3: curlコマンドで実行

```bash
curl -X POST https://email-organizer-3kx6vtr4ha-uc.a.run.app/process \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json"
```

## プロジェクト構成

```
.
├── main.py              # メインアプリケーション
├── requirements.txt     # Python依存関係
├── Dockerfile          # Dockerイメージ定義
├── deploy.sh           # デプロイスクリプト
├── run_manual.sh       # 手動実行スクリプト
└── README.md
```

## 環境変数

Secret Managerで管理：
- `GMAIL_CLIENT_ID` - Gmail OAuth Client ID
- `GMAIL_CLIENT_SECRET` - Gmail OAuth Client Secret
- `GMAIL_REFRESH_TOKEN` - Gmail OAuth Refresh Token（Docs APIスコープ含む）
- `INVESTMENT_FOLDER_ID` - 投資物件用Google DriveフォルダID
- `PROCESSED_LABEL_NAME` - 処理済みメールに付与するラベル名
- `GOOGLE_MAPS_API_KEY` - Google Maps Geocoding API Key
- `GEMINI_API_KEY` - Gemini API Key

## ライセンス

MIT
