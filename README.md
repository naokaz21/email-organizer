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
- ✅ Cloud Scheduler自動実行（毎朝8時JST）

## 技術スタック

- **言語**: Python 3.11
- **フレームワーク**: Flask
- **実行環境**: Cloud Run
- **認証**: OAuth2.0
- **API**: Gmail API v1, Google Drive API v3
- **スケジューラ**: Cloud Scheduler
- **シークレット管理**: Secret Manager

## デプロイ

```bash
bash deploy.sh
```

## プロジェクト構成

```
.
├── main.py              # メインアプリケーション
├── requirements.txt     # Python依存関係
├── Dockerfile          # Dockerイメージ定義
├── deploy.sh           # デプロイスクリプト
└── README.md
```

## 環境変数

Secret Managerで管理：
- `GMAIL_CLIENT_ID`
- `GMAIL_CLIENT_SECRET`
- `GMAIL_REFRESH_TOKEN`
- `INVESTMENT_FOLDER_ID`
- `PROCESSED_LABEL_NAME`

## ライセンス

MIT
