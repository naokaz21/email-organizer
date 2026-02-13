#!/usr/bin/env python3
"""OAuth Refresh Token を取得するスクリプト

使い方:
1. OAuth Consent Screen で Docs API スコープを追加
2. このスクリプトを実行
3. ブラウザで認証
4. 表示された refresh_token を Secret Manager に保存
"""

import os
from google_auth_oauthlib.flow import InstalledAppFlow
from google.cloud import secretmanager

# 必要なスコープ
SCOPES = [
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.labels',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/documents'  # 新規追加
]

def get_client_config():
    """Secret Manager から OAuth クライアント情報を取得"""
    client = secretmanager.SecretManagerServiceClient()
    project_id = "project-3255e657-b52f-4d63-ae7"

    def get_secret(name):
        secret_name = f"projects/{project_id}/secrets/{name}/versions/latest"
        response = client.access_secret_version(request={"name": secret_name})
        return response.payload.data.decode('UTF-8')

    client_id = get_secret("GMAIL_CLIENT_ID")
    client_secret = get_secret("GMAIL_CLIENT_SECRET")

    return {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost:8080/"]
        }
    }

def main():
    print("=" * 60)
    print("Google OAuth Refresh Token 取得ツール")
    print("=" * 60)
    print()

    # OAuth クライアント設定取得
    print("📋 Secret Manager から OAuth 設定を取得中...")
    client_config = get_client_config()
    print("✅ OAuth 設定取得完了")
    print()

    # OAuth フロー開始
    print("🔐 認証フローを開始します...")
    print("ブラウザが開きます。Googleアカウントでログインしてください。")
    print()

    flow = InstalledAppFlow.from_client_config(
        client_config,
        scopes=SCOPES,
        redirect_uri='http://localhost:8080/'
    )

    # 認証実行（ブラウザが開く）
    creds = flow.run_local_server(
        port=8080,
        authorization_prompt_message='ブラウザで認証してください...',
        success_message='認証成功！このタブを閉じてターミナルに戻ってください。'
    )

    print()
    print("=" * 60)
    print("✅ 認証成功！")
    print("=" * 60)
    print()
    print("📝 Refresh Token:")
    print(creds.refresh_token)
    print()
    print("=" * 60)
    print("次のステップ:")
    print("=" * 60)
    print("1. 上記の refresh_token をコピー")
    print("2. Secret Manager の GMAIL_REFRESH_TOKEN を更新")
    print()
    print("コマンド:")
    print(f"echo -n '{creds.refresh_token}' | gcloud secrets versions add GMAIL_REFRESH_TOKEN --data-file=-")
    print()

if __name__ == '__main__':
    main()
