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

    # prompt='consent' で常に新しいrefresh tokenを取得
    creds = flow.run_local_server(
        port=8080,
        authorization_prompt_message='ブラウザで認証してください...',
        success_message='認証成功！このタブを閉じてターミナルに戻ってください。',
        access_type='offline',
        prompt='consent'
    )

    print()
    print("=" * 60)
    print("認証成功！")
    print("=" * 60)
    print()
    print("Refresh Token:")
    print(creds.refresh_token)
    print()

    # Secret Manager 自動更新
    answer = input("Secret Manager の GMAIL_REFRESH_TOKEN を自動更新しますか？ (Y/n): ").strip()
    if answer.lower() != 'n':
        import subprocess
        result = subprocess.run(
            ['gcloud', 'secrets', 'versions', 'add', 'GMAIL_REFRESH_TOKEN',
             '--data-file=-', '--project=project-3255e657-b52f-4d63-ae7'],
            input=creds.refresh_token.encode(),
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print("Secret Manager 更新完了！")
            print("Cloud Run は次回リクエスト時に新しいトークンを自動的に使用します。")
        else:
            print(f"更新失敗: {result.stderr}")
            print()
            print("手動コマンド:")
            print(f"echo -n '{creds.refresh_token}' | gcloud secrets versions add GMAIL_REFRESH_TOKEN --data-file=- --project=project-3255e657-b52f-4d63-ae7")
    else:
        print("手動コマンド:")
        print(f"echo -n '{creds.refresh_token}' | gcloud secrets versions add GMAIL_REFRESH_TOKEN --data-file=- --project=project-3255e657-b52f-4d63-ae7")
    print()

if __name__ == '__main__':
    main()
