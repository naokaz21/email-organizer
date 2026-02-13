#!/usr/bin/env python3
"""指定メールから処理済みラベルを削除"""

import os
from google.cloud import secretmanager
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

def get_secret(secret_name):
    """Secret Managerからシークレット取得"""
    client = secretmanager.SecretManagerServiceClient()
    project_id = "project-3255e657-b52f-4d63-ae7"
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')

def get_gmail_service():
    """Gmail APIサービスを取得"""
    client_id = get_secret("GMAIL_CLIENT_ID")
    client_secret = get_secret("GMAIL_CLIENT_SECRET")
    refresh_token = get_secret("GMAIL_REFRESH_TOKEN")

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=[
            "https://www.googleapis.com/auth/gmail.modify",
            "https://www.googleapis.com/auth/gmail.labels"
        ]
    )
    return build('gmail', 'v1', credentials=creds)

def get_label_id(gmail, label_name):
    """ラベル名からラベルIDを取得"""
    labels = gmail.users().labels().list(userId='me').execute()
    for label in labels.get('labels', []):
        if label['name'] == label_name:
            return label['id']
    return None

def main():
    print("=" * 60)
    print("Gmail ラベル削除ツール")
    print("=" * 60)
    print()

    # Gmail API初期化
    gmail = get_gmail_service()

    # ラベルID取得
    label_name = get_secret("PROCESSED_LABEL_NAME")
    label_id = get_label_id(gmail, label_name)

    if not label_id:
        print(f"❌ ラベル '{label_name}' が見つかりません")
        return

    print(f"📋 ラベル: {label_name} (ID: {label_id})")
    print()

    # 販売図面メールで処理済みラベル付きのものを検索
    query = f"subject:販売図面 label:{label_name} has:attachment"
    print(f"🔍 検索クエリ: {query}")

    results = gmail.users().messages().list(userId='me', q=query, maxResults=20).execute()
    messages = results.get('messages', [])

    print(f"📧 該当メール: {len(messages)}件")
    print()

    if not messages:
        print("❌ 該当メールが見つかりません")
        return

    # メール一覧表示
    for i, msg in enumerate(messages, 1):
        message = gmail.users().messages().get(userId='me', id=msg['id']).execute()
        snippet = message.get('snippet', '')

        print(f"{i}. {snippet[:100]}")

    print()

    # セシボン江戸川のメールを探す
    target_message_id = None
    for msg in messages:
        message = gmail.users().messages().get(userId='me', id=msg['id']).execute()
        snippet = message.get('snippet', '')

        if 'セシボン江戸川' in snippet or '1385983102' in snippet:
            target_message_id = msg['id']
            print(f"✅ 対象メール発見: {snippet[:100]}")
            break

    if not target_message_id:
        print("❌ セシボン江戸川のメールが見つかりません")
        return

    print()
    print(f"🗑️  ラベル '{label_name}' を削除中...")

    # ラベル削除
    gmail.users().messages().modify(
        userId='me',
        id=target_message_id,
        body={'removeLabelIds': [label_id]}
    ).execute()

    print("✅ ラベル削除完了")
    print()
    print("次のステップ: bash run_manual.sh を実行してください")

if __name__ == '__main__':
    main()
