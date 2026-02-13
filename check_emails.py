#!/usr/bin/env python3
"""最近のメールを確認"""

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

gmail = get_gmail_service()
label_name = get_secret("PROCESSED_LABEL_NAME")

# 販売図面メール（過去2時間、未処理）
query1 = f"subject:販売図面 newer_than:2h has:attachment -label:{label_name}"
results1 = gmail.users().messages().list(userId='me', q=query1, maxResults=5).execute()
messages1 = results1.get('messages', [])

print(f"📧 販売図面（過去2時間、未処理）: {len(messages1)}件")
for msg in messages1:
    message = gmail.users().messages().get(userId='me', id=msg['id']).execute()
    print(f"  - {message.get('snippet', '')[:80]}")

print()

# 販売図面メール（過去1日、全て）
query2 = "subject:販売図面 newer_than:1d has:attachment"
results2 = gmail.users().messages().list(userId='me', q=query2, maxResults=5).execute()
messages2 = results2.get('messages', [])

print(f"📧 販売図面（過去1日、全て）: {len(messages2)}件")
for msg in messages2:
    message = gmail.users().messages().get(userId='me', id=msg['id']).execute()
    print(f"  - {message.get('snippet', '')[:80]}")
