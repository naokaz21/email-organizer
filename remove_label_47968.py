#!/usr/bin/env python3
"""物件47968のメールからラベル削除"""

from google.cloud import secretmanager
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "project-3255e657-b52f-4d63-ae7"
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')

def get_gmail_service():
    client_id = get_secret("GMAIL_CLIENT_ID")
    client_secret = get_secret("GMAIL_CLIENT_SECRET")
    refresh_token = get_secret("GMAIL_REFRESH_TOKEN")

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=["https://www.googleapis.com/auth/gmail.modify", "https://www.googleapis.com/auth/gmail.labels"]
    )
    return build('gmail', 'v1', credentials=creds)

gmail = get_gmail_service()
label_name = get_secret("PROCESSED_LABEL_NAME")

# ラベルID取得
labels = gmail.users().labels().list(userId='me').execute()
label_id = None
for label in labels.get('labels', []):
    if label['name'] == label_name:
        label_id = label['id']
        break

if not label_id:
    print(f"ラベル '{label_name}' が見つかりません")
    exit(1)

# 物件47968のメールを検索
query = f"subject:販売図面 label:{label_name} has:attachment"
results = gmail.users().messages().list(userId='me', q=query, maxResults=20).execute()
messages = results.get('messages', [])

target_message_id = None
for msg in messages:
    message = gmail.users().messages().get(userId='me', id=msg['id']).execute()
    snippet = message.get('snippet', '')

    if '47968' in snippet or '子安' in snippet:
        target_message_id = msg['id']
        print(f"✅ 対象メール発見: {snippet[:100]}")
        break

if not target_message_id:
    print("❌ 物件47968のメールが見つかりません")
    exit(1)

# ラベル削除
gmail.users().messages().modify(
    userId='me',
    id=target_message_id,
    body={'removeLabelIds': [label_id]}
).execute()

print("✅ ラベル削除完了")
