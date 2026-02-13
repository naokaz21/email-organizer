#!/usr/bin/env python3
"""物件47968のフォルダからJPGファイルを削除"""

from google.cloud import secretmanager
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "project-3255e657-b52f-4d63-ae7"
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')

def get_drive_service():
    client_id = get_secret("GMAIL_CLIENT_ID")
    client_secret = get_secret("GMAIL_CLIENT_SECRET")
    refresh_token = get_secret("GMAIL_REFRESH_TOKEN")

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build('drive', 'v3', credentials=creds)

drive = get_drive_service()
investment_folder_id = get_secret("INVESTMENT_FOLDER_ID")

# 20260213_子安_47968 フォルダを検索
query = f"name contains '47968' and '{investment_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
folders = drive.files().list(q=query, fields='files(id, name)').execute()
folder_files = folders.get('files', [])

if not folder_files:
    print("❌ フォルダが見つかりません")
    exit(1)

folder_id = folder_files[0]['id']
folder_name = folder_files[0]['name']
print(f"✅ フォルダ発見: {folder_name} (ID: {folder_id})")

# フォルダ内のJPGファイルを検索
query = f"'{folder_id}' in parents and name contains '.jpg' and trashed = false"
files = drive.files().list(q=query, fields='files(id, name)').execute()
jpg_files = files.get('files', [])

if not jpg_files:
    print("❌ JPGファイルが見つかりません")
    exit(1)

# JPGファイルを削除
for file in jpg_files:
    print(f"🗑️  削除中: {file['name']}")
    drive.files().delete(fileId=file['id']).execute()
    print(f"✅ 削除完了: {file['name']}")
