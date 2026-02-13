#!/usr/bin/env python3
"""フォルダ内のファイル一覧を確認"""

import os
from google.auth import default
from googleapiclient.discovery import build

FOLDER_ID = "1pIBpjP0aCY-R9Zs7NUgyHwnw2lSTy1ya"

# Application Default Credentialsを使用
creds, project = default(scopes=['https://www.googleapis.com/auth/drive.readonly'])
drive = build('drive', 'v3', credentials=creds)

# フォルダ内の全ファイルを取得
query = f"'{FOLDER_ID}' in parents and trashed=false"
results = drive.files().list(
    q=query,
    fields='files(id, name, mimeType)',
    pageSize=100
).execute()

files = results.get('files', [])

print(f"フォルダ内のファイル数: {len(files)}")
print()

if files:
    for f in files:
        print(f"名前: {f['name']}")
        print(f"  ID: {f['id']}")
        print(f"  種類: {f['mimeType']}")
        print()
else:
    print("フォルダが空です、またはアクセス権がありません")
