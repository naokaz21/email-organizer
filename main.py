"""
Email Organizer - Cloud Run Service
Gmailから販売図面・住宅地図メールを取得し、Google Driveに自動整理
"""

import os
import re
from flask import Flask, request, jsonify
from google.cloud import secretmanager
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta

app = Flask(__name__)

# Secret Manager クライアント
secret_client = secretmanager.SecretManagerServiceClient()
PROJECT_ID = os.environ.get('GCP_PROJECT_ID')

def get_secret(secret_name):
    """Secret Managerからシークレットを取得"""
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = secret_client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_gmail_service():
    """Gmail APIサービスを取得"""
    client_id = get_secret("GMAIL_CLIENT_ID")
    client_secret = get_secret("GMAIL_CLIENT_SECRET")
    refresh_token = get_secret("GMAIL_REFRESH_TOKEN")

    # 修正：正しいスコープを指定
    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=[
            "https://www.googleapis.com/auth/gmail.modify",
            "https://www.googleapis.com/auth/gmail.labels",
            "https://www.googleapis.com/auth/drive"  # drive.file → drive に変更
        ]
    )

    return build('gmail', 'v1', credentials=creds)

def get_drive_service():
    """Drive APIサービスを取得"""
    client_id = get_secret("GMAIL_CLIENT_ID")
    client_secret = get_secret("GMAIL_CLIENT_SECRET")
    refresh_token = get_secret("GMAIL_REFRESH_TOKEN")

    # 修正：正しいスコープを指定
    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=[
            "https://www.googleapis.com/auth/gmail.modify",
            "https://www.googleapis.com/auth/gmail.labels",
            "https://www.googleapis.com/auth/drive"  # drive.file → drive に変更
        ]
    )

    return build('drive', 'v3', credentials=creds)

def get_or_create_label(gmail_service, label_name):
    """Gmailラベルを取得または作成"""
    labels = gmail_service.users().labels().list(userId='me').execute()
    for label in labels.get('labels', []):
        if label['name'] == label_name:
            return label['id']

    # ラベル作成
    label = gmail_service.users().labels().create(
        userId='me',
        body={'name': label_name}
    ).execute()
    return label['id']

def extract_property_info_from_hanbaizumen(message_body, attachments):
    """販売図面メールから物件情報を抽出"""
    property_number = None
    station = None

    # 添付ファイル名から物件番号を抽出（優先）
    for att in attachments:
        match = re.search(r'Hanbaizumen_(\d+)', att.get('filename', ''))
        if match:
            property_number = match.group(1)
            break

    # 本文から物件番号と駅名を抽出
    match = re.search(r'物件番号[:：]\s*(\d+)\s*駅[:：]\s*([^\s\r\n]+)', message_body)
    if match:
        if not property_number:
            property_number = match.group(1)
        station = match.group(2)

    # 本文のURLからも物件番号を取得（バックアップ）
    if not property_number:
        url_match = re.search(r'hid=(\d+)', message_body)
        if url_match:
            property_number = url_match.group(1)

    # 駅名が取れなかった場合の追加パターン
    if not station:
        station_match = re.search(r'駅[:：]\s*([^\s\r\n,、]+)', message_body)
        if station_match:
            station = station_match.group(1)

    if not station:
        station = '不明'

    return property_number, station

def extract_property_info_from_chizu(message_body):
    """住宅地図・路線価図メールから物件情報を抽出"""
    property_number = None
    station = None

    # 本文から物件番号と駅名を抽出
    match = re.search(r'物件番号[:：]\s*(\d+)\s*駅[:：]\s*([^\s\r\n]+)', message_body)
    if match:
        property_number = match.group(1)
        station = match.group(2)

    # URLから物件番号を取得（バックアップ）
    if not property_number:
        url_match = re.search(r'hid=(\d+)', message_body)
        if url_match:
            property_number = url_match.group(1)

    # 駅名が取れなかった場合
    if not station:
        station_match = re.search(r'駅[:：]\s*([^\s\r\n,、]+)', message_body)
        if station_match:
            station = station_match.group(1)

    if not station:
        station = '不明'

    return property_number, station

def get_or_create_folder(drive_service, parent_folder_id, folder_name, property_number):
    """Driveフォルダを取得または作成（物件番号で部分一致検索）"""
    # まず完全一致で検索
    query = f"name = '{folder_name}' and '{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = drive_service.files().list(q=query, fields='files(id, name)').execute()
    files = results.get('files', [])

    if files:
        return files[0]['id']

    # 物件番号で部分一致検索
    partial_query = f"name contains '_{property_number}' and '{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    partial_results = drive_service.files().list(q=partial_query, fields='files(id, name)', pageSize=10).execute()
    partial_files = partial_results.get('files', [])

    if partial_files:
        for folder in partial_files:
            if folder['name'].endswith(f'_{property_number}'):
                print(f"既存フォルダを使用: {folder['name']}")
                return folder['id']

    # 新規作成
    print(f"フォルダ作成: {folder_name}")
    folder_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_folder_id]
    }
    folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
    return folder['id']

def save_attachment(drive_service, folder_id, filename, content):
    """添付ファイルをDriveに保存"""
    # 既存ファイルチェック
    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    results = drive_service.files().list(q=query, fields='files(id)').execute()
    if results.get('files'):
        return  # 既に存在

    # ファイル保存
    file_metadata = {
        'name': filename,
        'parents': [folder_id]
    }
    drive_service.files().create(
        body=file_metadata,
        media_body=content,
        fields='id'
    ).execute()

def process_email_type(gmail, drive, query, label_name, processed_label_id, investment_folder_id, extract_info_fn):
    """特定タイプのメールを処理"""
    results = []

    response = gmail.users().messages().list(userId='me', q=query).execute()
    messages = response.get('messages', [])

    print(f"検索クエリ: {query}")
    print(f"該当メール数: {len(messages)}")

    for msg in messages:
        try:
            message = gmail.users().messages().get(userId='me', id=msg['id']).execute()

            # 本文取得
            body = ""
            attachments = []
            if 'parts' in message['payload']:
                for part in message['payload']['parts']:
                    if part.get('mimeType') == 'text/plain' and 'data' in part.get('body', {}):
                        import base64
                        body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                    if part.get('filename'):
                        attachments.append(part)

            # 物件情報抽出
            info = extract_info_fn(body, attachments) if len(extract_info_fn.__code__.co_varnames) > 1 else extract_info_fn(body)
            property_number, station = info

            if not property_number:
                print(f"物件番号を抽出できませんでした: {message.get('snippet', '')[:50]}")
                continue

            print(f"処理中: 物件番号={property_number} 駅={station}")

            # メール受信日を取得
            date_str = datetime.now().strftime('%Y%m%d')

            # フォルダ名を生成
            folder_name = f"{date_str}_{station}_{property_number}"

            # フォルダ作成
            folder_id = get_or_create_folder(drive, investment_folder_id, folder_name, property_number)

            # 添付ファイル保存
            for part in attachments:
                filename = part.get('filename')
                attachment_id = part['body'].get('attachmentId')

                if attachment_id:
                    attachment = gmail.users().messages().attachments().get(
                        userId='me', messageId=msg['id'], id=attachment_id
                    ).execute()

                    import base64
                    file_data = base64.urlsafe_b64decode(attachment['data'])

                    # 既存ファイルチェック
                    query_file = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
                    existing = drive.files().list(q=query_file, fields='files(id)').execute()

                    if existing.get('files'):
                        print(f"スキップ（既存）: {filename}")
                        continue

                    # ファイル保存
                    from io import BytesIO
                    from googleapiclient.http import MediaIoBaseUpload

                    media = MediaIoBaseUpload(BytesIO(file_data), mimetype='application/octet-stream', resumable=True)
                    file_metadata = {
                        'name': filename,
                        'parents': [folder_id]
                    }
                    drive.files().create(body=file_metadata, media_body=media, fields='id').execute()
                    print(f"保存完了: {filename} → {folder_name}")

            # 処理済みラベル追加
            gmail.users().messages().modify(
                userId='me',
                id=msg['id'],
                body={'addLabelIds': [processed_label_id]}
            ).execute()

            results.append(f"Processed: {folder_name}")

        except Exception as e:
            print(f"エラー: {e}")
            import traceback
            traceback.print_exc()

    return results

def process_emails():
    """メールを処理"""
    gmail = get_gmail_service()
    drive = get_drive_service()

    investment_folder_id = get_secret("INVESTMENT_FOLDER_ID")
    label_name = get_secret("PROCESSED_LABEL_NAME")
    processed_label_id = get_or_create_label(gmail, label_name)

    all_results = []

    # 販売図面メールを処理
    query1 = f'subject:販売図面 newer_than:2h has:attachment -label:{label_name}'
    results1 = process_email_type(
        gmail, drive, query1, label_name, processed_label_id,
        investment_folder_id, extract_property_info_from_hanbaizumen
    )
    all_results.extend(results1)

    # 住宅地図・路線価図メールを処理
    query2 = f'subject:住宅地図・路線価図 newer_than:2h has:attachment -label:{label_name}'
    results2 = process_email_type(
        gmail, drive, query2, label_name, processed_label_id,
        investment_folder_id, extract_property_info_from_chizu
    )
    all_results.extend(results2)

    return all_results

@app.route('/health', methods=['GET'])
def health():
    """ヘルスチェック"""
    return jsonify({"status": "ok"})

@app.route('/process', methods=['POST'])
def process():
    """メール処理エンドポイント"""
    try:
        results = process_emails()
        return jsonify({
            "status": "success",
            "processed": len(results),
            "details": results
        })
    except Exception as e:
        import traceback
        print(f"ERROR: {e}")
        print(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
