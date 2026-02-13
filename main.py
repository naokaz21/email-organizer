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
from typing import Optional
import io
from pypdf import PdfReader
import google.generativeai as genai
import googlemaps

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

def get_docs_service():
    """Docs APIサービスを取得"""
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
            "https://www.googleapis.com/auth/gmail.labels",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/documents"
        ]
    )

    return build('docs', 'v1', credentials=creds)

def get_gmaps_client():
    """Google Maps APIクライアントを取得"""
    api_key = get_secret("GOOGLE_MAPS_API_KEY")
    return googlemaps.Client(key=api_key)

def get_gemini_client():
    """Gemini APIクライアントを取得"""
    api_key = get_secret("GEMINI_API_KEY")
    genai.configure(api_key=api_key)
    return genai.GenerativeModel('gemini-2.0-flash-exp')

def extract_text_from_pdf(file_data: bytes) -> str:
    """PDFバイナリデータからテキストを抽出"""
    try:
        pdf_file = io.BytesIO(file_data)
        reader = PdfReader(pdf_file)
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        return text.strip()
    except Exception as e:
        print(f"PDF解析エラー: {e}")
        return ""

def extract_text_from_image(file_data: bytes, gemini_client) -> str:
    """画像ファイルからテキストを抽出（Gemini Vision使用）"""
    try:
        import PIL.Image
        image = PIL.Image.open(io.BytesIO(file_data))

        prompt = """この画像は不動産の販売図面です。画像内のすべてのテキストを抽出してください。
特に以下の情報を正確に抽出してください：
- 住所
- 物件番号
- 専有面積
- 間取り
- 築年月
- 管理費
- 修繕積立金
- その他すべての文字情報

すべてのテキストを改行で区切って出力してください。"""

        response = gemini_client.generate_content([prompt, image])
        text = response.text.strip()
        print(f"画像からテキスト抽出完了: {len(text)} 文字")
        return text
    except Exception as e:
        print(f"画像解析エラー: {e}")
        import traceback
        traceback.print_exc()
        return ""

def is_hanbaizumen(text: str) -> bool:
    """テキスト内容から販売図面かどうかを判定（キーワードベース）"""
    # 販売図面に特有のキーワード
    keywords = [
        '販売図面',
        '物件番号',
        '専有面積',
        '間取り',
        'バルコニー面積',
        '築年月',
        '総戸数',
        '管理費',
        '修繕積立金'
    ]

    # 3つ以上のキーワードが含まれていれば販売図面と判定
    match_count = sum(1 for keyword in keywords if keyword in text)
    print(f"販売図面判定: {match_count}個のキーワードマッチ")
    return match_count >= 3

def extract_address_with_regex(text: str) -> Optional[str]:
    """正規表現で住所を抽出"""
    patterns = [
        r'(東京都|大阪府|京都府|北海道|[一-龥]+県)[一-龥ぁ-んa-zA-Z0-9ー\s]+市[一-龥ぁ-んa-zA-Z0-9ー\s]+',
        r'(東京都|大阪府|京都府|北海道|[一-龥]+県)[一-龥ぁ-んa-zA-Z0-9ー\s]+区[一-龥ぁ-んa-zA-Z0-9ー\s]+',
        r'東京都[一-龥ぁ-んa-zA-Z0-9ー\s]+区[一-龥ぁ-んa-zA-Z0-9ー\s]+[0-9]+',
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0)
    return None

def extract_address_with_gemini(text: str, gemini_client) -> Optional[str]:
    """Gemini APIで住所を抽出（フォールバック）"""
    try:
        prompt = f"""
以下のテキストから不動産物件の住所を抽出してください。
住所のみを出力してください（説明不要）。

テキスト:
{text[:2000]}
"""
        response = gemini_client.generate_content(prompt)
        address = response.text.strip()
        return address if address else None
    except Exception as e:
        print(f"Gemini住所抽出エラー: {e}")
        return None

def geocode_address(address: str, gmaps_client) -> Optional[dict]:
    """住所から位置情報を取得"""
    try:
        geocode_result = gmaps_client.geocode(address, language='ja')
        if geocode_result:
            location = geocode_result[0]['geometry']['location']
            formatted_address = geocode_result[0]['formatted_address']
            return {
                'lat': location['lat'],
                'lng': location['lng'],
                'formatted_address': formatted_address
            }
        return None
    except Exception as e:
        print(f"Geocoding エラー: {e}")
        return None

def research_market_price(location: dict, property_info: dict, gemini_client) -> dict:
    """Gemini APIで周辺相場を調査"""
    try:
        prompt = f"""
あなたは不動産投資の専門家です。以下の物件について、周辺の類似物件の家賃相場を調査してください。

物件情報:
- 住所: {location['formatted_address']}
- 緯度経度: {location['lat']}, {location['lng']}
- 駅: {property_info.get('station', '不明')}
- 物件番号: {property_info.get('property_number')}

以下の形式でレポートしてください:
1. 周辺エリアの特徴
2. 類似物件の家賃相場（ワンルーム、1K、1DK、2DKなど）
3. 相場の根拠となる情報源
4. 投資観点での評価コメント

マークダウン形式で出力してください。
"""
        response = gemini_client.generate_content(prompt)
        return {
            'status': 'success',
            'report': response.text,
            'model': 'gemini-2.0-flash-exp'
        }
    except Exception as e:
        print(f"Gemini相場調査エラー: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'report': '相場調査に失敗しました。'
        }

def create_evaluation_report(docs_service, drive_service, folder_id: str, report_data: dict) -> str:
    """Google Docsでレポートを作成"""
    try:
        # ドキュメント作成
        title = f"物件評価レポート_{report_data['property_number']}_{report_data['station']}"
        doc = docs_service.documents().create(body={'title': title}).execute()
        doc_id = doc['documentId']

        # コンテンツ作成
        content = f"""物件評価レポート

物件番号: {report_data['property_number']}
駅: {report_data['station']}
住所: {report_data.get('address', '不明')}
緯度経度: {report_data['location']['lat']}, {report_data['location']['lng']}

【相場調査結果】
{report_data.get('market_report', '')}

作成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

        # テキスト挿入
        requests = [
            {
                'insertText': {
                    'location': {'index': 1},
                    'text': content
                }
            }
        ]

        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': requests}
        ).execute()

        # ドキュメントを物件フォルダに移動
        file = drive_service.files().get(fileId=doc_id, fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))
        drive_service.files().update(
            fileId=doc_id,
            addParents=folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()

        print(f"レポート作成完了: {title}")
        return doc_id

    except Exception as e:
        print(f"レポート作成エラー: {e}")
        import traceback
        traceback.print_exc()
        return None

def generate_property_evaluation_report(
    drive_service,
    docs_service,
    gmaps_client,
    gemini_client,
    folder_id: str,
    pdf_file_id: str,
    property_number: str,
    station: str,
    extracted_text: Optional[str] = None
) -> Optional[str]:
    """物件評価レポートを生成するメインフロー"""

    print(f"レポート生成開始: 物件番号={property_number}")

    try:
        # 1. テキスト取得（既に抽出済みの場合はそれを使用）
        if extracted_text:
            text = extracted_text
            print(f"抽出済みテキスト使用: {len(text)} 文字")
        else:
            # PDFダウンロードしてテキスト抽出
            request = drive_service.files().get_media(fileId=pdf_file_id)
            fh = io.BytesIO()
            from googleapiclient.http import MediaIoBaseDownload
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()

            pdf_data = fh.getvalue()
            print(f"PDF取得完了: {len(pdf_data)} bytes")

            text = extract_text_from_pdf(pdf_data)
            if not text:
                print("エラー: PDFからテキスト抽出失敗")
                return None
            print(f"テキスト抽出完了: {len(text)} 文字")

        # 3. 住所抽出（正規表現 → Geminiフォールバック）
        address = extract_address_with_regex(text)
        if not address:
            print("正規表現で住所抽出失敗、Geminiを使用")
            address = extract_address_with_gemini(text, gemini_client)

        if not address:
            print("エラー: 住所抽出失敗")
            return None
        print(f"住所抽出完了: {address}")

        # 4. 位置情報取得
        location = geocode_address(address, gmaps_client)
        if not location:
            print("エラー: Geocoding失敗")
            return None
        print(f"位置情報取得完了: {location}")

        # 5. 相場調査
        property_info = {
            'property_number': property_number,
            'station': station
        }
        market_data = research_market_price(location, property_info, gemini_client)
        print(f"相場調査完了: {market_data['status']}")

        # 6. レポート作成
        report_data = {
            'property_number': property_number,
            'station': station,
            'address': location['formatted_address'],
            'location': location,
            'market_report': market_data['report']
        }

        doc_id = create_evaluation_report(docs_service, drive_service, folder_id, report_data)

        if doc_id:
            print(f"レポート生成完了: Doc ID={doc_id}")
            return doc_id
        else:
            print("エラー: レポート作成失敗")
            return None

    except Exception as e:
        print(f"レポート生成エラー: {e}")
        import traceback
        traceback.print_exc()
        return None

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
    """販売図面メールから物件情報を抽出（Gemini使用）"""
    property_number = None
    station = None

    # 添付ファイル名から物件番号を抽出（優先）
    for att in attachments:
        match = re.search(r'Hanbaizumen_(\d+)', att.get('filename', ''))
        if match:
            property_number = match.group(1)
            break

    # Gemini APIで本文から物件番号と駅名を抽出
    try:
        gemini_client = get_gemini_client()

        prompt = f"""あなたは不動産メールから物件情報を抽出する専門アシスタントです。

タスク: 以下のメール本文から物件番号と最寄駅を抽出してください。

抽出条件:
- 物件番号: "物件番号:数字" "物件番号：数字" "hid=数字" という記載から数字部分のみ
- 駅名: "駅名+駅" "駅:駅名" "駅：駅名" という記載から駅名部分のみ（「駅」という文字は除く）
- 見つからない場合はnull

重要: メール本文に実際に書かれている情報のみを抽出してください。推測・補完は禁止です。

=== メール本文ここから ===
{message_body}
=== メール本文ここまで ===

JSON形式で回答:
{{"property_number": "数字のみ", "station": "駅名のみ"}}"""

        response = gemini_client.generate_content(prompt)
        result_text = response.text.strip()

        # JSONとして解析
        import json
        # ```json ``` で囲まれている場合は除去
        if result_text.startswith('```'):
            result_text = result_text.split('```')[1]
            if result_text.startswith('json'):
                result_text = result_text[4:]
            result_text = result_text.strip()

        result = json.loads(result_text)

        # 物件番号（添付ファイル名から取得できていない場合のみ）
        if not property_number and result.get('property_number'):
            property_number = str(result['property_number'])

        # 駅名
        if result.get('station'):
            station = result['station']

        print(f"✅ Gemini抽出成功 - 物件番号: {property_number}, 駅: {station}")

    except Exception as e:
        print(f"⚠️  Gemini抽出エラー（フォールバック実行）: {e}")

        # フォールバック: URLから物件番号を取得
        if not property_number:
            url_match = re.search(r'hid=(\d+)', message_body)
            if url_match:
                property_number = url_match.group(1)
                print(f"📍 URLから物件番号抽出: {property_number}")

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

            # 本文取得（再帰的にpartsを探索）
            import base64
            body = ""
            attachments = []

            def extract_body_and_attachments(parts):
                nonlocal body, attachments
                for part in parts:
                    mime_type = part.get('mimeType', '')

                    # text/plain を見つけたら本文として取得
                    if mime_type == 'text/plain' and 'data' in part.get('body', {}):
                        body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')

                    # 添付ファイル
                    if part.get('filename'):
                        attachments.append(part)

                    # multipart/* の場合は再帰的に探索
                    if mime_type.startswith('multipart/') and 'parts' in part:
                        extract_body_and_attachments(part['parts'])

            if 'parts' in message['payload']:
                extract_body_and_attachments(message['payload']['parts'])

            # parts がない、またはbodyが空の場合のフォールバック
            if not body and 'body' in message['payload'] and 'data' in message['payload']['body']:
                body = base64.urlsafe_b64decode(message['payload']['body']['data']).decode('utf-8', errors='ignore')

            # 物件情報抽出
            info = extract_info_fn(body, attachments) if len(extract_info_fn.__code__.co_varnames) > 1 else extract_info_fn(body)
            property_number, station = info

            if not property_number:
                print(f"⚠️  物件番号を抽出できませんでした（処理は継続）: {message.get('snippet', '')[:50]}")
                # 物件番号がない場合はメッセージIDの一部を使用
                property_number = msg['id'][:8]

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
                    uploaded_file = drive.files().create(body=file_metadata, media_body=media, fields='id').execute()
                    print(f"保存完了: {filename} → {folder_name}")

                    # PDF/画像の場合、中身を確認して販売図面か判定
                    is_pdf = filename.lower().endswith('.pdf')
                    is_image = filename.lower().endswith(('.jpg', '.jpeg', '.png'))

                    if is_pdf or is_image:
                        # テキスト抽出
                        if is_pdf:
                            extracted_text = extract_text_from_pdf(file_data)
                        else:  # 画像
                            gemini_client = get_gemini_client()
                            extracted_text = extract_text_from_image(file_data, gemini_client)

                        if is_hanbaizumen(extracted_text):
                            try:
                                print(f"販売図面検出、評価レポート生成を開始: {filename}")

                                # APIクライアント初期化
                                docs_service = get_docs_service()
                                gmaps_client = get_gmaps_client()
                                gemini_client = get_gemini_client()

                                # レポート生成（extracted_textを渡す）
                                report_doc_id = generate_property_evaluation_report(
                                    drive_service=drive,
                                    docs_service=docs_service,
                                    gmaps_client=gmaps_client,
                                    gemini_client=gemini_client,
                                    folder_id=folder_id,
                                    pdf_file_id=uploaded_file['id'],
                                    property_number=property_number,
                                    station=station,
                                    extracted_text=extracted_text
                                )

                                if report_doc_id:
                                    print(f"評価レポート生成成功: {report_doc_id}")
                                else:
                                    print(f"評価レポート生成失敗（処理は継続）")
                            except Exception as e:
                                print(f"レポート生成エラー（処理継続）: {e}")
                                import traceback
                                traceback.print_exc()

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
    query1 = f'subject:販売図面 newer_than:15m has:attachment -label:{label_name}'
    results1 = process_email_type(
        gmail, drive, query1, label_name, processed_label_id,
        investment_folder_id, extract_property_info_from_hanbaizumen
    )
    all_results.extend(results1)

    # 住宅地図・路線価図メールを処理
    query2 = f'subject:住宅地図・路線価図 newer_than:15m has:attachment -label:{label_name}'
    results2 = process_email_type(
        gmail, drive, query2, label_name, processed_label_id,
        investment_folder_id, extract_property_info_from_chizu
    )
    all_results.extend(results2)

    return all_results

@app.route('/', methods=['GET'])
def index():
    """手動実行用WebUI"""
    html = """
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Email Organizer - 手動実行</title>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                max-width: 800px;
                margin: 50px auto;
                padding: 20px;
                background: #f5f5f5;
            }
            .container {
                background: white;
                padding: 30px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            h1 { color: #333; margin-top: 0; }
            button {
                background: #4285f4;
                color: white;
                border: none;
                padding: 12px 24px;
                font-size: 16px;
                border-radius: 4px;
                cursor: pointer;
                transition: background 0.3s;
            }
            button:hover { background: #357ae8; }
            button:disabled { background: #ccc; cursor: not-allowed; }
            #result {
                margin-top: 20px;
                padding: 15px;
                border-radius: 4px;
                display: none;
            }
            .success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
            .error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
            .loading { background: #fff3cd; color: #856404; border: 1px solid #ffeaa7; }
            pre { background: #f5f5f5; padding: 10px; border-radius: 4px; overflow-x: auto; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📧 Email Organizer</h1>
            <p>販売図面・住宅地図メールを手動で処理します</p>
            <button onclick="runProcess()" id="runBtn">🚀 メール整理を実行</button>
            <div id="result"></div>
        </div>
        <script>
            async function runProcess() {
                const btn = document.getElementById('runBtn');
                const result = document.getElementById('result');

                btn.disabled = true;
                result.className = 'loading';
                result.style.display = 'block';
                result.innerHTML = '⏳ 処理中...';

                try {
                    const response = await fetch('/process', { method: 'POST' });
                    const data = await response.json();

                    if (response.ok) {
                        result.className = 'success';
                        result.innerHTML = `
                            <strong>✅ 処理完了</strong><br>
                            処理件数: ${data.processed}件<br>
                            <pre>${JSON.stringify(data.details, null, 2)}</pre>
                        `;
                    } else {
                        throw new Error(data.message || '処理に失敗しました');
                    }
                } catch (error) {
                    result.className = 'error';
                    result.innerHTML = `<strong>❌ エラー</strong><br>${error.message}`;
                } finally {
                    btn.disabled = false;
                }
            }
        </script>
    </body>
    </html>
    """
    return html

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
