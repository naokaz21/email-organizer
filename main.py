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
from simulation import run_simulation, create_simulation_excel, format_simulation_summary_for_report

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
    # Gemini 2.5 Flash (2026年現在の推奨モデル、1.5は廃止済み)
    return genai.GenerativeModel('gemini-2.5-flash')

def get_perplexity_client():
    """Perplexity APIクライアントを取得（OpenAI互換）"""
    try:
        # PERPLEXITY_API_KEYはオプション（なければフリー層で動作）
        try:
            api_key = get_secret("PERPLEXITY_API_KEY")
            print("Perplexity API Key取得成功（有料層）")
        except Exception:
            api_key = "pplx-dummy-key"  # フリー層用
            print("Perplexity API Key未設定（フリー層: 5リクエスト/日）")

        from openai import OpenAI
        client = OpenAI(
            api_key=api_key,
            base_url="https://api.perplexity.ai"
        )
        return client

    except Exception as e:
        print(f"Perplexity クライアント初期化エラー: {e}")
        return None

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

def parse_gemini_property_response(response_text: str) -> dict:
    """GeminiのJSON応答を安全にパース"""
    try:
        import json

        # マークダウンコードブロック（```json```）を除去
        text = response_text.strip()
        if text.startswith('```'):
            # ```json で始まる場合
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
            text = text.strip()

        # JSON パース
        data = json.loads(text)

        # 数値型への変換（文字列として返される可能性があるため）
        numeric_fields = ['price', 'land_area', 'building_area', 'total_units',
                         'full_occupancy_rent', 'management_fee', 'reserve_fund']

        for field in numeric_fields:
            if field in data and data[field] is not None:
                try:
                    # カンマ除去して数値変換
                    if isinstance(data[field], str):
                        data[field] = float(data[field].replace(',', ''))
                except (ValueError, AttributeError):
                    data[field] = None

        # rent_rollの各部屋の数値も変換
        if data.get('rent_roll') and isinstance(data['rent_roll'], list):
            for room in data['rent_roll']:
                if 'area' in room and room['area'] is not None:
                    try:
                        if isinstance(room['area'], str):
                            room['area'] = float(room['area'].replace(',', ''))
                    except (ValueError, AttributeError):
                        room['area'] = None
                if 'rent' in room and room['rent'] is not None:
                    try:
                        if isinstance(room['rent'], str):
                            room['rent'] = float(room['rent'].replace(',', ''))
                    except (ValueError, AttributeError):
                        room['rent'] = None

        return data

    except json.JSONDecodeError as e:
        print(f"JSON パースエラー: {e}")
        print(f"レスポンス: {response_text[:200]}...")
        return {}
    except Exception as e:
        print(f"予期しないエラー: {e}")
        return {}

def extract_comprehensive_property_data(file_data: bytes, filename: str, gemini_client) -> dict:
    """販売図面から包括的な物件情報を抽出（Gemini使用）"""
    try:
        # ファイル種別判定
        is_pdf = filename.lower().endswith('.pdf')
        is_image = filename.lower().endswith(('.jpg', '.jpeg', '.png'))

        # テキスト抽出
        if is_pdf:
            # PDFからテキスト抽出
            text = extract_text_from_pdf(file_data)
            if not text:
                print("PDFからのテキスト抽出に失敗")
                return {}

            # Geminiで構造化分析
            prompt = f"""あなたは不動産販売図面から物件情報を抽出する専門AIです。

以下のテキストから物件情報を抽出し、JSON形式で出力してください。

【テキスト】
{text}

【抽出項目】
1. 基本情報:
   - property_number: 物件番号 (数字のみ)
   - station: 最寄駅 (「駅」を除く駅名のみ)
   - address: 住所 (完全な住所)

2. 価格・構造:
   - price: 販売価格 (円、数値のみ)
   - structure: 構造 (RC, SRC, 木造など)
   - year_built: 築年月 (YYYY年MM月 または YYYY/MM形式)

3. 面積・規模:
   - land_area: 土地面積 (㎡、数値のみ)
   - building_area: 建物面積 (㎡、数値のみ)
   - total_units: 総戸数 (数値のみ)

4. 賃料情報:
   - full_occupancy_rent: 満室想定賃料 (月額円、数値のみ)
   - floor_plan: 間取り (例: 1K, 1DK, 2LDK)
   - management_fee: 管理費 (月額円、数値のみ)
   - reserve_fund: 修繕積立金 (月額円、数値のみ)

5. レントロール (部屋別賃料一覧):
   - rent_roll: 配列形式 [{{"room": "部屋番号", "plan": "間取り", "area": 面積, "rent": 賃料}}, ...]

【重要な指示】
- 情報が見つからない場合は null を設定
- 推測や補完は禁止、記載されている情報のみ抽出
- 数値は数字のみ抽出（単位記号、カンマは除く）
- 出力は必ず有効なJSON形式

【出力形式】
{{
  "property_number": "物件番号 or null",
  "station": "駅名 or null",
  "address": "住所 or null",
  "price": 価格数値 or null,
  "structure": "構造 or null",
  "year_built": "築年月 or null",
  "land_area": 面積数値 or null,
  "building_area": 面積数値 or null,
  "total_units": 戸数 or null,
  "full_occupancy_rent": 賃料数値 or null,
  "floor_plan": "間取り or null",
  "management_fee": 管理費数値 or null,
  "reserve_fund": 積立金数値 or null,
  "rent_roll": [配列] or null
}}
"""
            response = gemini_client.generate_content(prompt)
            result = parse_gemini_property_response(response.text)
            print(f"PDF詳細抽出完了: {len(result)} フィールド")
            return result

        elif is_image:
            # Gemini Visionで画像を直接分析
            import PIL.Image
            image = PIL.Image.open(io.BytesIO(file_data))

            prompt = """あなたは不動産販売図面から物件情報を抽出する専門AIです。

この画像から物件情報を抽出し、JSON形式で出力してください。

【抽出項目】
1. 基本情報:
   - property_number: 物件番号 (数字のみ)
   - station: 最寄駅 (「駅」を除く駅名のみ)
   - address: 住所 (完全な住所)

2. 価格・構造:
   - price: 販売価格 (円、数値のみ)
   - structure: 構造 (RC, SRC, 木造など)
   - year_built: 築年月 (YYYY年MM月 または YYYY/MM形式)

3. 面積・規模:
   - land_area: 土地面積 (㎡、数値のみ)
   - building_area: 建物面積 (㎡、数値のみ)
   - total_units: 総戸数 (数値のみ)

4. 賃料情報:
   - full_occupancy_rent: 満室想定賃料 (月額円、数値のみ)
   - floor_plan: 間取り (例: 1K, 1DK, 2LDK)
   - management_fee: 管理費 (月額円、数値のみ)
   - reserve_fund: 修繕積立金 (月額円、数値のみ)

5. レントロール (部屋別賃料一覧):
   - rent_roll: 配列形式 [{"room": "部屋番号", "plan": "間取り", "area": 面積, "rent": 賃料}, ...]

【重要な指示】
- 情報が見つからない場合は null を設定
- 推測や補完は禁止、記載されている情報のみ抽出
- 数値は数字のみ抽出（単位記号、カンマは除く）
- 出力は必ず有効なJSON形式

【出力形式】
{
  "property_number": "物件番号 or null",
  "station": "駅名 or null",
  "address": "住所 or null",
  "price": 価格数値 or null,
  "structure": "構造 or null",
  "year_built": "築年月 or null",
  "land_area": 面積数値 or null,
  "building_area": 面積数値 or null,
  "total_units": 戸数 or null,
  "full_occupancy_rent": 賃料数値 or null,
  "floor_plan": "間取り or null",
  "management_fee": 管理費数値 or null,
  "reserve_fund": 積立金数値 or null,
  "rent_roll": [配列] or null
}
"""
            response = gemini_client.generate_content([prompt, image])
            result = parse_gemini_property_response(response.text)
            print(f"画像詳細抽出完了: {len(result)} フィールド")
            return result

        else:
            print(f"サポートされていないファイル形式: {filename}")
            return {}

    except Exception as e:
        print(f"包括的データ抽出エラー: {e}")
        import traceback
        traceback.print_exc()
        return {}

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
        '修繕積立金',
        '販売価格',  # Phase 1で追加
        '構造',  # Phase 1で追加
        '満室想定賃料',  # Phase 1で追加
        'レントロール'  # Phase 1で追加
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

def research_area_with_perplexity(location: dict, property_info: dict, perplexity_client) -> dict:
    """Perplexity APIでエリア調査（人口動態、ハザードマップ、再開発計画）"""
    if not perplexity_client:
        return {
            'status': 'error',
            'error': 'Perplexity client not available',
            'report': 'エリア調査をスキップしました（Perplexity APIクライアントなし）'
        }

    try:
        prompt = f"""
あなたは不動産投資エリア分析の専門家です。以下の物件エリアについて最新情報を調査してください。

物件情報:
- 住所: {location['formatted_address']}
- 緯度経度: {location['lat']}, {location['lng']}
- 駅: {property_info.get('station', '不明')}

以下の3つの観点で調査し、マークダウン形式でレポートしてください:

## 1. 人口動態
- 過去10年の人口推移
- 単身世帯比率
- 年齢構成（特に賃貸需要層）
- 将来予測

## 2. ハザードマップ
- 洪水リスク（浸水想定区域）
- 地震リスク（液状化、活断層）
- 土砂災害リスク
- 公式ハザードマップのURL

## 3. 再開発計画
- 周辺の大規模開発プロジェクト
- 新駅・路線延伸計画
- 商業施設・インフラ整備
- 公式発表のURL

**重要**: 必ず出典URLを記載してください。2024年以降の最新情報を優先してください。
"""

        response = perplexity_client.chat.completions.create(
            model="sonar",  # Perplexityの標準モデル
            messages=[
                {"role": "system", "content": "あなたは不動産投資エリア分析の専門家です。最新の公開情報に基づいて正確な調査を行います。"},
                {"role": "user", "content": prompt}
            ]
        )

        report_text = response.choices[0].message.content

        return {
            'status': 'success',
            'report': report_text,
            'model': 'perplexity-sonar'
        }

    except Exception as e:
        print(f"Perplexityエリア調査エラー: {e}")
        import traceback
        traceback.print_exc()
        return {
            'status': 'error',
            'error': str(e),
            'report': 'エリア調査に失敗しました。'
        }

def combine_research_reports(gemini_market_report: dict, perplexity_area_report: dict) -> str:
    """Gemini市場調査とPerplexityエリア調査を統合"""
    combined_parts = []

    # Gemini市場調査
    if gemini_market_report.get('status') == 'success':
        combined_parts.append("## 市場調査（Gemini）")
        combined_parts.append(gemini_market_report.get('report', ''))
    else:
        combined_parts.append("## 市場調査")
        combined_parts.append("市場調査に失敗しました。")

    combined_parts.append("")

    # Perplexityエリア調査
    if perplexity_area_report.get('status') == 'success':
        combined_parts.append("## エリア分析（Perplexity）")
        combined_parts.append(perplexity_area_report.get('report', ''))
    else:
        # Perplexity失敗時はスキップ（graceful degradation）
        combined_parts.append("## エリア分析")
        combined_parts.append("エリア分析をスキップしました。")

    return "\n".join(combined_parts)

def _find_placeholder_index(docs_service, doc_id, placeholder):
    """ドキュメント内のプレースホルダー文字列のインデックスを検索"""
    doc = docs_service.documents().get(documentId=doc_id).execute()
    for element in doc['body']['content']:
        if 'paragraph' in element:
            for run in element['paragraph'].get('elements', []):
                text = run.get('textRun', {}).get('content', '')
                if placeholder in text:
                    return run['startIndex']
    return None


def _find_placeholder_range(docs_service, doc_id, placeholder):
    """プレースホルダー行全体のstart/endインデックスを返す"""
    doc = docs_service.documents().get(documentId=doc_id).execute()
    for element in doc['body']['content']:
        if 'paragraph' in element:
            full_text = ''
            for run in element['paragraph'].get('elements', []):
                full_text += run.get('textRun', {}).get('content', '')
            if placeholder in full_text:
                return element['startIndex'], element['endIndex']
    return None, None


def _insert_table_at_placeholder(docs_service, doc_id, placeholder, rows_data, col_count):
    """プレースホルダーをテーブルに置換し、セルにデータを入力"""
    start, end = _find_placeholder_range(docs_service, doc_id, placeholder)
    if start is None:
        print(f"プレースホルダー未検出: {placeholder}")
        return

    # プレースホルダー行を削除
    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={'requests': [{'deleteContentRange': {'range': {'startIndex': start, 'endIndex': end}}}]}
    ).execute()

    # テーブル挿入
    row_count = len(rows_data)
    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={'requests': [{'insertTable': {
            'rows': row_count, 'columns': col_count,
            'location': {'index': start}
        }}]}
    ).execute()

    # ドキュメント再取得してテーブルセルのインデックスを取得
    doc = docs_service.documents().get(documentId=doc_id).execute()
    table = None
    for element in doc['body']['content']:
        if 'table' in element and element['startIndex'] >= start:
            table = element['table']
            break

    if not table:
        print(f"テーブル未検出: {placeholder}")
        return

    # セルにデータを入力（逆順でインデックスずれ防止）
    cell_requests = []
    for r in range(row_count - 1, -1, -1):
        row = table['tableRows'][r]
        for c in range(col_count - 1, -1, -1):
            cell = row['tableCells'][c]
            cell_index = cell['content'][0]['paragraph']['elements'][0]['startIndex']
            text = str(rows_data[r][c]) if c < len(rows_data[r]) else ''
            if text:
                cell_requests.append({'insertText': {'location': {'index': cell_index}, 'text': text}})

    if cell_requests:
        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': cell_requests}
        ).execute()

    # ヘッダー行（1行目）を太字にする
    header_row = table['tableRows'][0]
    bold_requests = []
    for c in range(col_count):
        cell = header_row['tableCells'][c]
        cell_start = cell['content'][0]['paragraph']['elements'][0]['startIndex']
        cell_end = cell['content'][0]['paragraph']['elements'][-1]['endIndex']
        if cell_end > cell_start:
            bold_requests.append({
                'updateTextStyle': {
                    'range': {'startIndex': cell_start, 'endIndex': cell_end - 1},
                    'textStyle': {'bold': True},
                    'fields': 'bold'
                }
            })
    if bold_requests:
        try:
            docs_service.documents().batchUpdate(
                documentId=doc_id,
                body={'requests': bold_requests}
            ).execute()
        except Exception:
            pass  # ヘッダー太字は見た目のみなのでエラーは無視


def create_evaluation_report(docs_service, drive_service, folder_id: str, report_data: dict) -> str:
    """Google Docsで要件定義書サンプル準拠の構造化レポートを作成"""
    try:
        # ドキュメント作成
        title = f"物件評価レポート_{report_data['property_number']}_{report_data['station']}"
        doc = docs_service.documents().create(body={'title': title}).execute()
        doc_id = doc['documentId']

        detailed = report_data.get('detailed_data', {})
        sim_result = detailed.get('simulation_result')
        now = datetime.now().strftime('%Y年%m月%d日')

        # === Step 1: テキスト部分を構築 ===
        # 各セクションをリストで管理 (text, style) のペア
        # style: 'TITLE', 'HEADING_1', 'HEADING_2', 'NORMAL_TEXT'
        sections = []

        # タイトル
        sections.append((f"{report_data['station']}_{report_data['property_number']} 物件調査レポート", 'TITLE'))
        sections.append((f"調査日：{now}", 'NORMAL_TEXT'))

        # A1. 物件概要
        sections.append(("A1. 物件概要", 'HEADING_1'))
        sections.append(("基本情報", 'HEADING_2'))
        sections.append(("{{TABLE_BASIC_INFO}}", 'NORMAL_TEXT'))

        # レントロール
        if detailed.get('rent_roll') and len(detailed['rent_roll']) > 0:
            sections.append(("レントロール", 'HEADING_2'))
            sections.append(("{{TABLE_RENT_ROLL}}", 'NORMAL_TEXT'))

        # A2. 周辺環境調査
        sections.append(("A2. 周辺環境調査", 'HEADING_1'))
        market_text = report_data.get('market_report', '調査データなし')
        sections.append((market_text, 'NORMAL_TEXT'))

        # A3. 収益シミュレーション概要
        sections.append(("A3. 収益シミュレーション概要", 'HEADING_1'))
        if sim_result:
            sections.append(("主要設定条件", 'HEADING_2'))
            sections.append(("{{TABLE_SIM_CONDITIONS}}", 'NORMAL_TEXT'))
            sections.append(("投資分析結果", 'HEADING_2'))
            sections.append(("{{TABLE_SIM_RESULTS}}", 'NORMAL_TEXT'))
        else:
            sections.append(("シミュレーション実行不可（データ不足）", 'NORMAL_TEXT'))

        # A4. 投資判断コメント
        sections.append(("A4. 投資判断コメント", 'HEADING_1'))
        if sim_result:
            d = sim_result['decision']
            m = sim_result['metrics']
            judgment_lines = []
            judgment_lines.append(f"総合判定: {d['recommendation']}（{d['pass_count']}/{d['total_count']}項目クリア）")
            judgment_lines.append("")
            for key, item in d['decisions'].items():
                mark = "○" if item['pass'] else "×"
                judgment_lines.append(f"  {mark} {item['label']}: {item['detail']}")
            if sim_result.get('warnings'):
                judgment_lines.append("")
                judgment_lines.append("※ 注意事項:")
                for w in sim_result['warnings']:
                    judgment_lines.append(f"  - {w}")
            sections.append(("\n".join(judgment_lines), 'NORMAL_TEXT'))
        else:
            sections.append(("データ不足のため投資判断不可", 'NORMAL_TEXT'))

        # 免責事項
        sections.append(("", 'NORMAL_TEXT'))
        sections.append(("※ 本レポートは投資判断の参考情報であり、最終的な投資判断はご自身の責任において行ってください。", 'NORMAL_TEXT'))
        sections.append((f"作成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 'NORMAL_TEXT'))

        # === Step 2: テキスト一括挿入 + スタイル適用 ===
        full_text = "\n".join(s[0] for s in sections)
        requests = [{'insertText': {'location': {'index': 1}, 'text': full_text}}]
        docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()

        # スタイル適用
        style_requests = []
        idx = 1  # ドキュメントのインデックスは1から
        for text, style in sections:
            end_idx = idx + len(text)
            if style != 'NORMAL_TEXT':
                style_requests.append({
                    'updateParagraphStyle': {
                        'range': {'startIndex': idx, 'endIndex': end_idx},
                        'paragraphStyle': {'namedStyleType': style},
                        'fields': 'namedStyleType'
                    }
                })
            idx = end_idx + 1  # +1 for \n separator

        if style_requests:
            docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': style_requests}).execute()

        # === Step 3: テーブル挿入（末尾から逆順） ===

        # 投資分析結果テーブル
        if sim_result:
            p = sim_result['params']
            m = sim_result['metrics']
            d = sim_result['decision']

            def mark(passed):
                return "○" if passed else "×"

            sim_results_data = [
                ["指標", "算出値", "判断基準"],
                ["表面利回り", f"{m['gross_yield']:.2%}", "参考値"],
                ["FCR（総収益率）", f"{m['fcr']:.2%}", f"FCR > K% → {mark(d['decisions']['fcr_vs_k']['pass'])}"],
                ["K%（ローン定数）", f"{m['k_percent']:.2%}", "参考値"],
                ["CCR（自己資本配当率）", f"{m['ccr']:.2%}", f"CCR > FCR → {mark(d['decisions']['ccr_vs_fcr']['pass'])}"],
                ["レバレッジ分析", m['leverage'], f"{mark(d['decisions']['ccr_vs_fcr']['pass'])}"],
                ["DCR（借入償還余裕率）", f"{m['dcr']:.2f}", f"DCR ≥ 1.2 → {mark(d['decisions']['dcr']['pass'])}"],
                ["BER（損益分岐入居率）", f"{m['ber']:.2%}", f"BER ≤ 80% → {mark(d['decisions']['ber']['pass'])}"],
            ]
            if m.get('irr') is not None:
                sim_results_data.append(["IRR（内部収益率）", f"{m['irr']:.2%}", f"IRR > 期待収益率 → {mark(d['decisions']['irr']['pass'])}"])
            else:
                sim_results_data.append(["IRR（内部収益率）", "計算不可", "×"])
            if m.get('npv') is not None:
                sim_results_data.append(["NPV（正味現在価値）", f"¥{m['npv']:,.0f}", f"NPV > 0 → {mark(d['decisions']['npv']['pass'])}"])
            else:
                sim_results_data.append(["NPV（正味現在価値）", "計算不可", "×"])

            _insert_table_at_placeholder(docs_service, doc_id, '{{TABLE_SIM_RESULTS}}', sim_results_data, 3)

            # 設定条件テーブル
            sim_cond_data = [
                ["条件", "値"],
                ["物件購入価格", f"¥{p['purchase_price']:,.0f}"],
                ["購入諸費用（約8%）", f"¥{p['purchase_expenses']:,.0f}"],
                ["購入総費用", f"¥{p['total_purchase_cost']:,.0f}"],
                ["LTV（借入割合）", f"{p['ltv']:.0%}"],
                ["ローン総額", f"¥{p['loan_amount']:,.0f}"],
                ["自己資金", f"¥{p['equity']:,.0f}"],
                ["ローン金利", f"{p['interest_rate']:.3%}"],
                ["返済期間", f"{p['loan_term']}年（元利均等）"],
                ["空室率", f"{p.get('vacancy_rate', 0.05):.0%}"],
                ["保有期間", f"{p.get('holding_period', 10)}年"],
            ]
            _insert_table_at_placeholder(docs_service, doc_id, '{{TABLE_SIM_CONDITIONS}}', sim_cond_data, 2)

        # レントロールテーブル
        if detailed.get('rent_roll') and len(detailed['rent_roll']) > 0:
            rent_data = [["部屋番号", "間取り・広さ", "想定賃料（月額）"]]
            for unit in detailed['rent_roll']:
                room = unit.get('room', unit.get('room_number', '不明'))
                plan = unit.get('plan', unit.get('floor_plan', ''))
                area = unit.get('area', '')
                plan_area = f"{plan}" + (f"（{area}畳）" if area else "")
                rent = unit.get('rent', 0)
                rent_data.append([str(room), plan_area, f"¥{rent:,.0f}"])
            _insert_table_at_placeholder(docs_service, doc_id, '{{TABLE_RENT_ROLL}}', rent_data, 3)

        # 基本情報テーブル
        basic_rows = [["項目", "内容"]]
        basic_rows.append(["所在地", report_data.get('address', '不明')])
        basic_rows.append(["最寄駅", report_data['station']])
        if detailed.get('price'):
            basic_rows.append(["物件価格", f"¥{detailed['price']:,.0f}"])
        if detailed.get('structure'):
            basic_rows.append(["構造", detailed['structure']])
        if detailed.get('year_built'):
            basic_rows.append(["築年月", str(detailed['year_built'])])
        if detailed.get('land_area'):
            basic_rows.append(["土地面積", f"{detailed['land_area']}㎡"])
        if detailed.get('building_area'):
            basic_rows.append(["建物面積", f"{detailed['building_area']}㎡"])
        if detailed.get('total_units'):
            basic_rows.append(["総戸数", f"{int(detailed['total_units'])}戸"])
        if detailed.get('full_occupancy_rent'):
            basic_rows.append(["満室時賃料", f"月額¥{detailed['full_occupancy_rent']:,.0f}（年額¥{detailed['full_occupancy_rent'] * 12:,.0f}）"])
        if detailed.get('floor_plan'):
            basic_rows.append(["間取り", detailed['floor_plan']])
        if sim_result:
            basic_rows.append(["表面利回り", f"{sim_result['metrics']['gross_yield']:.2%}"])

        _insert_table_at_placeholder(docs_service, doc_id, '{{TABLE_BASIC_INFO}}', basic_rows, 2)

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
    extracted_text: Optional[str] = None,
    detailed_data: Optional[dict] = None
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

        # 5. 相場調査（Gemini）
        property_info = {
            'property_number': property_number,
            'station': station
        }
        market_data = research_market_price(location, property_info, gemini_client)
        print(f"相場調査完了: {market_data['status']}")

        # 5.5. エリア調査（Perplexity）
        perplexity_client = get_perplexity_client()
        if perplexity_client:
            area_data = research_area_with_perplexity(location, property_info, perplexity_client)
            print(f"エリア調査完了: {area_data['status']}")
        else:
            area_data = {
                'status': 'error',
                'report': 'Perplexity APIクライアント初期化失敗'
            }
            print("エリア調査スキップ（Perplexity未設定）")

        # 両方の調査結果を統合
        combined_report = combine_research_reports(market_data, area_data)

        # 6. レポート作成
        report_data = {
            'property_number': property_number,
            'station': station,
            'address': location['formatted_address'],
            'location': location,
            'market_report': combined_report,
            'detailed_data': detailed_data or {}
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
    detailed_data = {}

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

        # 添付ファイルから包括的な物件データを抽出
        for att in attachments:
            filename = att.get('filename', '')
            attachment_id = att['body'].get('attachmentId')

            if attachment_id and (filename.lower().endswith('.pdf') or
                                filename.lower().endswith(('.jpg', '.jpeg', '.png'))):
                try:
                    import base64
                    # attachmentは既にprocess_email_typeで取得される前提だが、
                    # ここでは添付ファイルのメタデータのみ参照
                    # 実際のファイルデータは後でprocess_email_typeで取得される
                    print(f"📎 添付ファイル検出（詳細抽出は後で実行）: {filename}")
                except Exception as att_e:
                    print(f"⚠️  添付ファイル処理エラー: {att_e}")

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

    return {
        'property_number': property_number,
        'station': station,
        'detailed_data': detailed_data
    }

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

    # 新形式（dict）で返す
    return {
        'property_number': property_number,
        'station': station,
        'detailed_data': {}
    }

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

            # 新形式（dict）と旧形式（tuple）の両方に対応
            if isinstance(info, dict):
                property_number = info.get('property_number')
                station = info.get('station')
                detailed_data = info.get('detailed_data', {})
            else:
                # 旧形式（tuple）
                property_number, station = info
                detailed_data = {}

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

                                # 包括的な物件データを抽出
                                comprehensive_data = extract_comprehensive_property_data(
                                    file_data, filename, gemini_client
                                )
                                print(f"詳細データ抽出完了: {len(comprehensive_data)} フィールド")

                                # 投資シミュレーション実行
                                simulation_result = None
                                try:
                                    simulation_result = run_simulation(comprehensive_data)
                                    if simulation_result:
                                        print(f"投資シミュレーション完了: {simulation_result['decision']['recommendation']}")
                                        excel_file_id = create_simulation_excel(
                                            simulation_result,
                                            {"property_number": property_number, "station": station},
                                            drive, folder_id
                                        )
                                        if excel_file_id:
                                            print(f"シミュレーションExcel保存完了: {excel_file_id}")
                                    else:
                                        print("投資シミュレーションスキップ（データ不足）")
                                except Exception as sim_e:
                                    print(f"投資シミュレーションエラー（処理継続）: {sim_e}")
                                    import traceback
                                    traceback.print_exc()

                                if simulation_result:
                                    comprehensive_data['simulation_result'] = simulation_result

                                # レポート生成（extracted_textと詳細データを渡す）
                                report_doc_id = generate_property_evaluation_report(
                                    drive_service=drive,
                                    docs_service=docs_service,
                                    gmaps_client=gmaps_client,
                                    gemini_client=gemini_client,
                                    folder_id=folder_id,
                                    pdf_file_id=uploaded_file['id'],
                                    property_number=property_number,
                                    station=station,
                                    extracted_text=extracted_text,
                                    detailed_data=comprehensive_data
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

@app.route('/test/<folder_id>', methods=['POST'])
def test_folder(folder_id):
    """既存フォルダのPDFからシミュレーション+レポート生成をテスト（メール受信スキップ）"""
    try:
        drive = get_drive_service()

        # フォルダ情報取得
        folder_info = drive.files().get(fileId=folder_id, fields='name').execute()
        folder_name = folder_info['name']
        parts = folder_name.split('_')
        if len(parts) >= 3:
            property_number, station = parts[2], parts[1]
        elif len(parts) == 2:
            property_number, station = parts[1], parts[0]
        else:
            property_number, station = folder_name, '不明'

        print(f"テスト開始: {folder_name} (物件:{property_number}, 駅:{station})")

        # フォルダ内のPDF/画像を検索
        query = f"'{folder_id}' in parents and trashed=false and (mimeType='application/pdf' or mimeType contains 'image/')"
        files = drive.files().list(q=query, fields='files(id, name, mimeType)', pageSize=10).execute().get('files', [])

        if not files:
            return jsonify({"status": "error", "message": "PDF/画像ファイルが見つかりません"}), 404

        # 全ファイルを試して販売図面を探す
        from googleapiclient.http import MediaIoBaseDownload
        gemini_client = get_gemini_client()
        target = None
        file_data = None
        extracted_text = ''
        is_sales = False

        # 買付書・地図を後回しにソート
        def sort_key(f):
            name = f['name'].lower()
            if name.startswith('kaitsuke') or name.startswith('map'):
                return 1
            return 0
        sorted_files = sorted(files, key=sort_key)

        for candidate in sorted_files:
            print(f"ファイル確認中: {candidate['name']}")
            req = drive.files().get_media(fileId=candidate['id'])
            fh = io.BytesIO()
            dl = MediaIoBaseDownload(fh, req)
            done = False
            while not done:
                _, done = dl.next_chunk()
            candidate_data = fh.getvalue()

            is_pdf = candidate['name'].lower().endswith('.pdf')
            if is_pdf:
                candidate_text = extract_text_from_pdf(candidate_data)
            else:
                candidate_text = extract_text_from_image(candidate_data, gemini_client)

            if is_hanbaizumen(candidate_text):
                target = candidate
                file_data = candidate_data
                extracted_text = candidate_text
                is_sales = True
                print(f"販売図面発見: {candidate['name']}")
                break
            print(f"  → 販売図面ではない ({len(candidate_text)}文字)")

        # 販売図面が見つからない場合は最初のファイルを使用
        if target is None:
            target = sorted_files[0]
            req = drive.files().get_media(fileId=target['id'])
            fh = io.BytesIO()
            dl = MediaIoBaseDownload(fh, req)
            done = False
            while not done:
                _, done = dl.next_chunk()
            file_data = fh.getvalue()
            is_pdf_fallback = target['name'].lower().endswith('.pdf')
            if is_pdf_fallback:
                extracted_text = extract_text_from_pdf(file_data)
            else:
                extracted_text = extract_text_from_image(file_data, gemini_client)
            print(f"販売図面なし、フォールバック: {target['name']}")

        print(f"対象ファイル: {target['name']} (販売図面: {is_sales})")

        # 包括的データ抽出
        comprehensive_data = extract_comprehensive_property_data(file_data, target['name'], gemini_client)
        print(f"データ抽出完了: {len(comprehensive_data)} フィールド")

        # シミュレーション
        simulation_result = None
        excel_file_id = None
        try:
            simulation_result = run_simulation(comprehensive_data)
            if simulation_result:
                print(f"シミュレーション完了: {simulation_result['decision']['recommendation']}")
                excel_file_id = create_simulation_excel(
                    simulation_result,
                    {"property_number": property_number, "station": station},
                    drive, folder_id
                )
                if excel_file_id:
                    print(f"Excel保存完了: {excel_file_id}")
                comprehensive_data['simulation_result'] = simulation_result
            else:
                print("シミュレーションスキップ（データ不足）")
        except Exception as sim_e:
            print(f"シミュレーションエラー: {sim_e}")
            import traceback
            traceback.print_exc()

        # レポート生成
        docs_service = get_docs_service()
        gmaps_client = get_gmaps_client()
        report_doc_id = generate_property_evaluation_report(
            drive_service=drive,
            docs_service=docs_service,
            gmaps_client=gmaps_client,
            gemini_client=gemini_client,
            folder_id=folder_id,
            pdf_file_id=target['id'],
            property_number=property_number,
            station=station,
            extracted_text=extracted_text,
            detailed_data=comprehensive_data,
        )

        result = {
            "status": "success",
            "folder": folder_name,
            "property_number": property_number,
            "station": station,
            "target_file": target['name'],
            "is_hanbaizumen": is_sales,
            "data_fields": len(comprehensive_data),
            "simulation": simulation_result['decision']['recommendation'] if simulation_result else "スキップ",
            "excel_file_id": excel_file_id,
            "report_doc_id": report_doc_id,
        }
        print(f"テスト完了: {result}")
        return jsonify(result)

    except Exception as e:
        import traceback
        print(f"テストエラー: {e}")
        print(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/test/list', methods=['GET'])
def test_list_folders():
    """投資フォルダ内の物件フォルダ一覧を取得"""
    try:
        drive = get_drive_service()
        investment_folder_id = get_secret("INVESTMENT_FOLDER_ID")

        query = f"'{investment_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = drive.files().list(
            q=query, fields='files(id, name)', orderBy='name desc', pageSize=20
        ).execute()
        folders = results.get('files', [])

        folder_list = []
        for folder in folders:
            fquery = f"'{folder['id']}' in parents and trashed=false"
            files = drive.files().list(q=fquery, fields='files(mimeType)', pageSize=50).execute().get('files', [])
            file_types = {}
            for f in files:
                mt = f['mimeType'].split('/')[-1]
                file_types[mt] = file_types.get(mt, 0) + 1
            folder_list.append({
                "id": folder['id'],
                "name": folder['name'],
                "files": file_types,
            })

        return jsonify({"status": "success", "folders": folder_list})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
