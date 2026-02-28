"""
Email Organizer - Cloud Run Service
Gmailから販売図面・住宅地図メールを取得し、Google Driveに自動整理
"""

import os
import re
import threading
import time
from flask import Flask, request, jsonify
from google.cloud import secretmanager
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as AuthRequest
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

def _read_secret(secret_name):
    """環境変数から読み取り、なければSecret Manager APIにフォールバック"""
    val = os.environ.get(secret_name)
    if val:
        return val
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = secret_client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# ============================================================
# OAuth Credential Cache（スレッドセーフ）
# ============================================================
_creds_lock = threading.Lock()
_cached_creds: Optional[Credentials] = None
_cached_creds_expiry: float = 0

ALL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.labels",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]

def get_credentials() -> Credentials:
    """共有キャッシュ付きCredentialsを返す。スレッドセーフ。"""
    global _cached_creds, _cached_creds_expiry

    with _creds_lock:
        now = time.time()
        # キャッシュが有効なら（期限5分前まで）そのまま返す
        if _cached_creds and _cached_creds.token and now < (_cached_creds_expiry - 300):
            return _cached_creds

        # 初回 or キャッシュ無効時: Credentialsオブジェクトを作成
        if _cached_creds is None:
            _cached_creds = Credentials(
                token=None,
                refresh_token=_read_secret("GMAIL_REFRESH_TOKEN"),
                token_uri="https://oauth2.googleapis.com/token",
                client_id=_read_secret("GMAIL_CLIENT_ID"),
                client_secret=_read_secret("GMAIL_CLIENT_SECRET"),
                scopes=ALL_SCOPES,
            )

        # Access tokenをリフレッシュ
        try:
            _cached_creds.refresh(AuthRequest())
            _cached_creds_expiry = now + 3600
            print(f"OAuth token refreshed, expires in ~60 min")
        except Exception as e:
            print(f"ERROR: OAuth token refresh failed: {e}")
            _cached_creds = None
            _cached_creds_expiry = 0
            raise RuntimeError(
                f"OAuth token refresh failed. "
                f"POST /refresh-token で再試行するか、get_refresh_token.py を再実行してください。"
                f"Error: {e}"
            )

        return _cached_creds

def invalidate_credentials():
    """キャッシュを無効化し、次回呼び出し時に再作成させる"""
    global _cached_creds, _cached_creds_expiry
    with _creds_lock:
        _cached_creds = None
        _cached_creds_expiry = 0
        print("Credentials cache invalidated")

def get_gmail_service():
    """Gmail APIサービスを取得（cached credentials）"""
    return build('gmail', 'v1', credentials=get_credentials())

def get_drive_service():
    """Drive APIサービスを取得（cached credentials）"""
    return build('drive', 'v3', credentials=get_credentials())

def get_docs_service():
    """Docs APIサービスを取得（cached credentials）"""
    return build('docs', 'v1', credentials=get_credentials())

def get_gmaps_client():
    """Google Maps APIクライアントを取得"""
    api_key = _read_secret("GOOGLE_MAPS_API_KEY")
    return googlemaps.Client(key=api_key)

def get_gemini_client():
    """Gemini APIクライアントを取得"""
    api_key = _read_secret("GEMINI_API_KEY")
    genai.configure(api_key=api_key)
    # Gemini 2.5 Flash (2026年現在の推奨モデル、1.5は廃止済み)
    return genai.GenerativeModel('gemini-2.5-flash')

def get_perplexity_client():
    """Perplexity APIクライアントを取得（OpenAI互換）"""
    try:
        # PERPLEXITY_API_KEYはオプション（なければフリー層で動作）
        try:
            api_key = _read_secret("PERPLEXITY_API_KEY")
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

プレーンテキストで出力してください。マークダウン記法（#、##、###、**、*、```等）は一切使わないでください。
見出しには番号を付けて区別してください（例: 「1. 周辺エリアの特徴」）。
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

def research_area_with_gemini_search(location: dict, property_info: dict, gemini_client) -> dict:
    """Gemini Web Search（Google Search grounding）でエリア調査"""
    try:
        prompt = f"""
あなたは不動産投資エリア分析の専門家です。以下の物件エリアについてWeb検索で最新情報を調査してください。

物件情報:
- 住所: {location['formatted_address']}
- 緯度経度: {location['lat']}, {location['lng']}
- 駅: {property_info.get('station', '不明')}

以下の5つの観点で調査してください:

1. 最寄駅情報
  - 最寄駅（{property_info.get('station', '不明')}駅）の1日あたりの乗降客数（最新データ）
  - 過去5年の乗降客数推移
  - 利用可能な路線名

2. 路線価
  - 物件所在地（{location['formatted_address']}）付近の路線価（最新年度）
  - 過去5年の路線価推移（上昇/下降トレンド）

3. 人口動態
  - 過去10年の人口推移
  - 単身世帯比率
  - 年齢構成（特に賃貸需要層）
  - 将来予測

4. ハザードマップ
  - 洪水リスク（浸水想定区域）
  - 地震リスク（液状化、活断層）
  - 土砂災害リスク

5. 再開発計画
  - 周辺の大規模開発プロジェクト
  - 新駅・路線延伸計画
  - 商業施設・インフラ整備

重要: 可能な限り出典URLを記載してください。最新情報を優先してください。
プレーンテキストで出力してください。マークダウン記法（#、##、###、**、*、```等）は一切使わないでください。
見出しには番号を付けて区別してください（例: 「1. 最寄駅情報」）。
"""

        from google.generativeai.types import content_types
        response = gemini_client.generate_content(
            prompt,
            tools='google_search_retrieval'
        )

        report_text = response.text

        return {
            'status': 'success',
            'report': report_text,
            'model': 'gemini-2.5-flash-google-search'
        }

    except Exception as e:
        print(f"Gemini Web Searchエリア調査エラー: {e}")
        import traceback
        traceback.print_exc()
        return {
            'status': 'error',
            'error': str(e),
            'report': 'エリア調査に失敗しました。'
        }

def _strip_markdown(text: str) -> str:
    """Markdown記法をプレーンテキストに変換"""
    # 見出し記号を除去 (### heading → heading)
    text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
    # 太字/斜体を除去 (**text** → text, *text* → text)
    text = re.sub(r'\*{1,3}(.+?)\*{1,3}', r'\1', text)
    # コードブロックを除去
    text = re.sub(r'```[\s\S]*?```', '', text)
    # インラインコードを除去
    text = re.sub(r'`([^`]+)`', r'\1', text)
    # リンク [text](url) → text (url)
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'\1 (\2)', text)
    # 水平線 --- を除去
    text = re.sub(r'^-{3,}$', '', text, flags=re.MULTILINE)
    # 連続空行を1行に
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def combine_research_reports(gemini_market_report: dict, area_report: dict) -> str:
    """Gemini市場調査とエリア調査を統合"""
    combined_parts = []

    # Gemini市場調査
    if gemini_market_report.get('status') == 'success':
        combined_parts.append("【市場調査】")
        combined_parts.append(gemini_market_report.get('report', ''))
    else:
        combined_parts.append("【市場調査】")
        combined_parts.append("市場調査に失敗しました。")

    combined_parts.append("")

    # エリア調査（Gemini Web Search）
    if area_report.get('status') == 'success':
        combined_parts.append("【エリア分析】")
        combined_parts.append(area_report.get('report', ''))
    else:
        combined_parts.append("【エリア分析】")
        combined_parts.append("エリア分析をスキップしました。")

    return _strip_markdown("\n".join(combined_parts))

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


# デザイン定数（McKinsey/BCG品質）
_NAVY = {'red': 0.11, 'green': 0.18, 'blue': 0.33}      # #1C2E54 ダークネイビー
_LIGHT_NAVY = {'red': 0.22, 'green': 0.33, 'blue': 0.53}  # #385487
_HEADER_BG = {'red': 0.11, 'green': 0.18, 'blue': 0.33}   # テーブルヘッダー背景
_HEADER_TEXT = {'red': 1.0, 'green': 1.0, 'blue': 1.0}     # 白文字
_ALT_ROW_BG = {'red': 0.95, 'green': 0.96, 'blue': 0.98}   # #F2F5FA 交互行
_BORDER_COLOR = {'red': 0.80, 'green': 0.82, 'blue': 0.86}  # #CCD1DB 薄いグレー
_ACCENT = {'red': 0.16, 'green': 0.50, 'blue': 0.73}       # #2980BA アクセント青


def _rgb(color_dict):
    return {'color': {'rgbColor': color_dict}}


def _insert_table_at_placeholder(docs_service, doc_id, placeholder, rows_data, col_count):
    """プレースホルダーをスタイル付きテーブルに置換"""
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

    # ドキュメント再取得してテーブル構造を取得
    doc = docs_service.documents().get(documentId=doc_id).execute()
    table_element = None
    table_start_index = None
    for element in doc['body']['content']:
        if 'table' in element and element['startIndex'] >= start:
            table_element = element
            table_start_index = element['startIndex']
            break

    if not table_element:
        print(f"テーブル未検出: {placeholder}")
        return

    table = table_element['table']

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
            documentId=doc_id, body={'requests': cell_requests}
        ).execute()

    # === テーブルスタイリング ===
    style_requests = []

    # ヘッダー行: ネイビー背景 + 白太字
    style_requests.append({
        'updateTableCellStyle': {
            'tableCellStyle': {
                'backgroundColor': _rgb(_HEADER_BG),
                'paddingTop': {'magnitude': 5, 'unit': 'PT'},
                'paddingBottom': {'magnitude': 5, 'unit': 'PT'},
                'paddingLeft': {'magnitude': 7, 'unit': 'PT'},
                'paddingRight': {'magnitude': 7, 'unit': 'PT'},
            },
            'fields': 'backgroundColor,paddingTop,paddingBottom,paddingLeft,paddingRight',
            'tableRange': {
                'tableCellLocation': {'tableStartLocation': {'index': table_start_index}, 'rowIndex': 0, 'columnIndex': 0},
                'rowSpan': 1, 'columnSpan': col_count
            }
        }
    })

    # データ行: パディング + 交互背景色
    for r in range(1, row_count):
        bg = _ALT_ROW_BG if r % 2 == 0 else {'red': 1.0, 'green': 1.0, 'blue': 1.0}
        style_requests.append({
            'updateTableCellStyle': {
                'tableCellStyle': {
                    'backgroundColor': _rgb(bg),
                    'paddingTop': {'magnitude': 4, 'unit': 'PT'},
                    'paddingBottom': {'magnitude': 4, 'unit': 'PT'},
                    'paddingLeft': {'magnitude': 7, 'unit': 'PT'},
                    'paddingRight': {'magnitude': 7, 'unit': 'PT'},
                },
                'fields': 'backgroundColor,paddingTop,paddingBottom,paddingLeft,paddingRight',
                'tableRange': {
                    'tableCellLocation': {'tableStartLocation': {'index': table_start_index}, 'rowIndex': r, 'columnIndex': 0},
                    'rowSpan': 1, 'columnSpan': col_count
                }
            }
        })

    # 全セルのボーダー: 薄いグレー
    style_requests.append({
        'updateTableCellStyle': {
            'tableCellStyle': {
                'borderTop': {'color': _rgb(_BORDER_COLOR), 'width': {'magnitude': 0.5, 'unit': 'PT'}, 'dashStyle': 'SOLID'},
                'borderBottom': {'color': _rgb(_BORDER_COLOR), 'width': {'magnitude': 0.5, 'unit': 'PT'}, 'dashStyle': 'SOLID'},
                'borderLeft': {'color': _rgb(_BORDER_COLOR), 'width': {'magnitude': 0.5, 'unit': 'PT'}, 'dashStyle': 'SOLID'},
                'borderRight': {'color': _rgb(_BORDER_COLOR), 'width': {'magnitude': 0.5, 'unit': 'PT'}, 'dashStyle': 'SOLID'},
            },
            'fields': 'borderTop,borderBottom,borderLeft,borderRight',
            'tableRange': {
                'tableCellLocation': {'tableStartLocation': {'index': table_start_index}, 'rowIndex': 0, 'columnIndex': 0},
                'rowSpan': row_count, 'columnSpan': col_count
            }
        }
    })

    if style_requests:
        try:
            docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': style_requests}).execute()
        except Exception as e:
            print(f"テーブルスタイル適用エラー（無視）: {e}")

    # ドキュメント再取得（テキスト挿入でインデックスが変わったため）
    doc = docs_service.documents().get(documentId=doc_id).execute()
    table_element = None
    for element in doc['body']['content']:
        if 'table' in element and element['startIndex'] >= start:
            table_element = element
            break

    if not table_element:
        return

    table = table_element['table']

    # ヘッダー行テキスト: 白・太字・フォント設定
    text_requests = []
    header_row = table['tableRows'][0]
    for c in range(col_count):
        cell = header_row['tableCells'][c]
        el = cell['content'][0]['paragraph']['elements'][0]
        cs = el['startIndex']
        ce = el.get('endIndex', cs)
        if ce > cs:
            text_requests.append({
                'updateTextStyle': {
                    'range': {'startIndex': cs, 'endIndex': ce - 1},
                    'textStyle': {
                        'bold': True,
                        'foregroundColor': _rgb(_HEADER_TEXT),
                        'fontSize': {'magnitude': 9, 'unit': 'PT'},
                    },
                    'fields': 'bold,foregroundColor,fontSize'
                }
            })

    # データ行テキスト: フォントサイズ統一
    for r in range(1, row_count):
        row = table['tableRows'][r]
        for c in range(col_count):
            cell = row['tableCells'][c]
            el = cell['content'][0]['paragraph']['elements'][0]
            cs = el['startIndex']
            ce = el.get('endIndex', cs)
            if ce > cs:
                text_requests.append({
                    'updateTextStyle': {
                        'range': {'startIndex': cs, 'endIndex': ce - 1},
                        'textStyle': {
                            'fontSize': {'magnitude': 9, 'unit': 'PT'},
                        },
                        'fields': 'fontSize'
                    }
                })

    if text_requests:
        try:
            docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': text_requests}).execute()
        except Exception:
            pass


def _insert_map_image(docs_service, drive_service, doc_id, location):
    """地図画像をDrive経由でプレースホルダー位置に挿入"""
    try:
        import requests as req
        from googleapiclient.http import MediaIoBaseUpload

        start, end = _find_placeholder_range(docs_service, doc_id, '{{MAP_IMAGE}}')
        if start is None:
            print("地図プレースホルダー未検出")
            return

        # プレースホルダー削除
        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': [{'deleteContentRange': {'range': {'startIndex': start, 'endIndex': end}}}]}
        ).execute()

        lat, lng = location['lat'], location['lng']
        api_key = _read_secret("GOOGLE_MAPS_API_KEY")

        # Google Maps Static API で画像ダウンロード
        map_url = (
            f"https://maps.googleapis.com/maps/api/staticmap"
            f"?center={lat},{lng}&zoom=15&size=600x400&scale=2&maptype=roadmap"
            f"&markers=color:red%7C{lat},{lng}"
            f"&key={api_key}"
        )
        resp = req.get(map_url, timeout=15)
        if resp.status_code != 200:
            print(f"地図画像ダウンロード失敗: HTTP {resp.status_code}")
            return

        # Driveにアップロード
        image_data = io.BytesIO(resp.content)
        media = MediaIoBaseUpload(image_data, mimetype='image/png', resumable=False)
        map_file = drive_service.files().create(
            body={'name': 'map_temp.png', 'mimeType': 'image/png'},
            media_body=media, fields='id'
        ).execute()
        map_file_id = map_file['id']

        # 公開URLを設定（anyone can view）
        drive_service.permissions().create(
            fileId=map_file_id,
            body={'type': 'anyone', 'role': 'reader'}
        ).execute()
        image_url = f"https://drive.google.com/uc?id={map_file_id}"

        # Google Mapsリンク
        maps_link = f"https://www.google.com/maps?q={lat},{lng}"

        # 画像挿入
        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': [{
                'insertInlineImage': {
                    'uri': image_url,
                    'location': {'index': start},
                    'objectSize': {
                        'width': {'magnitude': 450, 'unit': 'PT'},
                        'height': {'magnitude': 300, 'unit': 'PT'},
                    }
                }
            }]}
        ).execute()

        # 画像の後にリンクテキストを追加
        link_text = f"\nGoogle Mapsで開く\n"
        link_index = start + 1

        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': [
                {'insertText': {'location': {'index': link_index}, 'text': link_text}},
                {'updateTextStyle': {
                    'range': {'startIndex': link_index + 1, 'endIndex': link_index + 1 + len("Google Mapsで開く")},
                    'textStyle': {
                        'link': {'url': maps_link},
                        'foregroundColor': _rgb(_ACCENT),
                        'fontSize': {'magnitude': 9, 'unit': 'PT'},
                    },
                    'fields': 'link,foregroundColor,fontSize'
                }}
            ]}
        ).execute()

        print(f"地図画像挿入完了")

    except Exception as e:
        print(f"地図画像挿入エラー（無視）: {e}")
        import traceback
        traceback.print_exc()


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
        sections.append((f"調査日：{now}", 'SUBTITLE'))

        # A1. 物件概要
        sections.append(("A1. 物件概要", 'HEADING_1'))
        sections.append(("基本情報", 'HEADING_2'))
        sections.append(("{{TABLE_BASIC_INFO}}", 'NORMAL_TEXT'))

        # 地図
        location = report_data.get('location')
        if location and location.get('lat') and location.get('lng'):
            sections.append(("所在地マップ", 'HEADING_2'))
            sections.append(("{{MAP_IMAGE}}", 'NORMAL_TEXT'))

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

        # スタイル適用（段落スタイル + テキストスタイル）
        style_requests = []
        text_style_requests = []
        idx = 1
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
            idx = end_idx + 1

        if style_requests:
            docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': style_requests}).execute()

        # カスタムカラー・フォント適用
        idx = 1
        for text, style in sections:
            end_idx = idx + len(text)
            if style == 'SUBTITLE':
                text_style_requests.append({
                    'updateTextStyle': {
                        'range': {'startIndex': idx, 'endIndex': end_idx},
                        'textStyle': {
                            'foregroundColor': _rgb({'red': 0.45, 'green': 0.45, 'blue': 0.45}),
                            'fontSize': {'magnitude': 11, 'unit': 'PT'},
                        },
                        'fields': 'foregroundColor,fontSize'
                    }
                })
                text_style_requests.append({
                    'updateParagraphStyle': {
                        'range': {'startIndex': idx, 'endIndex': end_idx},
                        'paragraphStyle': {
                            'spaceBelow': {'magnitude': 16, 'unit': 'PT'},
                            'borderBottom': {
                                'color': _rgb(_BORDER_COLOR),
                                'width': {'magnitude': 0.5, 'unit': 'PT'},
                                'padding': {'magnitude': 8, 'unit': 'PT'},
                                'dashStyle': 'SOLID',
                            },
                        },
                        'fields': 'spaceBelow,borderBottom'
                    }
                })
            elif style == 'TITLE':
                text_style_requests.append({
                    'updateTextStyle': {
                        'range': {'startIndex': idx, 'endIndex': end_idx},
                        'textStyle': {
                            'foregroundColor': _rgb(_NAVY),
                            'fontSize': {'magnitude': 22, 'unit': 'PT'},
                            'bold': True,
                        },
                        'fields': 'foregroundColor,fontSize,bold'
                    }
                })
            elif style == 'HEADING_1':
                text_style_requests.append({
                    'updateTextStyle': {
                        'range': {'startIndex': idx, 'endIndex': end_idx},
                        'textStyle': {
                            'foregroundColor': _rgb(_NAVY),
                            'fontSize': {'magnitude': 16, 'unit': 'PT'},
                            'bold': True,
                        },
                        'fields': 'foregroundColor,fontSize,bold'
                    }
                })
                # HEADING_1の下に罫線風のスペーシング
                text_style_requests.append({
                    'updateParagraphStyle': {
                        'range': {'startIndex': idx, 'endIndex': end_idx},
                        'paragraphStyle': {
                            'borderBottom': {
                                'color': _rgb(_NAVY),
                                'width': {'magnitude': 1.5, 'unit': 'PT'},
                                'padding': {'magnitude': 6, 'unit': 'PT'},
                                'dashStyle': 'SOLID',
                            },
                            'spaceBelow': {'magnitude': 10, 'unit': 'PT'},
                            'spaceAbove': {'magnitude': 18, 'unit': 'PT'},
                        },
                        'fields': 'borderBottom,spaceBelow,spaceAbove'
                    }
                })
            elif style == 'HEADING_2':
                text_style_requests.append({
                    'updateTextStyle': {
                        'range': {'startIndex': idx, 'endIndex': end_idx},
                        'textStyle': {
                            'foregroundColor': _rgb(_LIGHT_NAVY),
                            'fontSize': {'magnitude': 12, 'unit': 'PT'},
                            'bold': True,
                        },
                        'fields': 'foregroundColor,fontSize,bold'
                    }
                })
                text_style_requests.append({
                    'updateParagraphStyle': {
                        'range': {'startIndex': idx, 'endIndex': end_idx},
                        'paragraphStyle': {
                            'spaceBelow': {'magnitude': 6, 'unit': 'PT'},
                            'spaceAbove': {'magnitude': 12, 'unit': 'PT'},
                        },
                        'fields': 'spaceBelow,spaceAbove'
                    }
                })
            elif style == 'NORMAL_TEXT' and text and not text.startswith('{{'):
                text_style_requests.append({
                    'updateTextStyle': {
                        'range': {'startIndex': idx, 'endIndex': end_idx},
                        'textStyle': {
                            'fontSize': {'magnitude': 10, 'unit': 'PT'},
                        },
                        'fields': 'fontSize'
                    }
                })
            idx = end_idx + 1

        if text_style_requests:
            try:
                docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': text_style_requests}).execute()
            except Exception as e:
                print(f"テキストスタイル適用エラー（無視）: {e}")

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
        # 住所: detailed_data（Gemini抽出）を優先、fallbackでgeocode結果
        address_display = detailed.get('address') or report_data.get('address', '不明')
        basic_rows.append(["所在地", address_display])
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
        if location and location.get('lat'):
            maps_url = f"https://www.google.com/maps?q={location['lat']},{location['lng']}"
            basic_rows.append(["Google Maps", maps_url])

        _insert_table_at_placeholder(docs_service, doc_id, '{{TABLE_BASIC_INFO}}', basic_rows, 2)

        # 地図画像挿入
        if location and location.get('lat') and location.get('lng'):
            _insert_map_image(docs_service, drive_service, doc_id, location)

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

        # 5.5. エリア調査（Gemini Web Search）
        area_data = research_area_with_gemini_search(location, property_info, gemini_client)
        print(f"エリア調査完了: {area_data['status']}")

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

    investment_folder_id = _read_secret("INVESTMENT_FOLDER_ID")
    label_name = _read_secret("PROCESSED_LABEL_NAME")
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

@app.route('/auth-status', methods=['GET'])
def auth_status():
    """OAuth認証状態の確認"""
    try:
        status = {
            "has_cached_creds": _cached_creds is not None,
            "has_token": _cached_creds.token is not None if _cached_creds else False,
            "token_expiry": _cached_creds_expiry,
            "seconds_until_expiry": max(0, _cached_creds_expiry - time.time()) if _cached_creds_expiry else 0,
            "env_vars_present": {
                "GMAIL_CLIENT_ID": bool(os.environ.get("GMAIL_CLIENT_ID")),
                "GMAIL_CLIENT_SECRET": bool(os.environ.get("GMAIL_CLIENT_SECRET")),
                "GMAIL_REFRESH_TOKEN": bool(os.environ.get("GMAIL_REFRESH_TOKEN")),
            }
        }
        # Gmail APIで疎通確認
        try:
            creds = get_credentials()
            gmail = build('gmail', 'v1', credentials=creds)
            profile = gmail.users().getProfile(userId='me').execute()
            status["gmail_email"] = profile.get("emailAddress")
            status["gmail_ok"] = True
        except Exception as e:
            status["gmail_ok"] = False
            status["gmail_error"] = str(e)

        return jsonify(status)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/refresh-token', methods=['POST'])
def refresh_token_endpoint():
    """緊急用: OAuthトークンの強制リフレッシュ。
    新しいrefresh_tokenをJSONで渡すことも可能。"""
    try:
        data = request.get_json(silent=True) or {}
        new_token = data.get('refresh_token')

        if new_token:
            global _cached_creds, _cached_creds_expiry
            with _creds_lock:
                _cached_creds = Credentials(
                    token=None,
                    refresh_token=new_token,
                    token_uri="https://oauth2.googleapis.com/token",
                    client_id=_read_secret("GMAIL_CLIENT_ID"),
                    client_secret=_read_secret("GMAIL_CLIENT_SECRET"),
                    scopes=ALL_SCOPES,
                )
                _cached_creds_expiry = 0
            print("Refresh token updated in memory")
        else:
            invalidate_credentials()

        creds = get_credentials()
        return jsonify({
            "status": "success",
            "message": "Credentials refreshed successfully",
            "token_valid": creds.token is not None,
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "message": str(e),
            "fix": "get_refresh_token.py を再実行して新しいrefresh tokenを取得し、"
                   "Secret Managerを更新してください。"
        }), 500

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
        investment_folder_id = _read_secret("INVESTMENT_FOLDER_ID")

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
