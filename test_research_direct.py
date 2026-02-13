#!/usr/bin/env python3
"""リサーチ＆レポート生成の直接テスト"""

import sys
from main import (
    get_gmaps_client,
    get_gemini_client,
    get_perplexity_client,
    get_drive_service,
    get_docs_service,
    geocode_address,
    research_market_price,
    research_area_with_perplexity,
    combine_research_reports,
    create_evaluation_report
)

def test_research_and_report():
    """テスト物件でリサーチ＆レポート生成をテスト"""

    print("=" * 60)
    print("リサーチ＆レポート生成テスト")
    print("=" * 60)
    print()

    # テスト物件データ
    test_property = {
        'property_number': 'TEST001',
        'station': '練馬',
        'address': '東京都練馬区練馬1-1-1',
        'price': 15000,
        'structure': 'RC造',
        'year_built': '2020年3月',
        'land_area': 120.5,
        'building_area': 350.8,
        'total_units': 8,
        'full_occupancy_rent': 480000,
        'floor_plan': '1K',
        'management_fee': 5000,
        'reserve_fund': 3000,
        'rent_roll': [
            {'room_number': '101', 'rent': 60000, 'status': '入居中'},
            {'room_number': '102', 'rent': 60000, 'status': '入居中'},
            {'room_number': '201', 'rent': 60000, 'status': '空室'},
            {'room_number': '202', 'rent': 60000, 'status': '入居中'},
        ]
    }

    try:
        # 1. クライアント初期化
        print("1️⃣  APIクライアント初期化中...")
        gmaps = get_gmaps_client()
        gemini = get_gemini_client()
        perplexity = get_perplexity_client()
        drive = get_drive_service()
        docs = get_docs_service()
        print("✅ 初期化完了")
        print()

        # 2. 位置情報取得
        print(f"2️⃣  位置情報取得中: {test_property['address']}")
        location = geocode_address(test_property['address'], gmaps)
        if not location:
            print("❌ Geocodingエラー")
            return
        print(f"✅ 位置情報: {location['lat']}, {location['lng']}")
        print()

        # 3. Gemini市場調査
        print("3️⃣  Gemini市場調査中...")
        property_info = {
            'property_number': test_property['property_number'],
            'station': test_property['station']
        }
        market_data = research_market_price(location, property_info, gemini)
        print(f"✅ 市場調査: {market_data['status']}")
        print(f"   レポート長: {len(market_data.get('report', ''))} 文字")
        print()

        # 4. Perplexityエリア調査
        print("4️⃣  Perplexityエリア調査中...")
        if perplexity:
            area_data = research_area_with_perplexity(location, property_info, perplexity)
            print(f"✅ エリア調査: {area_data['status']}")
            print(f"   レポート長: {len(area_data.get('report', ''))} 文字")
        else:
            area_data = {
                'status': 'error',
                'report': 'Perplexity APIクライアント未設定'
            }
            print("⚠️  Perplexityスキップ（クライアント未設定）")
        print()

        # 5. レポート統合
        print("5️⃣  レポート統合中...")
        combined_report = combine_research_reports(market_data, area_data)
        print(f"✅ 統合完了: {len(combined_report)} 文字")
        print()

        # 6. Google Docsレポート作成
        print("6️⃣  Google Docsレポート作成中...")

        # テスト用フォルダID（投資物件フォルダ）
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        project_id = "project-3255e657-b52f-4d63-ae7"
        name = f"projects/{project_id}/secrets/INVESTMENT_FOLDER_ID/versions/latest"
        response = client.access_secret_version(request={"name": name})
        folder_id = response.payload.data.decode('UTF-8')

        report_data = {
            'property_number': test_property['property_number'],
            'station': test_property['station'],
            'address': location['formatted_address'],
            'location': location,
            'market_report': combined_report,
            'detailed_data': test_property
        }

        doc_id = create_evaluation_report(docs, drive, folder_id, report_data)

        if doc_id:
            print(f"✅ レポート作成成功!")
            print(f"   Doc ID: {doc_id}")
            print(f"   URL: https://docs.google.com/document/d/{doc_id}/edit")
            print()
            print("🎉 すべてのテスト成功!")
            return doc_id
        else:
            print("❌ レポート作成失敗")
            return None

    except Exception as e:
        print(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == '__main__':
    doc_id = test_research_and_report()
    sys.exit(0 if doc_id else 1)
