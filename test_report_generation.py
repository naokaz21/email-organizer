#!/usr/bin/env python3
"""
物件評価レポート生成機能のテストスクリプト

指定されたGoogle DriveフォルダからPDFを取得し、
評価レポートを生成します。
"""

import os
import sys
from main import (
    get_drive_service,
    get_docs_service,
    get_gmaps_client,
    get_gemini_client,
    generate_property_evaluation_report
)

def list_pdfs_in_folder(drive_service, folder_id):
    """フォルダ内のPDFファイル一覧を取得"""
    query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false"
    results = drive_service.files().list(
        q=query,
        fields='files(id, name, parents)',
        pageSize=100
    ).execute()
    return results.get('files', [])

def extract_property_info_from_filename(filename):
    """ファイル名から物件情報を抽出（簡易版）"""
    # 例: "20250213_渋谷_12345_Hanbaizumen.pdf"
    parts = filename.replace('.pdf', '').split('_')

    property_number = "TEST"
    station = "テスト駅"

    # ファイル名から駅名と物件番号を抽出試行
    if len(parts) >= 3:
        station = parts[1] if parts[1] else "不明駅"
        property_number = parts[2] if parts[2] else "不明"

    return property_number, station

def test_report_generation(test_folder_id, limit=None):
    """レポート生成テスト実行"""

    print("=" * 60)
    print("物件評価レポート生成テスト")
    print("=" * 60)
    print()

    # APIクライアント初期化
    print("📡 APIクライアント初期化中...")
    drive_service = get_drive_service()
    docs_service = get_docs_service()
    gmaps_client = get_gmaps_client()
    gemini_client = get_gemini_client()
    print("✅ 初期化完了")
    print()

    # フォルダ内のPDF取得
    print(f"📂 フォルダ内のPDF検索中: {test_folder_id}")
    pdf_files = list_pdfs_in_folder(drive_service, test_folder_id)
    print(f"✅ {len(pdf_files)}件のPDFを発見")
    print()

    if not pdf_files:
        print("⚠️  PDFファイルが見つかりませんでした")
        return

    # 処理件数制限
    if limit:
        pdf_files = pdf_files[:limit]
        print(f"⚠️  テストのため最初の{limit}件のみ処理します")
        print()

    # 各PDFを処理
    success_count = 0
    for i, pdf_file in enumerate(pdf_files, 1):
        print(f"[{i}/{len(pdf_files)}] 処理中: {pdf_file['name']}")
        print("-" * 60)

        # ファイル名から物件情報抽出
        property_number, station = extract_property_info_from_filename(pdf_file['name'])

        print(f"  物件番号: {property_number}")
        print(f"  駅名: {station}")
        print(f"  PDF ID: {pdf_file['id']}")

        try:
            # レポート生成実行
            report_doc_id = generate_property_evaluation_report(
                drive_service=drive_service,
                docs_service=docs_service,
                gmaps_client=gmaps_client,
                gemini_client=gemini_client,
                folder_id=test_folder_id,  # 同じフォルダに保存
                pdf_file_id=pdf_file['id'],
                property_number=property_number,
                station=station
            )

            if report_doc_id:
                print(f"  ✅ レポート生成成功")
                print(f"  📄 Doc ID: {report_doc_id}")
                print(f"  🔗 URL: https://docs.google.com/document/d/{report_doc_id}/edit")
                success_count += 1
            else:
                print(f"  ❌ レポート生成失敗")

        except Exception as e:
            print(f"  ❌ エラー: {e}")
            import traceback
            traceback.print_exc()

        print()

    # サマリー
    print("=" * 60)
    print(f"テスト完了: {success_count}/{len(pdf_files)} 件成功")
    print("=" * 60)

if __name__ == '__main__':
    # テスト用フォルダID（デフォルト）
    TEST_FOLDER_ID = "1pIBpjP0aCY-R9Zs7NUgyHwnw2lSTy1ya"

    # コマンドライン引数でフォルダIDを指定可能
    if len(sys.argv) > 1:
        TEST_FOLDER_ID = sys.argv[1]

    # 処理件数制限（テスト時は1件のみ推奨）
    LIMIT = 1 if len(sys.argv) <= 2 else int(sys.argv[2])

    print(f"テストフォルダID: {TEST_FOLDER_ID}")
    print(f"処理件数上限: {LIMIT}件")
    print()

    test_report_generation(TEST_FOLDER_ID, limit=LIMIT)
