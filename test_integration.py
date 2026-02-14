#!/usr/bin/env python3
"""
統合テスト: 既存の物件フォルダからPDFを取得し、
シミュレーション + Excel出力 + レポート生成をテスト

メール受信部分をスキップし、Drive上の既存PDFを直接処理する。

使い方:
  python3 test_integration.py                    # 投資フォルダ内のサブフォルダ一覧表示
  python3 test_integration.py <folder_id>        # 指定フォルダ内のPDFでテスト実行
"""

import os
import sys
import io
import base64

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(__file__))

from main import (
    get_drive_service,
    get_docs_service,
    get_gmaps_client,
    get_gemini_client,
    get_perplexity_client,
    get_secret,
    extract_text_from_pdf,
    extract_text_from_image,
    extract_comprehensive_property_data,
    is_hanbaizumen,
    generate_property_evaluation_report,
)
from simulation import run_simulation, create_simulation_excel, format_simulation_summary_for_report


def list_property_folders(drive_service, investment_folder_id):
    """投資フォルダ内のサブフォルダ（物件フォルダ）一覧を取得"""
    query = f"'{investment_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = drive_service.files().list(
        q=query,
        fields='files(id, name)',
        orderBy='name desc',
        pageSize=20
    ).execute()
    return results.get('files', [])


def list_files_in_folder(drive_service, folder_id):
    """フォルダ内の全ファイル一覧"""
    query = f"'{folder_id}' in parents and trashed=false"
    results = drive_service.files().list(
        q=query,
        fields='files(id, name, mimeType)',
        pageSize=50
    ).execute()
    return results.get('files', [])


def find_pdf_in_folder(drive_service, folder_id):
    """フォルダ内のPDF/画像ファイルを検索"""
    query = f"'{folder_id}' in parents and trashed=false and (mimeType='application/pdf' or mimeType contains 'image/')"
    results = drive_service.files().list(
        q=query,
        fields='files(id, name, mimeType)',
        pageSize=10
    ).execute()
    return results.get('files', [])


def download_file(drive_service, file_id):
    """Driveからファイルをダウンロード"""
    from googleapiclient.http import MediaIoBaseDownload
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return fh.getvalue()


def extract_info_from_folder_name(folder_name):
    """フォルダ名から物件情報を抽出: YYYYMMDD_駅名_物件番号"""
    parts = folder_name.split('_')
    if len(parts) >= 3:
        return parts[2], parts[1]  # property_number, station
    elif len(parts) == 2:
        return parts[1], parts[0]
    return folder_name, '不明'


def mode_list_folders(drive_service):
    """モード1: 投資フォルダ内のサブフォルダ一覧を表示"""
    investment_folder_id = get_secret("INVESTMENT_FOLDER_ID")

    print("📂 投資フォルダ内の物件フォルダ一覧")
    print("=" * 60)

    folders = list_property_folders(drive_service, investment_folder_id)

    if not folders:
        print("⚠️  物件フォルダが見つかりません")
        return

    for i, folder in enumerate(folders, 1):
        files = list_files_in_folder(drive_service, folder['id'])
        file_types = {}
        for f in files:
            mt = f['mimeType'].split('/')[-1]
            file_types[mt] = file_types.get(mt, 0) + 1

        type_str = ', '.join(f"{v}x {k}" for k, v in file_types.items())
        print(f"  [{i:2d}] {folder['name']}")
        print(f"       ID: {folder['id']}")
        print(f"       ファイル: {type_str or 'なし'}")
        print()

    print("テスト実行:")
    print(f"  python3 test_integration.py <folder_id>")


def mode_test_folder(drive_service, folder_id):
    """モード2: 指定フォルダ内のPDFでシミュレーション+レポート生成テスト"""

    print("=" * 60)
    print("統合テスト: シミュレーション + Excel出力 + レポート生成")
    print("=" * 60)
    print()

    # フォルダ情報取得
    folder_info = drive_service.files().get(fileId=folder_id, fields='name').execute()
    folder_name = folder_info['name']
    property_number, station = extract_info_from_folder_name(folder_name)

    print(f"📂 フォルダ: {folder_name}")
    print(f"   物件番号: {property_number}")
    print(f"   駅: {station}")
    print()

    # フォルダ内のファイル一覧
    files = list_files_in_folder(drive_service, folder_id)
    print(f"📁 フォルダ内ファイル ({len(files)}件):")
    for f in files:
        print(f"   - {f['name']} ({f['mimeType']})")
    print()

    # PDF/画像を探す
    target_files = find_pdf_in_folder(drive_service, folder_id)
    if not target_files:
        print("❌ PDF/画像ファイルが見つかりません")
        return

    # 最初のPDF/画像を使用
    target = target_files[0]
    print(f"🎯 テスト対象: {target['name']}")
    print()

    # ファイルダウンロード
    print("📥 ファイルダウンロード中...")
    file_data = download_file(drive_service, target['id'])
    print(f"   {len(file_data):,} bytes")
    print()

    # Geminiクライアント初期化
    gemini_client = get_gemini_client()

    # テキスト抽出
    print("📝 テキスト抽出中...")
    is_pdf = target['name'].lower().endswith('.pdf')
    if is_pdf:
        extracted_text = extract_text_from_pdf(file_data)
    else:
        extracted_text = extract_text_from_image(file_data, gemini_client)
    print(f"   {len(extracted_text)} 文字抽出")
    print()

    # 販売図面判定
    is_sales = is_hanbaizumen(extracted_text)
    print(f"📋 販売図面判定: {'✅ YES' if is_sales else '❌ NO'}")
    print()

    # 包括的データ抽出
    print("🔍 包括的物件データ抽出中（Gemini）...")
    comprehensive_data = extract_comprehensive_property_data(file_data, target['name'], gemini_client)
    print(f"   抽出フィールド数: {len(comprehensive_data)}")
    for key, val in comprehensive_data.items():
        if key != 'rent_roll':
            print(f"   {key}: {val}")
    if comprehensive_data.get('rent_roll'):
        print(f"   rent_roll: {len(comprehensive_data['rent_roll'])}部屋")
        for room in comprehensive_data['rent_roll']:
            print(f"     {room}")
    print()

    # ===== シミュレーション実行 =====
    print("=" * 60)
    print("💰 投資シミュレーション実行")
    print("=" * 60)

    simulation_result = run_simulation(comprehensive_data)

    if simulation_result:
        p = simulation_result['params']
        m = simulation_result['metrics']
        d = simulation_result['decision']

        print(f"\n--- パラメータ ---")
        print(f"  購入価格:    {p['purchase_price']:>15,.0f}円")
        print(f"  諸費用(8%):  {p['purchase_expenses']:>15,.0f}円")
        print(f"  総投資額:    {p['total_purchase_cost']:>15,.0f}円")
        print(f"  借入額:      {p['loan_amount']:>15,.0f}円")
        print(f"  自己資金:    {p['equity']:>15,.0f}円")
        print(f"  ADS:         {p['ads']:>15,.0f}円/年")

        print(f"\n--- 投資指標 ---")
        print(f"  表面利回り:  {m['gross_yield']:.2%}")
        print(f"  FCR:         {m['fcr']:.2%}")
        print(f"  K%:          {m['k_percent']:.2%}")
        print(f"  CCR:         {m['ccr']:.2%}")
        print(f"  レバレッジ:  {m['leverage']}")
        print(f"  DCR:         {m['dcr']:.2f}")
        print(f"  BER:         {m['ber']:.2%}")
        if m['irr'] is not None:
            print(f"  IRR:         {m['irr']:.2%}")
        if m['npv'] is not None:
            print(f"  NPV:         {m['npv']:,.0f}円")

        print(f"\n--- 投資判断 ---")
        for key, item in d['decisions'].items():
            status = "○" if item['pass'] else "×"
            print(f"  {status} {item['label']}: {item['detail']}")
        print(f"\n  📊 総合判定: {d['recommendation']}（{d['pass_count']}/{d['total_count']}）")

        # Excel出力
        print(f"\n--- Excel出力 ---")
        excel_file_id = create_simulation_excel(
            simulation_result,
            {"property_number": property_number, "station": station},
            drive_service,
            folder_id
        )
        if excel_file_id:
            print(f"  ✅ Excel保存完了: {excel_file_id}")
        else:
            print(f"  ❌ Excel保存失敗")

        # comprehensive_dataにシミュレーション結果を追加
        comprehensive_data['simulation_result'] = simulation_result
    else:
        print("  ⚠️ シミュレーションスキップ（データ不足）")

    # ===== レポート生成 =====
    print()
    print("=" * 60)
    print("📄 評価レポート生成")
    print("=" * 60)

    docs_service = get_docs_service()
    gmaps_client = get_gmaps_client()

    report_doc_id = generate_property_evaluation_report(
        drive_service=drive_service,
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

    if report_doc_id:
        print(f"\n  ✅ レポート生成完了")
        print(f"  📄 Doc ID: {report_doc_id}")
        print(f"  🔗 URL: https://docs.google.com/document/d/{report_doc_id}/edit")
    else:
        print(f"\n  ❌ レポート生成失敗")

    # サマリー
    print()
    print("=" * 60)
    print("テスト完了サマリー")
    print("=" * 60)
    print(f"  物件: {folder_name}")
    print(f"  データ抽出: {'✅' if comprehensive_data else '❌'}")
    print(f"  シミュレーション: {'✅' if simulation_result else '❌'}")
    print(f"  Excel出力: {'✅' if simulation_result and excel_file_id else '❌'}")
    print(f"  レポート: {'✅' if report_doc_id else '❌'}")


if __name__ == '__main__':
    # 環境変数設定（ローカル実行用）
    if not os.environ.get('GCP_PROJECT_ID'):
        os.environ['GCP_PROJECT_ID'] = 'project-3255e657-b52f-4d63-ae7'

    drive_service = get_drive_service()

    if len(sys.argv) > 1:
        # フォルダID指定: テスト実行
        mode_test_folder(drive_service, sys.argv[1])
    else:
        # 引数なし: フォルダ一覧表示
        mode_list_folders(drive_service)
