"""
投資シミュレーションモジュール
収益シミュレーション（GPI→EGI→NOI→BTCFo）、投資判断ロジック、Excel出力
"""

import math
import numpy as np
import numpy_financial as npf
from io import BytesIO
from datetime import datetime

# ===== デフォルト投資パラメータ =====
DEFAULT_LTV = 0.90                    # 借入割合 90%
DEFAULT_INTEREST_RATE = 0.0225        # 金利 2.25%
DEFAULT_LOAN_TERM_YEARS = 30          # 返済期間 30年
DEFAULT_VACANCY_RATE = 0.05           # 空室率 5%
DEFAULT_RENT_DECLINE_RATE = 0.005     # 賃料下落率 0.5%/年
DEFAULT_HOLDING_PERIOD = 10           # 保有期間 10年
DEFAULT_EXPECTED_RETURN = 0.02        # 期待収益率（割引率）2.0%
DEFAULT_PURCHASE_EXPENSE_RATE = 0.08  # 購入諸費用 8%
DEFAULT_OPEX_RATIO = 0.15            # 運営費率 GPI比15%
DEFAULT_EXIT_CAP_RATE_SPREAD = 0.005  # 出口Cap Rate = 入口 + 0.5%
DEFAULT_SALE_EXPENSE_RATE = 0.04      # 売却諸費用 4%

# ===== 投資判断基準 =====
THRESHOLD_DCR = 1.2
THRESHOLD_BER = 0.80


def validate_simulation_inputs(data):
    """シミュレーション入力データのバリデーション"""
    warnings = []

    price = data.get('price')
    rent = data.get('full_occupancy_rent')

    if not price or price <= 0:
        return False, ["購入価格（price）が未設定または不正"]
    if not rent or rent <= 0:
        return False, ["満室想定賃料（full_occupancy_rent）が未設定または不正"]

    if not data.get('management_fee'):
        warnings.append("管理費が未抽出のためOPEXは概算率のみで計算")
    if not data.get('reserve_fund'):
        warnings.append("修繕積立金が未抽出のためOPEXは概算率のみで計算")
    if not data.get('total_units'):
        warnings.append("総戸数が未抽出")

    return True, warnings


def calculate_loan_payment(principal, annual_rate, years):
    """元利均等返済の月額返済額を計算"""
    if principal <= 0 or annual_rate <= 0 or years <= 0:
        return 0.0
    monthly_rate = annual_rate / 12
    n_payments = years * 12
    numerator = monthly_rate * (1 + monthly_rate) ** n_payments
    denominator = (1 + monthly_rate) ** n_payments - 1
    return principal * (numerator / denominator)


def calculate_remaining_balance(principal, annual_rate, total_months, paid_months):
    """ローン残高を計算"""
    if principal <= 0 or annual_rate <= 0:
        return 0.0
    r = annual_rate / 12
    balance = principal * ((1 + r) ** total_months - (1 + r) ** paid_months) / ((1 + r) ** total_months - 1)
    return max(balance, 0.0)


def build_annual_cashflows(params):
    """年次キャッシュフロー表を構築"""
    cashflows = []
    for year in range(1, params['holding_period'] + 1):
        # GPI: 賃料下落を反映
        gpi = params['full_occupancy_rent_annual'] * (1 - params['rent_decline_rate']) ** (year - 1)

        # EGI: 空室損控除
        vacancy_loss = gpi * params['vacancy_rate']
        egi = gpi - vacancy_loss

        # OPEX: GPI比率 + 抽出済み管理費・修繕積立金
        opex = gpi * params['opex_ratio']
        if params.get('management_fee_annual', 0) > 0:
            opex += params['management_fee_annual']
        if params.get('reserve_fund_annual', 0) > 0:
            opex += params['reserve_fund_annual']

        # NOI
        noi = egi - opex

        # BTCFo
        btcfo = noi - params['ads']

        cashflows.append({
            'year': year,
            'gpi': gpi,
            'vacancy_loss': vacancy_loss,
            'egi': egi,
            'opex': opex,
            'noi': noi,
            'ads': params['ads'],
            'btcfo': btcfo,
        })

    return cashflows


def calculate_sale_proceeds(noi_final_year, exit_cap_rate, loan_balance):
    """売却キャッシュフローを計算"""
    if exit_cap_rate <= 0:
        exit_cap_rate = 0.03  # 最低3%

    sale_price = noi_final_year / exit_cap_rate
    sale_expenses = sale_price * DEFAULT_SALE_EXPENSE_RATE
    net_proceeds = sale_price - sale_expenses - loan_balance

    return {
        'sale_price': sale_price,
        'sale_expenses': sale_expenses,
        'loan_balance': loan_balance,
        'net_proceeds': net_proceeds,
    }


def calculate_investment_metrics(cashflows, equity, sale, total_purchase_cost,
                                  loan_amount, ads, expected_return):
    """投資分析指標を計算"""
    year1 = cashflows[0]

    # FCR（総収益率）= 初年度NOI / 総投資額
    fcr = year1['noi'] / total_purchase_cost if total_purchase_cost > 0 else 0

    # K%（ローン定数）= ADS / 借入額
    k_percent = ads / loan_amount if loan_amount > 0 else 0

    # CCR（自己資本配当率）= 初年度BTCFo / 自己資金
    ccr = year1['btcfo'] / equity if equity > 0 else 0

    # DCR（借入償還余裕率）= 初年度NOI / ADS
    dcr = year1['noi'] / ads if ads > 0 else float('inf')

    # BER（損益分岐入居率）= (OPEX + ADS) / GPI
    ber = (year1['opex'] + ads) / year1['gpi'] if year1['gpi'] > 0 else 1.0

    # レバレッジ分析
    leverage = 'Positive' if fcr > k_percent else 'Negative'

    # IRR/NPV用キャッシュフロー配列
    cf_array = [-equity]
    for i, cf in enumerate(cashflows):
        if i == len(cashflows) - 1:
            cf_array.append(cf['btcfo'] + sale['net_proceeds'])
        else:
            cf_array.append(cf['btcfo'])

    # IRR
    try:
        irr = npf.irr(cf_array)
        if irr is None or np.isnan(irr):
            irr = None
    except Exception:
        irr = None

    # NPV
    try:
        npv = npf.npv(expected_return, cf_array)
        if npv is not None and np.isnan(npv):
            npv = None
    except Exception:
        npv = None

    # 表面利回り（参考値）
    gross_yield = (cashflows[0]['gpi'] / total_purchase_cost) if total_purchase_cost > 0 else 0

    return {
        'gross_yield': gross_yield,
        'fcr': fcr,
        'k_percent': k_percent,
        'ccr': ccr,
        'dcr': dcr,
        'ber': ber,
        'leverage': leverage,
        'irr': irr,
        'npv': npv,
    }


def evaluate_investment_decision(metrics):
    """投資判断ロジック（6基準）"""
    decisions = {
        'fcr_vs_k': {
            'pass': metrics['fcr'] > metrics['k_percent'],
            'label': 'FCR > K%',
            'detail': f"FCR {metrics['fcr']:.2%} vs K% {metrics['k_percent']:.2%}",
        },
        'ccr_vs_fcr': {
            'pass': metrics['ccr'] > metrics['fcr'],
            'label': 'CCR > FCR',
            'detail': f"CCR {metrics['ccr']:.2%} vs FCR {metrics['fcr']:.2%}",
        },
        'dcr': {
            'pass': metrics['dcr'] >= THRESHOLD_DCR,
            'label': f'DCR >= {THRESHOLD_DCR}',
            'detail': f"DCR {metrics['dcr']:.2f}",
        },
        'ber': {
            'pass': metrics['ber'] <= THRESHOLD_BER,
            'label': f'BER <= {THRESHOLD_BER:.0%}',
            'detail': f"BER {metrics['ber']:.2%}",
        },
        'irr': {
            'pass': metrics['irr'] is not None and metrics['irr'] > DEFAULT_EXPECTED_RETURN,
            'label': f'IRR > {DEFAULT_EXPECTED_RETURN:.1%}',
            'detail': f"IRR {metrics['irr']:.2%}" if metrics['irr'] is not None else "IRR 計算不可",
        },
        'npv': {
            'pass': metrics['npv'] is not None and metrics['npv'] > 0,
            'label': 'NPV > 0',
            'detail': f"NPV {metrics['npv']:,.0f}円" if metrics['npv'] is not None else "NPV 計算不可",
        },
    }

    pass_count = sum(1 for d in decisions.values() if d['pass'])
    total_count = len(decisions)
    all_pass = pass_count == total_count

    return {
        'decisions': decisions,
        'all_pass': all_pass,
        'recommendation': '投資検討推奨' if all_pass else '投資見送り推奨',
        'pass_count': pass_count,
        'total_count': total_count,
    }


def run_simulation(comprehensive_data):
    """メインシミュレーション実行関数

    Args:
        comprehensive_data: extract_comprehensive_property_dataの返り値

    Returns:
        dict: シミュレーション結果全体。データ不足の場合はNone
    """
    try:
        # 1. バリデーション
        valid, warnings = validate_simulation_inputs(comprehensive_data)
        if not valid:
            print(f"シミュレーション入力不正: {warnings}")
            return None

        # 2. 基本パラメータ算出
        purchase_price = float(comprehensive_data['price'])
        purchase_expenses = purchase_price * DEFAULT_PURCHASE_EXPENSE_RATE
        total_purchase_cost = purchase_price + purchase_expenses
        loan_amount = purchase_price * DEFAULT_LTV
        equity = total_purchase_cost - loan_amount

        # 月額→年額変換
        full_occupancy_rent_monthly = float(comprehensive_data['full_occupancy_rent'])
        full_occupancy_rent_annual = full_occupancy_rent_monthly * 12

        management_fee_monthly = float(comprehensive_data.get('management_fee') or 0)
        reserve_fund_monthly = float(comprehensive_data.get('reserve_fund') or 0)

        # ローン計算
        monthly_payment = calculate_loan_payment(loan_amount, DEFAULT_INTEREST_RATE, DEFAULT_LOAN_TERM_YEARS)
        ads = monthly_payment * 12

        # 3. 年次CF構築パラメータ
        params = {
            'full_occupancy_rent_annual': full_occupancy_rent_annual,
            'vacancy_rate': DEFAULT_VACANCY_RATE,
            'rent_decline_rate': DEFAULT_RENT_DECLINE_RATE,
            'opex_ratio': DEFAULT_OPEX_RATIO,
            'management_fee_annual': management_fee_monthly * 12,
            'reserve_fund_annual': reserve_fund_monthly * 12,
            'ads': ads,
            'holding_period': DEFAULT_HOLDING_PERIOD,
        }

        cashflows = build_annual_cashflows(params)

        # 4. 売却CF計算
        entry_cap_rate = cashflows[0]['noi'] / total_purchase_cost if total_purchase_cost > 0 else 0.05
        exit_cap_rate = max(entry_cap_rate + DEFAULT_EXIT_CAP_RATE_SPREAD, 0.03)

        total_months = DEFAULT_LOAN_TERM_YEARS * 12
        paid_months = DEFAULT_HOLDING_PERIOD * 12
        loan_balance = calculate_remaining_balance(loan_amount, DEFAULT_INTEREST_RATE, total_months, paid_months)

        sale = calculate_sale_proceeds(cashflows[-1]['noi'], exit_cap_rate, loan_balance)

        # 5. 投資指標計算
        metrics = calculate_investment_metrics(
            cashflows, equity, sale, total_purchase_cost,
            loan_amount, ads, DEFAULT_EXPECTED_RETURN
        )

        # 6. 投資判断
        decision = evaluate_investment_decision(metrics)

        return {
            'params': {
                'purchase_price': purchase_price,
                'purchase_expenses': purchase_expenses,
                'total_purchase_cost': total_purchase_cost,
                'loan_amount': loan_amount,
                'equity': equity,
                'ltv': DEFAULT_LTV,
                'interest_rate': DEFAULT_INTEREST_RATE,
                'loan_term': DEFAULT_LOAN_TERM_YEARS,
                'monthly_payment': monthly_payment,
                'ads': ads,
                'vacancy_rate': DEFAULT_VACANCY_RATE,
                'rent_decline_rate': DEFAULT_RENT_DECLINE_RATE,
                'holding_period': DEFAULT_HOLDING_PERIOD,
                'opex_ratio': DEFAULT_OPEX_RATIO,
                'exit_cap_rate': exit_cap_rate,
                'expected_return': DEFAULT_EXPECTED_RETURN,
                'full_occupancy_rent_monthly': full_occupancy_rent_monthly,
                'full_occupancy_rent_annual': full_occupancy_rent_annual,
                'management_fee_monthly': management_fee_monthly,
                'reserve_fund_monthly': reserve_fund_monthly,
            },
            'cashflows': cashflows,
            'sale': sale,
            'metrics': metrics,
            'decision': decision,
            'warnings': warnings,
        }

    except Exception as e:
        print(f"シミュレーション実行エラー: {e}")
        import traceback
        traceback.print_exc()
        return None


def create_simulation_excel(simulation_result, property_info, drive_service, folder_id):
    """Excelファイルを作成してGoogle Driveに保存

    Args:
        simulation_result: run_simulationの返り値
        property_info: {"property_number": str, "station": str}
        drive_service: Google Drive APIサービス
        folder_id: 保存先フォルダID

    Returns:
        str: アップロードしたファイルのID。失敗時はNone
    """
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers

        wb = Workbook()
        p = simulation_result['params']
        m = simulation_result['metrics']
        d = simulation_result['decision']
        cfs = simulation_result['cashflows']
        sale = simulation_result['sale']

        # スタイル定義
        title_font = Font(bold=True, size=14)
        header_font = Font(bold=True, size=11, color="FFFFFF")
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        pass_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        fail_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        section_font = Font(bold=True, size=12)
        yen_format = '#,##0'
        pct_format = '0.00%'
        thin_border = Border(
            left=Side(style='thin'), right=Side(style='thin'),
            top=Side(style='thin'), bottom=Side(style='thin')
        )

        # ===== Sheet 1: 投資概要 =====
        ws1 = wb.active
        ws1.title = "投資概要"
        ws1.column_dimensions['A'].width = 25
        ws1.column_dimensions['B'].width = 20
        ws1.column_dimensions['C'].width = 15

        row = 1
        ws1.cell(row=row, column=1, value="投資シミュレーション").font = title_font
        row += 1
        ws1.cell(row=row, column=1, value=f"物件番号: {property_info.get('property_number', '')}  駅: {property_info.get('station', '')}")
        row += 1
        ws1.cell(row=row, column=1, value=f"作成日: {datetime.now().strftime('%Y-%m-%d')}")
        row += 2

        # 投資パラメータ
        ws1.cell(row=row, column=1, value="投資パラメータ").font = section_font
        row += 1
        param_rows = [
            ("購入価格", p['purchase_price'], yen_format),
            (f"諸費用（{DEFAULT_PURCHASE_EXPENSE_RATE:.0%}）", p['purchase_expenses'], yen_format),
            ("総投資額", p['total_purchase_cost'], yen_format),
            (f"借入額（LTV {p['ltv']:.0%}）", p['loan_amount'], yen_format),
            ("自己資金", p['equity'], yen_format),
            ("金利", p['interest_rate'], pct_format),
            ("借入期間", f"{p['loan_term']}年", None),
            ("月額返済額", p['monthly_payment'], yen_format),
            ("年間返済額（ADS）", p['ads'], yen_format),
            ("空室率", p['vacancy_rate'], pct_format),
            ("賃料下落率（年率）", p['rent_decline_rate'], pct_format),
            ("保有期間", f"{p['holding_period']}年", None),
            ("期待収益率（割引率）", p['expected_return'], pct_format),
            ("満室想定賃料（月額）", p['full_occupancy_rent_monthly'], yen_format),
            ("満室想定賃料（年額）", p['full_occupancy_rent_annual'], yen_format),
        ]
        for label, value, fmt in param_rows:
            ws1.cell(row=row, column=1, value=label).border = thin_border
            cell = ws1.cell(row=row, column=2, value=value)
            cell.border = thin_border
            if fmt:
                cell.number_format = fmt
            row += 1

        row += 1

        # 投資指標
        ws1.cell(row=row, column=1, value="投資指標").font = section_font
        row += 1
        for col, header in enumerate(["指標", "算出値", "判定"], 1):
            c = ws1.cell(row=row, column=col, value=header)
            c.font = header_font
            c.fill = header_fill
            c.border = thin_border
        row += 1

        metric_rows = [
            ("表面利回り", m['gross_yield'], pct_format, None),
            ("FCR（総収益率）", m['fcr'], pct_format, d['decisions']['fcr_vs_k']),
            ("K%（ローン定数）", m['k_percent'], pct_format, None),
            ("CCR（自己資本配当率）", m['ccr'], pct_format, d['decisions']['ccr_vs_fcr']),
            ("レバレッジ分析", m['leverage'], None, None),
            ("DCR（借入償還余裕率）", m['dcr'], '0.00', d['decisions']['dcr']),
            ("BER（損益分岐入居率）", m['ber'], pct_format, d['decisions']['ber']),
            ("IRR（内部収益率）", m['irr'], pct_format, d['decisions']['irr']),
            ("NPV（正味現在価値）", m['npv'], yen_format, d['decisions']['npv']),
        ]
        for label, value, fmt, decision_item in metric_rows:
            ws1.cell(row=row, column=1, value=label).border = thin_border
            cell_val = ws1.cell(row=row, column=2, value=value if value is not None else "計算不可")
            cell_val.border = thin_border
            if fmt and value is not None:
                cell_val.number_format = fmt
            if decision_item:
                judge_cell = ws1.cell(row=row, column=3, value="○ PASS" if decision_item['pass'] else "× FAIL")
                judge_cell.fill = pass_fill if decision_item['pass'] else fail_fill
                judge_cell.border = thin_border
            row += 1

        row += 1
        ws1.cell(row=row, column=1, value="総合判定").font = section_font
        ws1.cell(row=row, column=1).border = thin_border
        judge = ws1.cell(row=row, column=2, value=f"{d['recommendation']}（{d['pass_count']}/{d['total_count']}項目クリア）")
        judge.font = Font(bold=True, size=12)
        judge.fill = pass_fill if d['all_pass'] else fail_fill
        judge.border = thin_border

        # ===== Sheet 2: 年次キャッシュフロー =====
        ws2 = wb.create_sheet("年次キャッシュフロー")
        headers = ["年度", "GPI", "空室損", "EGI", "OPEX", "NOI", "ADS", "BTCFo"]
        for col, h in enumerate(headers, 1):
            c = ws2.cell(row=1, column=col, value=h)
            c.font = header_font
            c.fill = header_fill
            c.border = thin_border
            c.alignment = Alignment(horizontal='center')

        # 列幅設定
        ws2.column_dimensions['A'].width = 8
        for col_letter in ['B', 'C', 'D', 'E', 'F', 'G', 'H']:
            ws2.column_dimensions[col_letter].width = 18

        # データ行
        even_fill = PatternFill(start_color="D9E2F3", end_color="D9E2F3", fill_type="solid")
        for i, cf in enumerate(cfs):
            row_num = i + 2
            values = [cf['year'], cf['gpi'], cf['vacancy_loss'], cf['egi'],
                      cf['opex'], cf['noi'], cf['ads'], cf['btcfo']]
            for col, val in enumerate(values, 1):
                cell = ws2.cell(row=row_num, column=col, value=val)
                cell.border = thin_border
                if col >= 2:
                    cell.number_format = yen_format
                if i % 2 == 1:
                    cell.fill = even_fill

        # 合計行
        total_row = len(cfs) + 2
        ws2.cell(row=total_row, column=1, value="合計").font = Font(bold=True)
        ws2.cell(row=total_row, column=1).border = thin_border
        key_map = {2: 'gpi', 3: 'vacancy_loss', 4: 'egi', 5: 'opex', 6: 'noi', 7: 'ads', 8: 'btcfo'}
        for col in range(2, 9):
            if col > 1:
                total = sum(cf[key_map[col]] for cf in cfs)
                cell = ws2.cell(row=total_row, column=col, value=total)
                cell.number_format = yen_format
                cell.font = Font(bold=True)
                cell.border = thin_border

        ws2.freeze_panes = 'A2'

        # ===== Sheet 3: 売却シミュレーション =====
        ws3 = wb.create_sheet("売却シミュレーション")
        ws3.column_dimensions['A'].width = 25
        ws3.column_dimensions['B'].width = 20

        row = 1
        ws3.cell(row=row, column=1, value="売却シミュレーション").font = title_font
        row += 2

        sale_rows = [
            ("保有期間", f"{p['holding_period']}年", None),
            ("出口キャップレート", p['exit_cap_rate'], pct_format),
            ("最終年NOI", cfs[-1]['noi'], yen_format),
            ("売却想定価格", sale['sale_price'], yen_format),
            (f"売却諸費用（{DEFAULT_SALE_EXPENSE_RATE:.0%}）", sale['sale_expenses'], yen_format),
            ("残債", sale['loan_balance'], yen_format),
            ("売却手取り", sale['net_proceeds'], yen_format),
        ]
        for label, value, fmt in sale_rows:
            ws3.cell(row=row, column=1, value=label).border = thin_border
            cell = ws3.cell(row=row, column=2, value=value)
            cell.border = thin_border
            if fmt:
                cell.number_format = fmt
            row += 1

        row += 1
        ws3.cell(row=row, column=1, value="IRR計算用キャッシュフロー").font = section_font
        row += 1
        for col, h in enumerate(["年度", "キャッシュフロー"], 1):
            c = ws3.cell(row=row, column=col, value=h)
            c.font = header_font
            c.fill = header_fill
            c.border = thin_border
        row += 1

        # 初期投資
        ws3.cell(row=row, column=1, value="0（初期投資）").border = thin_border
        cell = ws3.cell(row=row, column=2, value=-p['equity'])
        cell.number_format = yen_format
        cell.border = thin_border
        row += 1

        # 各年CF
        for i, cf in enumerate(cfs):
            ws3.cell(row=row, column=1, value=f"{cf['year']}").border = thin_border
            if i == len(cfs) - 1:
                val = cf['btcfo'] + sale['net_proceeds']
                label_suffix = "（売却含む）"
            else:
                val = cf['btcfo']
                label_suffix = ""
            cell = ws3.cell(row=row, column=2, value=val)
            cell.number_format = yen_format
            cell.border = thin_border
            row += 1

        # Excel保存 → Google Drive アップロード
        buffer = BytesIO()
        wb.save(buffer)
        buffer.seek(0)

        from googleapiclient.http import MediaIoBaseUpload
        filename = f"投資シミュレーション_{property_info.get('property_number', 'unknown')}_{property_info.get('station', '')}.xlsx"
        media = MediaIoBaseUpload(
            buffer,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True
        )
        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        uploaded = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()

        print(f"Excel保存完了: {filename}")
        return uploaded['id']

    except Exception as e:
        print(f"Excel作成エラー: {e}")
        import traceback
        traceback.print_exc()
        return None


def format_simulation_summary_for_report(simulation_result):
    """Google Docsレポート用のシミュレーションサマリーテキストを生成"""
    if not simulation_result:
        return ["", "【投資シミュレーション】", "シミュレーション実行不可（データ不足）"]

    p = simulation_result['params']
    m = simulation_result['metrics']
    d = simulation_result['decision']

    def mark(passed):
        return "○" if passed else "×"

    lines = [
        "",
        "【投資シミュレーション結果】",
        f"総投資額: {p['total_purchase_cost']:,.0f}円（購入価格 {p['purchase_price']:,.0f}円 + 諸費用 {p['purchase_expenses']:,.0f}円）",
        f"借入: {p['loan_amount']:,.0f}円（LTV {p['ltv']:.0%}）/ 自己資金: {p['equity']:,.0f}円",
        f"金利: {p['interest_rate']:.2%} / 期間: {p['loan_term']}年 / ADS: {p['ads']:,.0f}円/年",
        "",
        "【投資指標】",
        f"表面利回り: {m['gross_yield']:.2%}（参考値）",
        f"FCR（総収益率）: {m['fcr']:.2%}  {mark(d['decisions']['fcr_vs_k']['pass'])}",
        f"K%（ローン定数）: {m['k_percent']:.2%}",
        f"CCR（自己資本配当率）: {m['ccr']:.2%}  {mark(d['decisions']['ccr_vs_fcr']['pass'])}",
        f"レバレッジ分析: {m['leverage']}",
        f"DCR（借入償還余裕率）: {m['dcr']:.2f}  {mark(d['decisions']['dcr']['pass'])}",
        f"BER（損益分岐入居率）: {m['ber']:.2%}  {mark(d['decisions']['ber']['pass'])}",
    ]

    if m.get('irr') is not None:
        lines.append(f"IRR（内部収益率）: {m['irr']:.2%}  {mark(d['decisions']['irr']['pass'])}")
    else:
        lines.append("IRR（内部収益率）: 計算不可  ×")

    if m.get('npv') is not None:
        lines.append(f"NPV（正味現在価値）: {m['npv']:,.0f}円  {mark(d['decisions']['npv']['pass'])}")
    else:
        lines.append("NPV（正味現在価値）: 計算不可  ×")

    lines.extend([
        "",
        f"総合判定: {d['recommendation']}（{d['pass_count']}/{d['total_count']}項目クリア）",
    ])

    # 警告があれば追加
    if simulation_result.get('warnings'):
        lines.append("")
        lines.append("※ 注意事項:")
        for w in simulation_result['warnings']:
            lines.append(f"  - {w}")

    return lines
