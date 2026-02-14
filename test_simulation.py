"""
simulation.py のユニットテスト
要件定義書サンプル値（北区上中里3丁目アパート）で検証
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from simulation import (
    validate_simulation_inputs,
    calculate_loan_payment,
    calculate_remaining_balance,
    build_annual_cashflows,
    calculate_sale_proceeds,
    calculate_investment_metrics,
    evaluate_investment_decision,
    run_simulation,
    format_simulation_summary_for_report,
)


def test_validate_simulation_inputs():
    """入力バリデーションのテスト"""
    # 正常ケース
    valid, warnings = validate_simulation_inputs({'price': 61000000, 'full_occupancy_rent': 306000})
    assert valid is True, "正常データがバリデーション失敗"

    # 必須項目欠損
    valid, warnings = validate_simulation_inputs({'price': 0, 'full_occupancy_rent': 306000})
    assert valid is False, "price=0でバリデーション通過"

    valid, warnings = validate_simulation_inputs({'price': 61000000})
    assert valid is False, "rent欠損でバリデーション通過"

    # 警告付きケース
    valid, warnings = validate_simulation_inputs({'price': 61000000, 'full_occupancy_rent': 306000})
    assert valid is True
    assert len(warnings) > 0, "管理費・修繕積立金なしで警告がない"

    print("✅ validate_simulation_inputs: PASS")


def test_calculate_loan_payment():
    """ローン返済額の計算テスト"""
    # 要件定義書: ¥54,900,000, 2.25%, 30年
    monthly = calculate_loan_payment(54900000, 0.0225, 30)
    annual = monthly * 12

    # 月額は約21万前後のはず
    assert 200000 < monthly < 250000, f"月額返済が想定外: {monthly:,.0f}円"

    # K% = ADS / 借入額 ≒ 4-6%
    k_percent = annual / 54900000
    assert 0.04 < k_percent < 0.06, f"K%が想定外: {k_percent:.4f}"

    print(f"  月額返済: {monthly:,.0f}円")
    print(f"  年間返済(ADS): {annual:,.0f}円")
    print(f"  K%: {k_percent:.4f} ({k_percent*100:.2f}%)")
    print("✅ calculate_loan_payment: PASS")


def test_calculate_remaining_balance():
    """ローン残高計算テスト"""
    # 30年ローンで10年後の残高
    balance = calculate_remaining_balance(54900000, 0.0225, 360, 120)

    # 10年後の残高は元本の60-80%程度のはず
    ratio = balance / 54900000
    assert 0.60 < ratio < 0.85, f"残高比率が想定外: {ratio:.4f}"

    print(f"  10年後残高: {balance:,.0f}円 ({ratio*100:.1f}%)")
    print("✅ calculate_remaining_balance: PASS")


def test_build_annual_cashflows():
    """年次キャッシュフロー構築テスト"""
    monthly_payment = calculate_loan_payment(54900000, 0.0225, 30)

    params = {
        'full_occupancy_rent_annual': 306000 * 12,  # 年額 3,672,000
        'vacancy_rate': 0.05,
        'rent_decline_rate': 0.005,
        'opex_ratio': 0.15,
        'management_fee_annual': 0,
        'reserve_fund_annual': 0,
        'ads': monthly_payment * 12,
        'holding_period': 10,
    }

    cashflows = build_annual_cashflows(params)

    assert len(cashflows) == 10, f"年数が10ではない: {len(cashflows)}"

    # 1年目のGPI = 3,672,000
    assert abs(cashflows[0]['gpi'] - 3672000) < 1, f"GPI不正: {cashflows[0]['gpi']}"

    # 10年目のGPIは下落反映
    expected_gpi_y10 = 3672000 * (1 - 0.005) ** 9
    assert abs(cashflows[9]['gpi'] - expected_gpi_y10) < 1, f"10年目GPI不正"

    # NOI > 0（正常物件なら）
    assert cashflows[0]['noi'] > 0, f"初年度NOIが負: {cashflows[0]['noi']}"

    # EGI = GPI * 0.95
    assert abs(cashflows[0]['egi'] - 3672000 * 0.95) < 1

    print(f"  1年目: GPI={cashflows[0]['gpi']:,.0f} EGI={cashflows[0]['egi']:,.0f} NOI={cashflows[0]['noi']:,.0f} BTCFo={cashflows[0]['btcfo']:,.0f}")
    print(f"  10年目: GPI={cashflows[9]['gpi']:,.0f} NOI={cashflows[9]['noi']:,.0f} BTCFo={cashflows[9]['btcfo']:,.0f}")
    print("✅ build_annual_cashflows: PASS")


def test_run_simulation_sample():
    """要件定義書サンプル値での統合テスト（北区上中里3丁目アパート）"""
    # 要件定義書 付録A の物件データ
    sample_data = {
        'property_number': '99999',
        'station': '上中里',
        'address': '東京都北区上中里3丁目8-12',
        'price': 61000000,        # 6,100万円
        'structure': '木造',
        'year_built': '2019年3月',
        'land_area': 61.44,
        'building_area': 86.54,
        'total_units': 4,
        'full_occupancy_rent': 306000,  # 月額30.6万円
        'floor_plan': '1R',
        'management_fee': None,     # サンプルに管理費の明記なし
        'reserve_fund': None,
        'rent_roll': [
            {'room': '101', 'plan': '1R', 'area': 6.7, 'rent': 76500},
            {'room': '102', 'plan': '1R', 'area': 6.7, 'rent': 76500},
            {'room': '201', 'plan': '1R', 'area': 7.0, 'rent': 76500},
            {'room': '202', 'plan': '1R', 'area': 7.0, 'rent': 76500},
        ],
    }

    result = run_simulation(sample_data)

    assert result is not None, "シミュレーションがNoneを返した"

    p = result['params']
    m = result['metrics']
    d = result['decision']
    sale = result['sale']

    print("\n===== 要件定義書サンプル値での検証 =====")
    print(f"\n--- パラメータ ---")
    print(f"  購入価格:    {p['purchase_price']:>15,.0f}円")
    print(f"  諸費用(8%):  {p['purchase_expenses']:>15,.0f}円")
    print(f"  総投資額:    {p['total_purchase_cost']:>15,.0f}円")
    print(f"  借入額:      {p['loan_amount']:>15,.0f}円")
    print(f"  自己資金:    {p['equity']:>15,.0f}円")
    print(f"  月額返済:    {p['monthly_payment']:>15,.0f}円")
    print(f"  ADS:         {p['ads']:>15,.0f}円")

    # 要件定義書の値と比較
    assert p['purchase_price'] == 61000000
    assert abs(p['purchase_expenses'] - 4880000) < 1
    assert abs(p['total_purchase_cost'] - 65880000) < 1
    assert abs(p['loan_amount'] - 54900000) < 1
    assert abs(p['equity'] - 10980000) < 1

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
    else:
        print(f"  IRR:         計算不可")
    if m['npv'] is not None:
        print(f"  NPV:         {m['npv']:,.0f}円")
    else:
        print(f"  NPV:         計算不可")

    # 要件定義書サンプルの概算値との大まかな比較
    # 注: OPEX率や詳細条件の違いにより完全一致はしない
    assert abs(m['gross_yield'] - 0.0601) < 0.01, f"表面利回りが大きくずれている: {m['gross_yield']:.4f}"

    print(f"\n--- 売却シミュレーション ---")
    print(f"  売却想定価格: {sale['sale_price']:,.0f}円")
    print(f"  売却諸費用:  {sale['sale_expenses']:,.0f}円")
    print(f"  残債:        {sale['loan_balance']:,.0f}円")
    print(f"  売却手取り:  {sale['net_proceeds']:,.0f}円")

    print(f"\n--- 投資判断 ---")
    for key, item in d['decisions'].items():
        status = "○ PASS" if item['pass'] else "× FAIL"
        print(f"  {item['label']}: {item['detail']} → {status}")
    print(f"  総合判定: {d['recommendation']}（{d['pass_count']}/{d['total_count']}）")

    print("✅ run_simulation (sample): PASS")


def test_format_simulation_summary():
    """レポート用サマリーフォーマットテスト"""
    # None入力
    lines = format_simulation_summary_for_report(None)
    assert len(lines) > 0
    assert "データ不足" in lines[-1]

    # 正常入力
    sample_data = {
        'price': 61000000,
        'full_occupancy_rent': 306000,
    }
    result = run_simulation(sample_data)
    lines = format_simulation_summary_for_report(result)

    assert any("投資シミュレーション" in line for line in lines)
    assert any("FCR" in line for line in lines)
    assert any("総合判定" in line for line in lines)

    print("✅ format_simulation_summary_for_report: PASS")


def test_edge_cases():
    """エッジケースのテスト"""
    # 極端に安い物件
    result = run_simulation({'price': 1000000, 'full_occupancy_rent': 50000})
    assert result is not None

    # 極端に高い物件
    result = run_simulation({'price': 1000000000, 'full_occupancy_rent': 5000000})
    assert result is not None

    # 管理費・修繕積立金付き
    result = run_simulation({
        'price': 61000000,
        'full_occupancy_rent': 306000,
        'management_fee': 10000,
        'reserve_fund': 5000,
    })
    assert result is not None
    # OPEXが管理費+修繕分だけ増えているはず
    base_result = run_simulation({'price': 61000000, 'full_occupancy_rent': 306000})
    assert result['cashflows'][0]['opex'] > base_result['cashflows'][0]['opex']

    print("✅ edge_cases: PASS")


if __name__ == '__main__':
    print("=" * 60)
    print("simulation.py テスト実行")
    print("=" * 60)

    test_validate_simulation_inputs()
    test_calculate_loan_payment()
    test_calculate_remaining_balance()
    test_build_annual_cashflows()
    test_run_simulation_sample()
    test_format_simulation_summary()
    test_edge_cases()

    print("\n" + "=" * 60)
    print("全テスト PASS")
    print("=" * 60)
