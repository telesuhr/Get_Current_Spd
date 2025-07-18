# LME Copper Spread Analysis Tools

LME（ロンドン金属取引所）銅スプレッドの分析・取得ツール集です。Bloomberg APIを使用して、アクティブなスプレッドの検索、満期日の表示、当日の取引データの取得などを行います。

## 必要な環境

- Python 3.x
- Bloomberg Terminal（起動済み）
- blpapi (Bloomberg Python API)
- pandas

## フォルダ構成

```
GetAllSpd/
├── scripts/
│   ├── analysis/          # スプレッド分析スクリプト
│   ├── market_data/       # 市場データ取得スクリプト
│   └── debug/             # デバッグ・検証用スクリプト
├── data/
│   ├── csv_results/       # 実行結果のCSVファイル
│   └── reference/         # 参照用データ
└── README.md
```

## 主要スクリプト

### 1. 最終版：満期日付き全アクティブスプレッド取得
```bash
python scripts/market_data/get_all_spreads_with_prompts.py
```
- 通常のカレンダースプレッド、3M-3Wスプレッド、Odd dateスプレッドを全て取得
- 各スプレッドの正確な満期日（Prompt Date）を2列で表示
- LMCADS03とLMCADYから実際のLME_PROMPT_DTを取得して使用
- 当日の出来高は別管理（過去の出来高と混在しない）

### 2. スプレッド検索・分析

#### 通常のスプレッド分析
```bash
python scripts/analysis/analyze_spreads.py
```
- カレンダースプレッド（例：Q25U25）
- 3M-3Wスプレッド（例：03X25）

#### Odd dateスプレッド分析
```bash
python scripts/analysis/analyze_odd_date_spreads.py
```
- 特定日付間スプレッド（例：250722-250729）
- Cash-特定日付スプレッド（例：00-250722）
- 3M-特定日付スプレッド（例：250722-03）

### 3. 市場データ取得

#### 当日の出来高を正確に取得
```bash
python scripts/market_data/get_todays_volume_spreads.py
```
- last_update_dtが今日のもののみ出来高を表示
- 過去の取引はvolume=0として処理

#### 現在のBid/Ask取得
```bash
python scripts/market_data/get_current_market_spreads.py
```
- 現在アクティブな板情報を取得
- Bid/Askスプレッドを計算

### 4. デバッグツール

```bash
python scripts/debug/check_prompt_dates.py     # 3M/Cashの満期日確認
python scripts/debug/debug_single_spread.py    # 特定スプレッドの詳細確認
```

## スプレッドの種類と表記

### 1. カレンダースプレッド
- **形式**: `LMCADS Q25U25 Comdty`
- **意味**: 2025年8月第3水曜日 - 2025年9月第3水曜日
- **満期日**: 2025/08/20 - 2025/09/17

### 2. 3M-3Wスプレッド
- **形式**: `LMCADS 03X25 Comdty`
- **意味**: 3ヶ月先物 - 2025年11月第3水曜日
- **満期日**: 2025/10/17 - 2025/11/19

### 3. Odd-Oddスプレッド
- **形式**: `LMCADS 250722-250729 Comdty`
- **意味**: 2025年7月22日 - 2025年7月29日
- **満期日**: 2025/07/22 - 2025/07/29

### 4. Cash-Oddスプレッド
- **形式**: `LMCADS 00-250722 Comdty`
- **意味**: Cash（T+2）- 2025年7月22日
- **満期日**: 2025/07/22 - 2025/07/22

## 月コード

| コード | 月 |
|-------|-----|
| F | 1月 |
| G | 2月 |
| H | 3月 |
| J | 4月 |
| K | 5月 |
| M | 6月 |
| N | 7月 |
| Q | 8月 |
| U | 9月 |
| V | 10月 |
| X | 11月 |
| Z | 12月 |

## 使用例

### 今日取引があったスプレッドを確認
```bash
python scripts/market_data/get_all_spreads_with_prompts.py
```

出力例：
```
Ticker                    | Prompt1      | Prompt2      | Type     | Bid    | Ask    | Last   | Today Vol
LMCADS U25V25 Comdty     | 2025/09/17   | 2025/10/15   | Calendar | -7.00  | -4.00  | -5.00  | 156
LMCADS V2503 Comdty      | 2025/10/15   | 2026/01/15   | Odd Date | 6.26   | 8.01   | 6.14   | 60
```

## 注意事項

- Bloomberg Terminalが起動している必要があります
- 大量のデータを取得する際はレート制限に注意してください
- 日付の切り替わりはロンドン時間基準（日本時間16-17時頃）です

## トラブルシューティング

### "Failed to start Bloomberg session"エラー
- Bloomberg Terminalが起動しているか確認
- ポート8194が使用可能か確認

### 出来高が前日のものになる
- `get_todays_volume_spreads.py`を使用して、last_update_dtでフィルタリング

### 満期日が正しくない
- 最新版の`get_all_spreads_with_prompts.py`は実際のLME_PROMPT_DTを使用