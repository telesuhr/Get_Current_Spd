# LME Metal Spread Analysis Tools

LME（ロンドン金属取引所）金属スプレッドの分析・取得ツール集です。Bloomberg APIを使用して、アクティブなスプレッドの検索、満期日の表示、当日の取引データの取得などを行います。

## 🆕 SQL Serverベースシステム（推奨）

ティックデータの保存とヒストリカル分析に対応した新しいSQL Serverベースのシステムが利用可能です。

### 特徴
- **複数金属対応**: 銅、アルミニウム、亜鉛、鉛、ニッケル、錫
- **ティックデータ保存**: 全ての価格更新を時系列で保存
- **重複防止機能**: データの整合性を自動的に維持
- **3段階の収集頻度**: リアルタイム（5分）、定期（30分）、日次メンテナンス
- **ヒストリカル分析**: 過去データの照会と分析が可能

詳細は [SQL Server Setup Guide](docs/sql_design/SQL_SERVER_SETUP.md) を参照してください。

## 必要な環境

### 基本要件
- Python 3.8+
- Bloomberg Terminal（起動済み）
- blpapi (Bloomberg Python API)
- pandas

### SQL Serverシステム追加要件
- SQL Server 2016以降（Express版でも可）
- pyodbc
- ODBC Driver 17 for SQL Server

## フォルダ構成

```
GetAllSpd/
├── scripts/
│   ├── analysis/          # スプレッド分析スクリプト
│   ├── market_data/       # 市場データ取得スクリプト
│   ├── sql_collector/     # SQL Serverデータ収集システム
│   ├── scheduler/         # スケジューラー
│   └── debug/             # デバッグ・検証用スクリプト
├── sql/
│   ├── schema/            # データベーススキーマ
│   ├── procedures/        # ストアドプロシージャ
│   └── views/             # ビュー定義
├── data/
│   ├── csv_results/       # 実行結果のCSVファイル
│   └── reference/         # 参照用データ
├── docs/
│   └── sql_design/        # SQL Server設計ドキュメント
├── config.example.json    # 設定ファイルサンプル
└── README.md
```

## 主要スクリプト

### 1. 最新版：リアルタイムスプレッド検索（推奨）
```bash
python scripts/market_data/get_all_spreads_realtime.py
```
- **新しいスプレッドの組み合わせも自動検出**
- CSVファイルに依存せず、毎回最新のスプレッドリストを作成
- 2,500以上のスプレッドから検索
- アクティブな板情報のみを表示

### 2. 満期日付き全アクティブスプレッド取得（CSVベース）
```bash
python scripts/market_data/get_all_spreads_with_prompts.py
```
- 通常のカレンダースプレッド、3M-3Wスプレッド、Odd dateスプレッドを全て取得
- 各スプレッドの正確な満期日（Prompt Date）を2列で表示
- LMCADS03とLMCADYから実際のLME_PROMPT_DTを取得して使用
- 当日の出来高は別管理（過去の出来高と混在しない）
- **注意**: 事前にanalyze_spreads.pyとanalyze_odd_date_spreads.pyの実行が必要

### 3. スプレッド検索・分析

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

### 4. 市場データ取得

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

### 5. デバッグツール

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

### 今日取引があったスプレッドを確認（リアルタイム版）
```bash
python scripts/market_data/get_all_spreads_realtime.py
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