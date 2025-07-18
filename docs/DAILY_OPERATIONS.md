# LME Spread Data Collection - Daily Operations Guide

## 概要
このガイドでは、LME銅スプレッドデータの日次収集と管理に必要な手順を説明します。

## 日次実行スクリプト

### 1. **メインデータ収集スクリプト** 
`scripts/sql_collector/quick_collect_copper.py`

**目的**: LME銅スプレッドの最新市場データをSQL Serverに収集
**実行タイミング**: 
- 日本時間 16:30以降（ロンドン市場開始後）
- 日本時間 01:30頃（ロンドン市場活発時）

```bash
python scripts/sql_collector/quick_collect_copper.py
```

**機能**:
- アクティブなスプレッドの最新Bid/Ask/Trade価格を収集
- 当日の出来高を正確に記録
- 重複データを防止
- エラー時の自動リトライ

### 2. **プロンプト日付更新スクリプト**
`scripts/sql_collector/update_prompt_dates.py`

**目的**: 新規スプレッドのプロンプト日付を更新
**実行タイミング**: 週1回または新規スプレッド追加時

```bash
python scripts/sql_collector/update_prompt_dates.py
```

### 3. **スプレッド分類更新スクリプト**
`scripts/sql_collector/classify_actual_spreads.py`

**目的**: 日付表記スプレッドの実際のタイプを分類
**実行タイミング**: 週1回または必要時

```bash
python scripts/sql_collector/classify_actual_spreads.py
```

## 実行順序（推奨）

### 日次処理
```bash
# 1. メインデータ収集（必須）
python scripts/sql_collector/quick_collect_copper.py
```

### 週次処理
```bash
# 1. プロンプト日付更新
python scripts/sql_collector/update_prompt_dates.py

# 2. スプレッド分類更新
python scripts/sql_collector/classify_actual_spreads.py

# 3. データ収集
python scripts/sql_collector/quick_collect_copper.py
```

## 自動化設定

### Windows Task Scheduler設定例

1. **日次データ収集タスク**
   - 名前: `LME_Daily_Collection`
   - トリガー: 毎日 16:30, 01:30
   - 操作: `python C:\Users\09848\Git\GetAllSpd\scripts\sql_collector\quick_collect_copper.py`

2. **週次更新タスク**
   - 名前: `LME_Weekly_Update`
   - トリガー: 毎週月曜日 15:00
   - 操作: バッチファイル実行（下記参照）

### バッチファイル例
`run_weekly_update.bat`:
```batch
@echo off
cd C:\Users\09848\Git\GetAllSpd

echo [%date% %time%] Starting weekly update...

echo Updating prompt dates...
python scripts\sql_collector\update_prompt_dates.py

echo Classifying spreads...
python scripts\sql_collector\classify_actual_spreads.py

echo Running data collection...
python scripts\sql_collector\quick_collect_copper.py

echo [%date% %time%] Weekly update completed.
```

## モニタリングとトラブルシューティング

### ログファイル
- 場所: `logs/lme_collector.log`
- 日次でローテーション（5世代保持）

### データ確認クエリ
```sql
-- 本日のデータ収集状況確認
SELECT COUNT(*), MAX(last_update_dt) 
FROM lme_market.V_latest_market_data 
WHERE last_update_dt >= CAST(GETDATE() AS DATE);

-- アクティブスプレッド数確認
SELECT spread_type, COUNT(*) 
FROM lme_market.V_active_spreads_actual 
WHERE metal_code = 'CU' 
GROUP BY spread_type;
```

### よくある問題と対処法

1. **Bloomberg接続エラー**
   - Bloomberg Terminalが起動していることを確認
   - ポート8194が使用可能か確認

2. **SQL Server接続エラー**
   - VPN接続を確認
   - ファイアウォール設定を確認

3. **データ重複**
   - 自動的に防止されるが、手動確認は以下で実行：
   ```sql
   SELECT ticker, COUNT(*) 
   FROM lme_market.LME_T_market_data 
   WHERE collection_dt = CAST(GETDATE() AS DATE) 
   GROUP BY ticker 
   HAVING COUNT(*) > 1;
   ```

## 設定ファイル
- `config.jcl.json`: データベース接続設定（要保護）

## サポート
問題が発生した場合は、ログファイルを確認し、必要に応じてスクリプトを再実行してください。