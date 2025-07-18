# JCLデータベースへのLMEシステム実装手順

## 概要

既存のJCLデータベース内に、LME金属スプレッドシステムを構築します。独立したスキーマ（lme_market, lme_config）を使用し、テーブル名にはLME_プレフィックスを付けて既存システムとの衝突を避けます。

## 実行手順

### 1. JCLデータベースに接続

SQL Server Management Studio (SSMS) で：
1. サーバーに接続
2. **データベース: JCL** を選択

### 2. SQLスクリプトを順番に実行

以下の順序で各スクリプトを実行してください：

#### Step 1: スキーマ作成
```sql
-- ファイル: sql\schema\01_create_schemas_in_JCL.sql
-- LME専用のスキーマを作成
```

#### Step 2: テーブル作成
```sql
-- ファイル: sql\schema\02_create_tables_in_JCL.sql
-- LME_プレフィックス付きのテーブルを作成
```

#### Step 3: ストアドプロシージャ作成
```sql
-- ファイル: sql\procedures\03_create_procedures_in_JCL.sql
-- データ操作用のストアドプロシージャを作成
```

#### Step 4: ビュー作成
```sql
-- ファイル: sql\views\04_create_views_in_JCL_v2.sql
-- 分析・レポート用のビューを作成（V_プレフィックス付き）
```

## 作成されるオブジェクト

### スキーマ (2個)
- `lme_config` - 設定・マスタデータ用
- `lme_market` - 市場データ用

### テーブル (5個)
```
lme_config.LME_M_metals              -- 金属マスタ
lme_config.LME_M_collection_config   -- 収集設定
lme_market.LME_M_spreads            -- スプレッド定義
lme_market.LME_T_tick_data          -- ティックデータ
lme_market.LME_T_daily_summary      -- 日次集計
```

### ストアドプロシージャ (5個)
```
lme_market.sp_UpsertSpread          -- スプレッド登録/更新
lme_market.sp_InsertTickData        -- ティックデータ挿入
lme_market.sp_BulkInsertTickData    -- 一括挿入
lme_market.sp_CalculateDailySummary -- 日次集計計算
lme_market.sp_GetActiveSpreads      -- アクティブスプレッド取得
```

### ビュー (7個)
```
lme_market.V_latest_market_data     -- 最新市場データ
lme_market.V_active_spreads         -- アクティブスプレッド
lme_market.V_todays_activity        -- 当日の取引活動
lme_market.V_spread_type_summary    -- スプレッドタイプ別サマリー
lme_market.V_price_history          -- 価格履歴
lme_market.V_daily_summary          -- 日次サマリー
lme_config.V_collection_health      -- 収集状況モニタリング
```

## 確認クエリ

### インストール確認
```sql
-- 現在のデータベース確認
SELECT DB_NAME() AS CurrentDatabase;

-- スキーマ確認
SELECT name FROM sys.schemas 
WHERE name IN ('lme_market', 'lme_config');

-- テーブル確認
SELECT 
    s.name AS SchemaName,
    t.name AS TableName
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name IN ('lme_market', 'lme_config')
ORDER BY s.name, t.name;

-- 初期データ確認
SELECT * FROM lme_config.LME_M_metals;
```

### データ確認
```sql
-- 最新の市場データ
SELECT TOP 10 * 
FROM lme_market.V_latest_market_data
WHERE metal_code = 'CU'
ORDER BY todays_volume DESC;

-- 収集状況確認
SELECT * FROM lme_config.V_collection_health;
```

## Python設定

### 1. 設定ファイルの作成

`config.jcl.json` を作成（`config.example.jcl.json` をコピー）：

```json
{
    "database": {
        "server": "localhost",
        "database": "JCL",
        "driver": "ODBC Driver 17 for SQL Server",
        "trusted_connection": true,
        "schema_prefix": "lme_"
    },
    "bloomberg": {
        "host": "localhost",
        "port": 8194
    }
}
```

### 2. Pythonスクリプトの使用

```python
# JCL版のコレクターを使用
from sql_data_collector_jcl import SQLServerDataCollectorJCL

collector = SQLServerDataCollectorJCL("config.jcl.json")
```

## 注意事項

1. **既存データとの分離**
   - LME_プレフィックスにより既存テーブルと区別
   - 専用スキーマで論理的に分離

2. **権限要件**
   - スキーマ作成権限
   - テーブル/ビュー/プロシージャ作成権限
   - データ読み書き権限

3. **バックアップ**
   - JCLデータベース全体のバックアップに含まれます
   - 個別のバックアップは不要

## トラブルシューティング

### スキーマが既に存在する場合
```sql
-- 既存のLMEオブジェクトを確認
SELECT * FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE s.name LIKE 'lme_%';
```

### 権限エラーの場合
```sql
-- 必要な権限を確認
SELECT * FROM sys.database_permissions
WHERE grantee_principal_id = USER_ID();
```