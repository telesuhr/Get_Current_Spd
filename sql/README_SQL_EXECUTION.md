# SQL スクリプト実行手順

## 実行方法

### 方法1: SQL Server Management Studio (SSMS) を使用

1. **SQL Server Management Studio を開く**

2. **SQL Server に接続**
   - サーバー名: `localhost` または実際のサーバー名
   - 認証: Windows 認証

3. **スクリプトを順番に実行**

   以下の順序で各スクリプトを開いて実行してください：

   1. **データベース作成**
      - ファイル: `schema\01_create_database.sql`
      - 新しいクエリウィンドウで開いて実行 (F5)

   2. **テーブル作成**
      - ファイル: `schema\02_create_tables_v2.sql`
      - 新しいクエリウィンドウで開いて実行 (F5)

   3. **ストアドプロシージャ作成**
      - ファイル: `procedures\01_insert_procedures_v2.sql`
      - 新しいクエリウィンドウで開いて実行 (F5)

   4. **ビュー作成**
      - ファイル: `views\01_market_views_v2.sql`
      - 新しいクエリウィンドウで開いて実行 (F5)

### 方法2: コマンドラインから実行 (SQL Server認証が設定されている場合)

```batch
cd sql

# Windows認証の場合
sqlcmd -S localhost -E -i schema\01_create_database.sql
sqlcmd -S localhost -E -d LMEMetalSpreads -i schema\02_create_tables_v2.sql
sqlcmd -S localhost -E -d LMEMetalSpreads -i procedures\01_insert_procedures_v2.sql
sqlcmd -S localhost -E -d LMEMetalSpreads -i views\01_market_views_v2.sql

# SQL Server認証の場合
sqlcmd -S localhost -U ユーザー名 -P パスワード -i schema\01_create_database.sql
```

### 方法3: PowerShellスクリプトを使用

管理者権限でPowerShellを開いて実行：

```powershell
cd sql
.\execute_all_scripts.ps1
```

## 確認方法

すべてのスクリプトが正常に実行されたことを確認：

```sql
-- SSMSで以下のクエリを実行

USE LMEMetalSpreads;
GO

-- テーブル確認
SELECT TABLE_SCHEMA, TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- ビュー確認
SELECT TABLE_SCHEMA, TABLE_NAME 
FROM INFORMATION_SCHEMA.VIEWS
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- ストアドプロシージャ確認
SELECT SCHEMA_NAME(schema_id) as [Schema], name as [Procedure]
FROM sys.procedures
ORDER BY [Schema], [Procedure];

-- 初期データ確認
SELECT * FROM config.M_metals;
SELECT * FROM config.M_collection_config;
```

## 期待される結果

### テーブル (5個)
- config.M_collection_config
- config.M_metals
- market.M_spreads
- market.T_daily_summary
- market.T_tick_data

### ビュー (7個)
- config.v_collection_health
- market.v_active_spreads
- market.v_daily_summary
- market.v_latest_market_data
- market.v_price_history
- market.v_spread_type_summary
- market.v_todays_activity

### ストアドプロシージャ (5個)
- market.sp_BulkInsertTickData
- market.sp_CalculateDailySummary
- market.sp_GetActiveSpreads
- market.sp_InsertTickData
- market.sp_UpsertSpread

### 初期データ
- 6種類の金属設定 (CU, AL, ZN, PB, NI, SN)
- 3種類の収集設定 (REALTIME, REGULAR, DAILY)