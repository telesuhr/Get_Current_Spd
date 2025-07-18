# SQL スクリプト実行手順（更新版）

## Azure SQL Database / 制限された環境での実行方法

### 手順1: データベース作成

1. **SQL Server Management Studio (SSMS) を開く**

2. **master データベースに接続**
   - サーバー名: お使いのサーバー名
   - データベース: master
   - 認証: 適切な認証方法を選択

3. **データベース作成スクリプトを実行**
   ```sql
   -- ファイル: schema\01_create_database_fixed.sql
   IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'LMEMetalSpreads')
   BEGIN
       CREATE DATABASE LMEMetalSpreads;
   END
   GO
   ```

### 手順2: LMEMetalSpreads データベースに接続

1. **新しい接続を作成**
   - SSMSで「接続」→「データベースエンジン」を選択
   - サーバー名: 同じサーバー
   - **データベース名: LMEMetalSpreads** （オプション → 接続プロパティで指定）

2. **接続後、以下を確認**
   ```sql
   SELECT DB_NAME() AS CurrentDatabase;
   -- 結果が 'LMEMetalSpreads' であることを確認
   ```

### 手順3: 残りのスクリプトを実行

LMEMetalSpreadsデータベースに接続した状態で、以下の順序で実行：

1. **データベース設定**
   - ファイル: `schema\01b_configure_database.sql`

2. **テーブル作成**
   - ファイル: `schema\02_create_tables_v2.sql`

3. **ストアドプロシージャ作成**
   - ファイル: `procedures\01_insert_procedures_v2.sql`

4. **ビュー作成**
   - ファイル: `views\01_market_views_v2.sql`

## SSMSでの具体的な実行手順

### 方法A: クエリウィンドウで実行

1. SSMSで新しいクエリウィンドウを開く
2. データベースドロップダウンで「LMEMetalSpreads」を選択
3. 各SQLファイルの内容をコピー＆ペーストして実行（F5）

### 方法B: ファイルを開いて実行

1. SSMSで「ファイル」→「開く」→「ファイル」
2. SQLファイルを選択
3. データベースドロップダウンで「LMEMetalSpreads」を選択
4. 実行（F5）

## 確認用SQL

すべてのオブジェクトが作成されたことを確認：

```sql
-- 現在のデータベースを確認
SELECT DB_NAME() AS CurrentDatabase;

-- スキーマ確認
SELECT name FROM sys.schemas WHERE name IN ('market', 'config');

-- テーブル確認
SELECT 
    s.name AS SchemaName,
    t.name AS TableName
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name IN ('market', 'config')
ORDER BY s.name, t.name;

-- ビュー確認
SELECT 
    s.name AS SchemaName,
    v.name AS ViewName
FROM sys.views v
JOIN sys.schemas s ON v.schema_id = s.schema_id
WHERE s.name IN ('market', 'config')
ORDER BY s.name, v.name;

-- ストアドプロシージャ確認
SELECT 
    s.name AS SchemaName,
    p.name AS ProcedureName
FROM sys.procedures p
JOIN sys.schemas s ON p.schema_id = s.schema_id
WHERE s.name IN ('market', 'config')
ORDER BY s.name, p.name;

-- 初期データ確認
SELECT * FROM config.M_metals;
SELECT * FROM config.M_collection_config;
```

## トラブルシューティング

### "USE statement is not supported" エラーの場合
- 新しい接続を作成し、接続時にデータベースを指定
- または、クエリウィンドウ上部のデータベースドロップダウンで選択

### 権限エラーの場合
- db_owner または db_ddladmin ロールが必要
- CREATE DATABASE 権限が必要（データベース作成時）

### Azure SQL Database の場合
- エラスティックプールの制限を確認
- DTUまたはvCoreの制限内であることを確認