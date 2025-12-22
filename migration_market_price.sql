-- 時価機能のためのデータベースマイグレーション

-- 1. m_メニューテーブルに「時価」カラムを追加
ALTER TABLE "m_メニュー" ADD COLUMN IF NOT EXISTS "時価" INTEGER NOT NULL DEFAULT 0;

-- 2. t_注文明細テーブルに「実際価格」カラムを追加
ALTER TABLE "t_注文明細" ADD COLUMN IF NOT EXISTS "実際価格" INTEGER;

-- 確認用クエリ
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'm_メニュー' AND column_name = '時価';

SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 't_注文明細' AND column_name = '実際価格';
