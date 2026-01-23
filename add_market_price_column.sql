-- Add market price column to M_メニュー table
ALTER TABLE "M_メニュー" ADD COLUMN IF NOT EXISTS "時価商品" BOOLEAN DEFAULT FALSE;

-- Add is_market_price column (English name) as well for compatibility
ALTER TABLE "M_メニュー" ADD COLUMN IF NOT EXISTS "is_market_price" BOOLEAN DEFAULT FALSE;

-- Sync the two columns if both exist
UPDATE "M_メニュー" SET "is_market_price" = "時価商品" WHERE "時価商品" IS NOT NULL;
UPDATE "M_メニュー" SET "時価商品" = "is_market_price" WHERE "is_market_price" IS NOT NULL;
