#!/usr/bin/env python3
"""
起動時にscheduled_dateカラムを追加するスクリプト
"""
import os
import psycopg2

# DATABASE_URLを取得
database_url = os.environ.get('DATABASE_URL')
if not database_url:
    print("DATABASE_URL not set")
    exit(0)

# postgres:// を postgresql:// に変換（psycopg2用）
if database_url.startswith('postgres://'):
    database_url = database_url.replace('postgres://', 'postgresql://', 1)

try:
    conn = psycopg2.connect(database_url)
    cur = conn.cursor()
    
    # scheduled_dateカラムを追加
    cur.execute('''
        ALTER TABLE "T_注文明細" 
        ADD COLUMN IF NOT EXISTS "売上計上日" TIMESTAMP WITH TIME ZONE;
    ''')
    
    conn.commit()
    print("✓ scheduled_date column added successfully")
    
except Exception as e:
    print(f"Error adding scheduled_date column: {e}")
finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
