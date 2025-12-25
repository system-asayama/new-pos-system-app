# -*- coding: utf-8 -*-
# =============================================================================
# 単一ファイル POSレジ + 写真付きメニュー + QRセルフオーダー + 厨房/ドリンカー自動印刷 + スタッフ注文
# + 店舗/管理者/従業員 ログイン（ID/パスワード） + SaaS（テナント）対応 + 4階層権限
#   4階層ロール: sysadmin(システム管理者) > tenant_admin(テナント管理者) > store_admin(店舗管理者) > staff(従業員)
# =============================================================================

from __future__ import annotations
from sqlalchemy.orm import sessionmaker, joinedload

# ---- builtins / stdlib ----
import base64
import contextlib            # closing/suppress で使用
import hmac
import hashlib
import ipaddress             # プリンタ検出で使用
import io                    # QR生成で使用
import json
import logging
import math
import os
import queue                 # SSE 待機キューで使用
import re
import secrets               # ← 追加：トークン生成で使用
import socket
import subprocess
import tempfile
import threading            # 非同期印刷で使用
import time
import traceback
import uuid
from collections import defaultdict
import random  # ← 合流PIN生成用
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone  # ★ timezone を追加
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse     # _is_safe_url で使用

# ---- Flask ----
from flask import (
    Flask,
    Response,
    abort,
    current_app,
    flash,
    g,
    jsonify,
    make_response,
    redirect,
    request,
    send_from_directory,
    session,
    stream_with_context,
    url_for,
)
from flask import render_template as flask_render_template
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

# ---- SQLAlchemy ----
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    Float,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Index,
    inspect,
    text,
    func,
    event,
    exists,
    and_,
)
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError
from sqlalchemy.orm import (
    Session,
    declarative_base,
    relationship,
    sessionmaker,
    scoped_session,
    declarative_mixin,
    declared_attr,
    with_loader_criteria,
)

# ★ 追加：履歴作成で使用（既にOK）
from sqlalchemy import desc
from sqlalchemy import func as sa_func

# Flask純正の render_template をそのまま使う（上書き防止）
render_template = flask_render_template

# ---- zeroconf (optional) ----
try:
    from zeroconf import Zeroconf, ServiceBrowser, ServiceListener
    HAS_ZEROCONF = True
except ImportError:
    HAS_ZEROCONF = False
    Zeroconf = None
    ServiceBrowser = None
    ServiceListener = None



# ---------------------------------------------------------------------
# 明細の提供・取消・要作業 判定ヘルパ（★ここを “最初に1回だけ” 定義）
# ---------------------------------------------------------------------
# --- [ヘルパ] 取消アイテム判定（_is_cancel_item） ----------------------------------
def _is_cancel_item(it) -> bool:
    for nm in ("is_cancel", "is_cancelled", "cancelled"):
        if getattr(it, nm, None):
            return True
    raw = getattr(it, "status", None) or getattr(it, "item_status", None) or getattr(it, "state", None)
    if raw is None:
        return False
    s = str(raw).lower()
    return ("取消" in s) or ("ｷｬﾝｾﾙ" in s) or ("cancel" in s) or ("void" in s)

# --- [ヘルパ] 提供済みアイテム判定（_is_served_item） -------------------------------
def _is_served_item(it) -> bool:
    if any([
        getattr(it, "served", None),
        getattr(it, "is_served", None),
        getattr(it, "served_at", None),
        getattr(it, "provided", None),
        getattr(it, "is_provided", None),
        getattr(it, "provided_at", None),
    ]):
        return True
    try:
        qty = int(getattr(it, "qty", getattr(it, "数量", 0)) or 0)
        provided_qty = int(
            getattr(it, "served_qty", None)
            or getattr(it, "provided_qty", None)
            or getattr(it, "提供数量", None)
            or 0
        )
        if qty > 0 and provided_qty >= qty:
            return True
    except Exception:
        pass
    raw = getattr(it, "status", None) or getattr(it, "item_status", None) or getattr(it, "state", None) or ""
    s = str(raw).lower()
    return ("提供済" in s) or ("提供完了" in s) or ("served" in s) or ("done" in s) or ("completed" in s)

# --- [ヘルパ] 要調理・提供中アイテム判定（_needs_work_item） ------------------------
def _needs_work_item(it) -> bool:
    """提供済でも取消でもない＝会計不可アイテム（= 新規/調理中 など）"""
    try:
        qty = int(getattr(it, "qty", getattr(it, "数量", 0)) or 0)
    except Exception:
        qty = 0
    if qty <= 0:
        return False
    if _is_cancel_item(it):
        return False
    return not _is_served_item(it)

# --- [ヘルパ] 注文の実売上計算（取り消しを除外） ------------------------
def _calculate_order_totals(order_items):
    """
    OrderItemのリストから、取り消しを除外した実際の売上を計算
    
    Args:
        order_items: OrderItemのリスト
        
    Returns:
        dict: {"subtotal": int, "tax": int, "total": int}
    """
    import math
    subtotal = 0
    tax = 0
    
    for it in order_items:
        qty = int(getattr(it, "qty", 0) or 0)
        if qty == 0:
            continue
        
        # 取り消し判定（正数量かつ取消ラベルは除外、負数量は必ず集計）
        if qty > 0:
            st = (getattr(it, "status", None) or getattr(it, "状態", None) or "")
            st_low = str(st).lower()
            is_cancel_label = (
                ("取消" in st_low) or ("ｷｬﾝｾﾙ" in st_low) or ("キャンセル" in st_low)
                or ("cancel" in st_low) or ("void" in st_low)
            )
            if is_cancel_label:
                continue
        
        # 金額計算
        unit_price = int(getattr(it, "unit_price", 0) or 0)
        excl = unit_price * qty
        rate = float(getattr(it, "tax_rate", 0.0) or 0.0)
        item_tax = int(math.floor(excl * rate))
        
        subtotal += excl
        tax += item_tax
    
    total = subtotal + tax
    return {"subtotal": subtotal, "tax": tax, "total": total}


# -----------------------------------------------------------------------------
# 設定値（環境変数で上書き可）
# -----------------------------------------------------------------------------
APP_TITLE = "Simple POS with QR Ordering (SQLAlchemy)"
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///database.db")
# Heroku の postgres:// を SQLAlchemy 用に postgresql:// へ変換
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
QR_SECRET = os.getenv("QR_SECRET")
if not QR_SECRET:
    raise RuntimeError("QR_SECRET is required")
PRINT_DIR = os.getenv("PRINT_DIR", "prints")       # フォールバック出力先
POS_VERIFY_SCHEMA = os.getenv("POS_VERIFY_SCHEMA", "1") == "1"
POS_CREATE_TABLES = os.getenv("POS_CREATE_TABLES", "0") == "1"  # 明示時のみ自動作成

# 画像アップロード（SaaS内保管）設定
UPLOAD_DIR = os.getenv("UPLOAD_DIR", os.path.join(os.path.dirname(__file__), "uploads"))
os.makedirs(UPLOAD_DIR, exist_ok=True)
ALLOWED_IMAGE_EXTS = {"jpg", "jpeg", "png", "gif", "webp"}
MAX_UPLOAD_MB = 5
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

# 価格入力モード（税込/税抜）
PRICE_INPUT_MODE_DEFAULT = os.getenv("PRICE_INPUT_MODE", "excl").lower()  # "incl" | "excl"

# SaaS（マルチテナント）設定
MAIN_DOMAIN = os.getenv("MAIN_DOMAIN", "")  # 例: "example.com"（空ならサブドメイン解決を無効化）
DEFAULT_TENANT_SLUG = os.getenv("DEFAULT_TENANT_SLUG", "default")
MULTI_TENANT_MODE = os.getenv("MULTI_TENANT_MODE", "shared")  # "shared" | "db-per-tenant"
DATABASE_URL_TEMPLATE = os.getenv("DATABASE_URL_TEMPLATE", "sqlite:///tenants/{slug}.db")
SCHEMA_AUTOGEN = int(os.getenv("SCHEMA_AUTOGEN", "0"))  # 1で自動ALTERを許可

# -----------------------------------------------------------------------------
# Flask アプリ（※ app は最初に作る：decorator順序の NameError 回避）
# -----------------------------------------------------------------------------
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES  # 5MB
app.secret_key = os.getenv("FLASK_SECRET_KEY")
if not app.secret_key:
    raise RuntimeError("FLASK_SECRET_KEY is required")

# -----------------------------------------------------------------------------
# 初期設定
# -----------------------------------------------------------------------------
app.config['ALLOW_DIRECT_REFUND'] = False  # 返金ボタンを非表示
app.config['DEBUG_TOTALS'] = True          # 再集計デバッグON
app.config['DEBUG_CANCEL'] = True          # （必要なら）取消デバッグON


# -----------------------------------------------------------------------------
# ポリシー（UI制御フラグ）
# -----------------------------------------------------------------------------
app.config['ALLOW_DIRECT_REFUND'] = False  # ← 返金ボタンを初期で非表示に


# -----------------------------------------------------------------------------
# テンプレートにフラグを流す
# -----------------------------------------------------------------------------
@app.context_processor
def inject_policy_flags():
    from flask import current_app
    return {
        "ALLOW_DIRECT_REFUND": bool(current_app.config.get("ALLOW_DIRECT_REFUND", True))
    }


# -----------------------------------------------------------------------------------------------
#  変更検知のグローバル（フロア版数）＋ユーティリティ
# -----------------------------------------------------------------------------------------------
_floor_version = int(time.time() * 1000)
_floor_lock = threading.Lock()
_floor_waiters = []

# --- [ヘルパ] フロア更新通知（版数更新 & SSE 待機へ通知） --------------------------------
def mark_floor_changed():
    """フロア状態が変わったら呼ぶ（版数を前に進め、SSE 待機中の接続に通知）"""
    global _floor_version
    with _floor_lock:
        _floor_version = int(time.time() * 1000)
        for q in list(_floor_waiters):
            try:
                q.put_nowait(_floor_version)
            except Exception:
                pass

# ---------------------------------------------------------------------
# Jinja フィルタ登録（価格表示：¥12,345 形式）
# ---------------------------------------------------------------------
# --- [テンプレートフィルタ] 金額表示（円・カンマ区切り） --------------------------------
@app.template_filter("yen")
def yen(v) -> str:
    try:
        return f"¥{int(v):,}"
    except Exception:
        return "¥0"


# -----------------------------------------------------------------------------
# SQLAlchemy 初期化（SaaS共有DB/テナント別DB対応） 〔貼り替え版：SQL_ECHO/プーリング/簡易診断付き〕
# -----------------------------------------------------------------------------
# --- [共有DB] エンジンキャッシュ -------------------------------------------------------
_engine_cache = {}

# --- [DB接続] エンジン生成ヘルパ ------------------------------------------------------
def _create_engine(url: str):
    """
    - SQL_ECHO=1 で発行SQLを出力（デバッグ用）
    - pool_pre_ping=True で切断検知
    - SQLite / Postgres で適切な connect_args を付与
    """
    echo_env = os.getenv("SQL_ECHO", "0") == "1"

    engine_kwargs = {
        "echo": echo_env,
        "future": True,
        "pool_pre_ping": True,        # 接続死活監視
    }

    if url.startswith("sqlite:///"):
        engine_kwargs["connect_args"] = {"check_same_thread": False}
        # SQLite はプール設定を持たない（デフォルトOK）
    elif url.startswith("postgresql://"):
        # 短時間で切断される環境向けチューニング（Heroku最適化）
        engine_kwargs.setdefault("connect_args", {}).update({
            "sslmode": "require",
            "client_encoding": "utf8",  # UTF-8エンコーディングを明示
        })
        engine_kwargs.update({
            "pool_size": int(os.getenv("DB_POOL_SIZE", "3")),  # Heroku向けに削減
            "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "5")),  # 削減
            "pool_recycle": int(os.getenv("DB_POOL_RECYCLE", "300")),  # 5分に短縮
            "pool_timeout": int(os.getenv("DB_POOL_TIMEOUT", "10")),  # 10秒でタイムアウト
            "pool_pre_ping": True,  # 明示的に設定
        })

    eng = create_engine(url, **engine_kwargs)

    # 起動時に1回だけ簡易診断を出す（失敗してもアプリは継続）
    try:
        with eng.connect() as conn:
            dialect = conn.dialect.name
            ver = conn.exec_driver_sql("select 1").scalar()
            # 実行できたらOK（値は 1 のはず）
            print(f"[DB] connected dialect={dialect} echo={echo_env} ping={ver}")
    except Exception as e:
        print(f"[DB] connect test failed: {e}")

    return eng

# --- [DB接続] セッションファクトリ（共有モード / テナント別モード） -------------------
if MULTI_TENANT_MODE == "shared":
    # 共有モード: 単一エンジン
    engine = _create_engine(DATABASE_URL)
    SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True))
else:
    # db-per-tenant: テナントごとにエンジンを切替

    # --- [DB接続] テナント別：エンジン取得 -------------------------------------------
    def _get_engine_for_tenant(slug: str):
        if slug not in _engine_cache:
            url = DATABASE_URL_TEMPLATE.format(slug=slug)
            _engine_cache[slug] = _create_engine(url)
        return _engine_cache[slug]

    # --- [DB接続] テナント別：SessionLocal ファクトリ -------------------------------
    def SessionLocal():
        # 呼び出し時点の g.tenant に紐づくエンジンで Session を返す
        if not getattr(g, "tenant", None):
            raise RuntimeError("Tenant context missing in db-per-tenant mode.")
        eng = _get_engine_for_tenant(g.tenant["slug"])
        return scoped_session(sessionmaker(bind=eng, autoflush=False, autocommit=False, future=True))

# --- [ORM] Declarative Base ----------------------------------------------------------
Base = declarative_base()

# --- [DBユーティリティ] 常に scoped_session を返す -----------------------------------
def _scoped_session():
    """
    常に scoped_session を返すユーティリティ。
    - shared モード: SessionLocal は scoped_session -> そのまま返す
    - db-per-tenant モード: SessionLocal は関数 -> 呼び出して scoped_session を返す
    """
    return SessionLocal if hasattr(SessionLocal, "remove") else SessionLocal()

# --- [DBユーティリティ] セッション管理のコンテキストマネージャー -------------------
@contextmanager
def get_db_session():
    """
    データベースセッションを安全に管理するコンテキストマネージャー。
    エラー時に自動的にロールバックし、正常終了時にコミットする。
    
    使用例:
        with get_db_session() as s:
            # データベース操作
            s.add(obj)
            # コミットは自動的に行われる
    """
    if MULTI_TENANT_MODE == "shared":
        session = SessionLocal()
    else:
        scoped = SessionLocal()
        session = scoped()
    
    try:
        yield session
        # 正常終了時は自動コミットしない（明示的なコミットを推奨）
    except Exception as e:
        # エラー時は必ずロールバック
        try:
            session.rollback()
            app.logger.error(f"[DB] Transaction rolled back due to error: {e}")
        except Exception as rollback_error:
            app.logger.error(f"[DB] Rollback failed: {rollback_error}")
        raise
    finally:
        # セッションをクローズ
        try:
            session.close()
        except Exception as close_error:
            app.logger.error(f"[DB] Session close failed: {close_error}")

# --- [Flask] リクエスト後のクリーンアップ ---------------------------------------------
@app.teardown_appcontext
def shutdown_session(exception=None):
    """
    リクエスト終了時にデータベースセッションをクリーンアップ。
    接続プールの枯渇を防ぐため、必ず実行する。
    """
    try:
        # shared モードの場合
        if MULTI_TENANT_MODE == "shared":
            if hasattr(SessionLocal, 'remove'):
                # エラーが発生した場合はロールバック
                if exception is not None:
                    try:
                        # scoped_sessionから実際のセッションを取得してロールバック
                        session = SessionLocal()
                        session.rollback()
                        app.logger.info(f"[teardown] rolled back session due to exception: {exception}")
                    except Exception as rollback_error:
                        app.logger.error(f"[teardown] rollback failed: {rollback_error}")
                # セッションをクリーンアップ
                SessionLocal.remove()
        # db-per-tenant モードの場合
        else:
            if hasattr(g, 'tenant') and g.tenant:
                try:
                    scoped = SessionLocal()
                    if exception is not None:
                        try:
                            session = scoped()
                            session.rollback()
                            app.logger.info(f"[teardown] rolled back session due to exception: {exception}")
                        except Exception as rollback_error:
                            app.logger.error(f"[teardown] rollback failed: {rollback_error}")
                    scoped.remove()
                except Exception as tenant_error:
                    app.logger.error(f"[teardown] tenant session cleanup failed: {tenant_error}")
    except Exception as e:
        app.logger.error(f"[teardown] session cleanup failed: {e}")



# ---------------------------------------------------------------------
# 店舗IDの不足カラムを自動追加 + 必要最小限のインデックス +（任意）バックフィル
#  - 何度実行しても安全（存在チェックしてからALTER/UPDATE）
#  - SQLite / PostgreSQL 両対応
# ---------------------------------------------------------------------
# --- [ヘルパ] SQLite 方言かどうか判定 --------------------------------------------------
def _dialect_is_sqlite() -> bool:
    eng = _shared_engine_or_none()
    return bool(eng and str(eng.url).startswith("sqlite"))


# --- [ヘルパ] カラム存在チェック（DB方言対応） ------------------------------------------
def _column_exists(conn, table: str, col: str) -> bool:
    if _dialect_is_sqlite():
        rows = conn.execute(text(f'PRAGMA table_info("{table}")')).fetchall()
        return any(r[1] == col for r in rows)  # (cid, name, type, notnull, dflt, pk)
    else:
        q = text("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_name=:t AND column_name=:c
            LIMIT 1
        """)
        return conn.execute(q, {"t": table, "c": col}).first() is not None


# --- [ヘルパ] インデックス作成（存在しなければ） ----------------------------------------
def _create_index_if_missing(conn, table: str, col: str, idx_name: str):
    if _dialect_is_sqlite():
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table}"("{col}")'))
    else:
        exists = conn.execute(text("""
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'public' AND indexname = :n
        """), {"n": idx_name}).first()
        if not exists:
            conn.execute(text(f'CREATE INDEX "{idx_name}" ON "{table}"("{col}")'))


# --- [DDL] 店舗IDカラム追加（必要なら） -------------------------------------------------
def add_store_id_columns(create_indexes: bool = True):
    """
    店舗IDが必要なテーブルへ "店舗ID" INTEGER を追加（無ければ）。
    """
    eng = _shared_engine_or_none()
    if eng is None:
        return

    targets = [
        "M_テーブル",
        "M_プリンタ",
        "M_メニュー",
        "T_商品カテゴリ",
        "T_商品カテゴリ付与",
        "T_QRトークン",
        "T_印刷ルール",
        "T_注文",
        "T_注文明細",
        "T_支払記録",
        # M_支払方法 は店舗共通マスタ想定のため通常は付けない
    ]

    with eng.begin() as conn:
        for t in targets:
            # 既にテーブルが無ければスキップ
            try:
                if _dialect_is_sqlite():
                    _ = conn.execute(text(f'SELECT 1 FROM "{t}" LIMIT 0'))
                else:
                    _ = conn.execute(text(f'SELECT 1 FROM "{t}" LIMIT 0'))
            except Exception:
                continue

            if not _column_exists(conn, t, "店舗ID"):
                conn.execute(text(f'ALTER TABLE "{t}" ADD COLUMN "店舗ID" INTEGER'))
            if create_indexes:
                _create_index_if_missing(conn, t, "店舗ID", f'idx_{t}_店舗ID')


# --- [DML] 店舗IDの埋め戻し（参照関係に基づく） ----------------------------------------
def backfill_store_id(minimal: bool = True):
    """
    可能な範囲で "店舗ID" を埋める。
      - T_QRトークン   ← M_テーブル.店舗ID（テーブルID経由）
      - T_注文         ← M_テーブル.店舗ID（テーブルID経由）
      - T_注文明細     ← T_注文.店舗ID（注文ID経由）
      - T_支払記録     ← T_注文.店舗ID（注文ID経由）
      - T_印刷ルール   ← M_プリンタ.店舗ID（プリンタID経由）
    M_メニュー / T_商品カテゴリ / T_商品カテゴリ付与 は運用によりNULLのままでもOK。
    """
    eng = _shared_engine_or_none()
    if eng is None:
        return

    sqlite = _dialect_is_sqlite()
    with eng.begin() as conn:
        # T_QRトークン ← M_テーブル
        if sqlite:
            conn.execute(text("""
                UPDATE "T_QRトークン"
                SET "店舗ID" = (
                    SELECT "店舗ID" FROM "M_テーブル" t
                    WHERE t.id = "T_QRトークン"."テーブルID" LIMIT 1
                )
                WHERE "店舗ID" IS NULL
            """))
        else:
            conn.execute(text("""
                UPDATE "T_QRトークン" q
                SET "店舗ID" = t."店舗ID"
                FROM "M_テーブル" t
                WHERE q."店舗ID" IS NULL AND q."テーブルID" = t.id
            """))

        # T_注文 ← M_テーブル
        if sqlite:
            conn.execute(text("""
                UPDATE "T_注文"
                SET "店舗ID" = (
                    SELECT "店舗ID" FROM "M_テーブル" t
                    WHERE t.id = "T_注文"."テーブルID" LIMIT 1
                )
                WHERE "店舗ID" IS NULL
            """))
        else:
            conn.execute(text("""
                UPDATE "T_注文" o
                SET "店舗ID" = t."店舗ID"
                FROM "M_テーブル" t
                WHERE o."店舗ID" IS NULL AND o."テーブルID" = t.id
            """))

        # T_注文明細 ← T_注文
        if sqlite:
            conn.execute(text("""
                UPDATE "T_注文明細"
                SET "店舗ID" = (
                    SELECT "店舗ID" FROM "T_注文" o
                    WHERE o.id = "T_注文明細"."注文ID" LIMIT 1
                )
                WHERE "店舗ID" IS NULL
            """))
        else:
            conn.execute(text("""
                UPDATE "T_注文明細" d
                SET "店舗ID" = o."店舗ID"
                FROM "T_注文" o
                WHERE d."店舗ID" IS NULL AND d."注文ID" = o.id
            """))

        # T_支払記録 ← T_注文
        if sqlite:
            conn.execute(text("""
                UPDATE "T_支払記録"
                SET "店舗ID" = (
                    SELECT "店舗ID" FROM "T_注文" o
                    WHERE o.id = "T_支払記録"."注文ID" LIMIT 1
                )
                WHERE "店舗ID" IS NULL
            """))
        else:
            conn.execute(text("""
                UPDATE "T_支払記録" p
                SET "店舗ID" = o."店舗ID"
                FROM "T_注文" o
                WHERE p."店舗ID" IS NULL AND p."注文ID" = o.id
            """))

        # T_印刷ルール ← M_プリンタ
        if sqlite:
            conn.execute(text("""
                UPDATE "T_印刷ルール"
                SET "店舗ID" = (
                    SELECT "店舗ID" FROM "M_プリンタ" pr
                    WHERE pr.id = "T_印刷ルール"."プリンタID" LIMIT 1
                )
                WHERE "店舗ID" IS NULL
            """))
        else:
            conn.execute(text("""
                UPDATE "T_印刷ルール" r
                SET "店舗ID" = pr."店舗ID"
                FROM "M_プリンタ" pr
                WHERE r."店舗ID" IS NULL AND r."プリンタID" = pr.id
            """))

    if minimal:
        return
    # ここに、必要であればメニュー/カテゴリ群の埋め方（店舗複製 or 店舗共通NULL維持）を追加してください。


# -----------------------------------------------------------------------------
# 店舗IDマスター管理機能
# -----------------------------------------------------------------------------

# --- 店舗IDマスター存在保証（未登録なら自動登録） -----------------------------
def ensure_store_id_in_master(store_code: str, store_name: str = None) -> int:
    """
    店舗IDマスターに店舗コードが存在することを保証し、店舗IDを返す
    存在しない場合は自動で登録する
    """
    s = SessionLocal()
    try:
        # 既存の店舗IDを検索
        existing = s.query(M_店舗IDマスター).filter(
            M_店舗IDマスター.店舗コード == store_code
        ).first()
        
        if existing:
            return existing.店舗ID
        
        # 存在しない場合は新規登録
        new_store = M_店舗IDマスター(
            店舗コード=store_code,
            店舗名=store_name or store_code,
            有効フラグ=1,
            備考="自動登録"
        )
        s.add(new_store)
        s.commit()
        s.refresh(new_store)
        
        app.logger.info(f"店舗IDマスターに新規登録: 店舗ID={new_store.店舗ID}, 店舗コード={store_code}")
        return new_store.店舗ID
        
    except Exception as e:
        s.rollback()
        app.logger.error(f"店舗IDマスター登録エラー: {e}")
        raise
    finally:
        s.close()


# --- 次の店舗IDの採番取得 -----------------------------------------------------
def get_next_store_id() -> int:
    """次の店舗IDを取得（自動採番）"""
    s = SessionLocal()
    try:
        max_id = s.query(func.max(M_店舗IDマスター.店舗ID)).scalar()
        return (max_id or 0) + 1
    finally:
        s.close()


# --- 店舗IDの有効性検証（有効フラグ=1で存在するか） ---------------------------
def validate_store_id(store_id: int) -> bool:
    s = SessionLocal()
    try:
        app.logger.info("[validate_store_id] lookup 店舗ID=%r", store_id)
        store = s.query(M_店舗IDマスター).filter(
            M_店舗IDマスター.店舗ID == store_id,
            M_店舗IDマスター.有効フラグ == 1
        ).first()
        app.logger.info("[validate_store_id] result=%s", bool(store))
        return store is not None
    finally:
        s.close()


# --- 店舗スコーピング保証（列追加＆最小バックフィル）※冪等 --------------------
def ensure_store_scoping():
    """
    1) "店舗ID" 列の付与（なければ追加）
    2) 可能なバックフィル
    ※ 何度呼んでも安全
    """
    add_store_id_columns()
    backfill_store_id(minimal=True)



# -----------------------------------------------------------------------------
# 共通ユーティリティ
# -----------------------------------------------------------------------------

# --- 現在時刻を文字列で取得（YYYY-MM-DD HH:MM:SS） ----------------------------
def now_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# --- 署名生成（HMAC-SHA256 → URL-safe Base64、末尾'='除去） -------------------
def sign_payload(payload: str) -> str:
    sig = hmac.new(QR_SECRET.encode(), payload.encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).decode().rstrip('=')


# --- 画像ファイルの拡張子許可チェック ------------------------------------------
def allowed_image(filename: str) -> bool:
    if "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in ALLOWED_IMAGE_EXTS


# --- 価格入力モードの取得（cookie: incl/excl、未設定は既定値） ------------------
def get_price_input_mode():
    mode = request.cookies.get("price_mode", "").lower() if request else ""
    return mode if mode in ("incl", "excl") else PRICE_INPUT_MODE_DEFAULT



# -----------------------------------------------------------------------------
# SaaS: テナントモデル + スコープ Mixin + 自動フィルタ/埋め込み
# -----------------------------------------------------------------------------

# --- テナントモデル（M_テナント） ------------------------------------------------
class M_テナント(Base):
    __tablename__ = "M_テナント"
    id = Column(Integer, primary_key=True)
    名称 = Column(String(200), nullable=False)
    slug = Column(String(100), nullable=False, unique=True)
    作成日時 = Column(DateTime(timezone=True), server_default=func.now())
    更新日時 = Column(DateTime(timezone=True), onupdate=func.now())


# --- テナントスコープ Mixin（tenant_id 自動列） ----------------------------------
@declarative_mixin
class TenantScoped:
    @declared_attr
    def tenant_id(cls):
        return Column(Integer, index=True)  # 参照整合は運用で担保（FKでもOK）


# --- 現在テナントIDの取得（g > session の順） -----------------------------------
def _current_tenant_id():
    # g.tenant_id が最優先・なければ session から
    return getattr(g, "tenant_id", None) or session.get("tenant_id")


# --- 4階層ロール定義とヘルパ ----------------------------------------------------
ROLE_LEVELS = {"staff": 1, "store_admin": 2, "tenant_admin": 3, "sysadmin": 4}

# --- ロールレベル数値化 ----------------------------------------------------------
def role_level():
    r = (session.get("role") or "").strip()
    return ROLE_LEVELS.get(r, 0)

# --- ロール閾値以上か判定 --------------------------------------------------------
def has_role_at_least(min_role: str) -> bool:
    return role_level() >= ROLE_LEVELS.get(min_role, 99)

# --- sysadmin 判定 ---------------------------------------------------------------
def is_sysadmin() -> bool:
    return (session.get("role") == "sysadmin")

# --- tenant_admin 以上か判定 -----------------------------------------------------
def is_tenant_admin_or_higher() -> bool:
    return has_role_at_least("tenant_admin")

from sqlalchemy import func

# --- store_admin 以上か判定（動的昇格: M_管理者 登録でも可） ---------------------
def is_store_admin_or_higher() -> bool:
    # まずはロールレベルで判定（store_admin/tenant_admin/sysadmin など）
    if has_role_at_least("store_admin"):
        return True

    # ここから動的昇格: staff などのロールでも、同一店舗で M_管理者 に登録されていれば OK
    store_id = session.get("store_id")
    login_id = session.get("login_id")
    if not (store_id and login_id):
        return False

    s = SessionLocal()
    try:
        exists = (
            s.query(Admin.id)
             .filter(
                 Admin.store_id == int(store_id),
                 func.lower(Admin.login_id) == func.lower(str(login_id).strip()),
                 Admin.active == 1
             )
             .first()
        )
        return exists is not None
    finally:
        s.close()


# --- SELECT時のテナント自動フィルタ（sysadmin は免除） ---------------------------
@event.listens_for(Session, "do_orm_execute")
def _apply_tenant_filter(execute_state):
    """SELECT時、TenantScoped を継承する全モデルに tenant_id 絞り込みを自動付与
       ただし sysadmin は免除（全体横断を許可）
    """
    if not execute_state.is_select:
        return
    # sysadmin 免除
    try:
        from flask import session as _flask_session
        if _flask_session.get("role") == "sysadmin":
            return
    except Exception:
        pass

    tenant_id = _current_tenant_id()
    if tenant_id is None:
        return
    for mapper in list(Base.registry.mappers):
        cls = mapper.class_
        try:
            if issubclass(cls, TenantScoped):
                execute_state.statement = execute_state.statement.options(
                    with_loader_criteria(
                        cls,
                        lambda ent: ent.tenant_id == tenant_id,
                        include_aliases=True
                    )
                )
        except TypeError:
            pass


# --- INSERT時の tenant_id 自動スタンプ ------------------------------------------
@event.listens_for(Session, "before_flush")
def _stamp_tenant_id(session_obj, flush_context, instances):
    """INSERT時、TenantScoped の新規行に tenant_id を自動セット（安全＆静粛）"""
    tid = _current_tenant_id()
    if tid in (None, 0, ""):
        return
    try:
        tid = int(tid)
    except Exception:
        return

    for obj in list(session_obj.new):
        # 既に値があれば尊重
        if isinstance(obj, TenantScoped):
            cur = getattr(obj, "tenant_id", None)
            if cur in (None, 0, ""):
                setattr(obj, "tenant_id", tid)


# --- INSERT時の store_id 自動付与（DB操作なし・単純代入のみ） -------------------
@event.listens_for(Session, "before_flush")
def _auto_set_store_id(session_obj, flush_context, instances):
    """
    INSERT時、store_id フィールドを持つ新規オブジェクトに対して、
    Flaskセッションの store_id を【単純付与】するだけに限定する。
    ※ ここでは DB アクセス（validate/ensure/commit/close 等）は一切しない。
       flush 中の別セッション操作は IllegalStateChange を誘発するため。
    """
    # Flask セッションから store_id を取得
    try:
        from flask import session as flask_session
        sid = flask_session.get("store_id")
    except Exception:
        return

    if sid in (None, "", 0):
        return

    # 数値に寄せる（失敗したら付与しない）
    try:
        sid = int(sid)
    except Exception:
        try:
            app.logger.warning("[auto_set_store_id] store_id not int: %r", sid)
        except Exception:
            pass
        return

    # new 行にだけ付与（既に値があれば尊重）
    try:
        new_objs = list(session_obj.new)
        try:
            app.logger.debug("[auto_set_store_id] new_objs=%d sid=%s", len(new_objs), sid)
        except Exception:
            pass
    except Exception:
        new_objs = list(session_obj.new)

    for obj in new_objs:
        if not hasattr(obj, "store_id"):
            continue
        cur = getattr(obj, "store_id", None)
        if cur in (None, "", 0):
            try:
                setattr(obj, "store_id", sid)
                try:
                    app.logger.debug("[auto_set_store_id] set store_id=%s for %s",
                                     sid, obj.__class__.__name__)
                except Exception:
                    pass
            except Exception as e:
                try:
                    import traceback
                    app.logger.error("[auto_set_store_id] set failed for %s: %s\n%s",
                                     obj.__class__.__name__, e, traceback.format_exc())
                except Exception:
                    pass
                continue


# --- テナント解決（slug の決定ロジック） ----------------------------------------
def _resolve_tenant_slug():
    """
    解決順:
      1) パス: /t/<slug>/... の <slug>（url_value_preprocessorで設定）
      2) ヘッダ: X-Tenant: <slug>
      3) サブドメイン: <slug>.MAIN_DOMAIN
      4) セッション: session['tenant_slug']
      5) DEFAULT_TENANT_SLUG
    """
    if getattr(g, "path_tenant_slug", None):
        return g.path_tenant_slug
    hdr = request.headers.get("X-Tenant")
    if hdr:
        return hdr.strip()
    if MAIN_DOMAIN:
        host = request.host.split(":")[0]
        if host.endswith(MAIN_DOMAIN) and host != MAIN_DOMAIN:
            sub = host[: -(len(MAIN_DOMAIN) + 1)]
            if sub:
                return sub
    if session.get("tenant_slug"):
        return session["tenant_slug"]
    return DEFAULT_TENANT_SLUG


# --- 共有モード用: エンジン取得（shared以外はNone） -----------------------------
def _shared_engine_or_none():
    if MULTI_TENANT_MODE != "shared":
        return None
    return engine


# --- データベースマイグレーション ---------------------------------------------
def run_migrations():
    """アプリケーション起動時にデータベースマイグレーションを実行"""
    print("[MIGRATION] Running database migrations...")
    
    eng = _shared_engine_or_none()
    if eng is None:
        print("[MIGRATION] No database engine available (not in shared mode), skipping migrations.")
        return
    
    with eng.begin() as conn:
        # 時価機能用のカラムを追加
        try:
            conn.execute(text('ALTER TABLE "m_メニュー" ADD COLUMN IF NOT EXISTS "時価" INTEGER NOT NULL DEFAULT 0'))
            print("[MIGRATION] Added column '時価' to m_メニュー table.")
        except Exception as e:
            print(f"[MIGRATION] Failed to add '時価' column: {e}")
        
        try:
            conn.execute(text('ALTER TABLE "t_注文明細" ADD COLUMN IF NOT EXISTS "実際価格" INTEGER'))
            print("[MIGRATION] Added column '実際価格' to t_注文明細 table.")
        except Exception as e:
            print(f"[MIGRATION] Failed to add '実際価格' column: {e}")
    
    print("[MIGRATION] Database migrations completed.")


# --- テナント行の読取（shared前提） ---------------------------------------------
def _load_tenant_row(db):
    slug = _resolve_tenant_slug()
    row = db.execute(
        text('SELECT id, "名称", slug FROM "M_テナント" WHERE slug = :slug'),
        {"slug": slug}
    ).mappings().first()
    return row


# --- リクエスト前処理：テナント解決＆自己修復ブートストラップ -------------------
@app.before_request
def _before_request_tenant():
    """
    各リクエストでテナントを解決し、g.tenant / g.tenant_id をセット。
    - "M_テナント" テーブル未作成（UndefinedTable）でも自己修復する
    """
    scoped = _scoped_session()
    tenant = None

    # 1) まず読みに行く（UndefinedTable も拾う）
    try:
        with scoped() as s:
            tenant = _load_tenant_row(s)  # sharedモード前提。db-per-tenantは適宜変更。
    except (OperationalError, ProgrammingError):
        # relation "m_テナント" does not exist 等 → 初期ブートストラップへ
        pass
    finally:
        try:
            scoped.remove()
        except Exception:
            pass

    # 2) 見つからない/例外時はブートストラップ
    if tenant is None:
        try:
            eng = _shared_engine_or_none()
            if eng is not None:
                insp = inspect(eng)
                existing_tables = set(insp.get_table_names())
                if "M_テナント" not in existing_tables:
                    # モデルからテーブル単体作成（他テーブルは作らない）
                    M_テナント.__table__.create(bind=eng)
        except Exception:
            pass

        scoped = _scoped_session()
        try:
            with scoped() as s:
                exists = s.execute(
                    text('SELECT 1 FROM "M_テナント" WHERE slug = :slug'),
                    {"slug": DEFAULT_TENANT_SLUG}
                ).first()
                if not exists:
                    s.execute(
                        text('INSERT INTO "M_テナント"("名称", slug) VALUES(:name, :slug)'),
                        {"name": "Default Tenant", "slug": DEFAULT_TENANT_SLUG}
                    )
                    s.commit()
        finally:
            try:
                scoped.remove()
            except Exception:
                pass

        scoped = _scoped_session()
        try:
            with scoped() as s:
                tenant = _load_tenant_row(s)
        finally:
            try:
                scoped.remove()
            except Exception:
                pass

    if tenant is None:
        abort(500, description="Tenant bootstrap failed: \"M_テナント\" table/row not available.")

    g.tenant = {"id": tenant["id"], "名称": tenant["名称"], "slug": tenant["slug"]}
    g.tenant_id = tenant["id"]
    session["tenant_id"] = tenant["id"]
    session["tenant_slug"] = tenant["slug"]


# --- URL からの <tenant_slug> を g にコピー（pop しない） -----------------------
@app.url_value_preprocessor
def pull_tenant_slug(endpoint, values):
    if values and "tenant_slug" in values:
        # ★ 引数を消さない：pop → 参照のみに変更
        g.path_tenant_slug = values["tenant_slug"]


# --- テナント一時解除コンテキスト（集計・メンテ用） -----------------------------
@contextmanager
def without_tenant():
    prev_gid = getattr(g, "tenant_id", None)
    try:
        g.tenant_id = None
        yield
    finally:
        g.tenant_id = prev_gid


# --- テーブルに列が存在するか確認（DB方言ごと） --------------------------------
def _table_has_column(conn, table_name, col_name):
    if DATABASE_URL.startswith("sqlite"):
        info = conn.execute(text(f"PRAGMA table_info('{table_name}')")).fetchall()
        cols = [r[1] for r in info]
        return col_name in cols
    else:
        res = conn.execute(text("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name=:t AND column_name=:c
        """), {"t": table_name, "c": col_name}).first()
        return bool(res)


# --- 共有DBの各テーブルへ tenant_id 列を自動付与（SCHEMA_AUTOGEN=1時のみ） -----
def ensure_tenant_columns():
    """SCHEMA_AUTOGEN=1 のときだけ、TenantScoped想定テーブルに tenant_id を追加"""
    if not SCHEMA_AUTOGEN:
        return
    scoped = _scoped_session()
    try:
        with scoped() as s:
            conn = s.connection()
            tables = []
            if DATABASE_URL.startswith("sqlite"):
                rows = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
                tables = [r[0] for r in rows if not r[0].startswith("sqlite_")]
            else:
                rows = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")).fetchall()
                tables = [r[0] for r in rows]
            for t in tables:
                if t == "M_テナント":
                    continue
                if not _table_has_column(conn, t, "tenant_id"):
                    conn.execute(text(f'ALTER TABLE "{t}" ADD COLUMN "tenant_id" INTEGER'))
            s.commit()
    finally:
        try:
            scoped.remove()
        except Exception:
            pass


# -----------------------------------------------------------------------------
# 価格/税率ヘルパ
# -----------------------------------------------------------------------------

# --- 税抜→税込（表示ロジック：税込 = 税抜 + floor(税抜×税率)） ----------------
def display_price_incl_from_excl(price_excl: int, rate: float) -> int:
    """税込 = 税抜 + floor(税抜×税率) を Decimal で厳密に"""
    from decimal import Decimal, ROUND_DOWN

    def _to_rate(val):
        # 税率正規化: 10 -> 0.10, 8 -> 0.08, 0.1 -> 0.1
        try:
            r = Decimal(str(val))
        except Exception:
            return Decimal('0.10')
        if r > 1:
            r = r / Decimal('100')
        if r < 0:
            r = Decimal('0')
        if r > 1:
            r = Decimal('1')
        return r

    pe = Decimal(price_excl)
    r  = _to_rate(rate)
    tax = (pe * r).quantize(Decimal('1'), rounding=ROUND_DOWN)
    return int(pe + tax)


# --- フォームから実効税率を決定（カテゴリ順優先→無ければ既定税率） --------------
def effective_tax_rate_from_form(f) -> float:
    from decimal import Decimal

    def _to_rate(val):
        try:
            r = Decimal(str(val))
        except Exception:
            return Decimal('0.10')
        if r > 1:
            r = r / Decimal('100')
        if r < 0:
            r = Decimal('0')
        if r > 1:
            r = Decimal('1')
        return r

    cat_ids    = f.getlist("cat_id[]")
    cat_orders = f.getlist("cat_order[]")
    cat_taxes  = f.getlist("cat_tax[]") if "cat_tax[]" in f else []
    rows = []
    for idx, cid in enumerate(cat_ids):
        if not cid:
            continue
        try:
            order = int(cat_orders[idx]) if idx < len(cat_orders) else 0
        except Exception:
            order = 0
        tax_val = None
        if idx < len(cat_taxes):
            raw = (cat_taxes[idx] or "").strip()
            if raw != "":
                try:
                    tax_val = float(_to_rate(raw))
                except Exception:
                    tax_val = None
        rows.append({"order": order, "tax": tax_val})

    rows.sort(key=lambda x: x["order"])
    for r in rows:
        if r["tax"] is not None:
            return float(r["tax"])
    try:
        return float(_to_rate(f.get("税率", 0.10)))
    except Exception:
        return 0.10


# --- 保存用の標準化（DBは税抜保持／税込入力時は割戻し整合） -------------------
def normalize_price_for_storage(input_price: int, mode: str, effective_rate: float) -> tuple[int, int | None]:
    """
    DB の Menu.price は税抜で保持。
    - mode == 'excl'（税抜入力）: そのまま整数化（half-up）
    - mode == 'incl'（税込入力）: 「表示ロジック(税込=税抜+floor(税抜×税率))」に
      一致する最小の税抜を求めて保存し、税込もその結果に揃える
    """
    from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP

    def _to_rate(val):
        try:
            r = Decimal(str(val))
        except Exception:
            return Decimal('0.10')
        if r > 1:
            r = r / Decimal('100')
        if r < 0:
            r = Decimal('0')
        if r > 1:
            r = Decimal('1')
        return r

    if mode == "incl":
        r = _to_rate(effective_rate)
        # まずは下駄をはかずに割り戻し（下限の候補）
        e = int((Decimal(input_price) / (Decimal('1') + r)).quantize(Decimal('1'), rounding=ROUND_DOWN))

        # 表示ロジックで税込を再計算
        def _incl_from_excl(excl: int) -> int:
            return display_price_incl_from_excl(excl, float(r))

        incl2 = _incl_from_excl(e)
        # 目標税込に届かない場合は、届くまで税抜を繰り上げ
        # （10%なら最大でも+1で収束。一般の税率でも数回以内）
        while incl2 < input_price:
            e += 1
            incl2 = _incl_from_excl(e)

        return int(e), int(incl2)

    # 税抜入力時：通常どおり
    return int(Decimal(input_price).quantize(Decimal('1'), rounding=ROUND_HALF_UP)), None


# --- メニュー表示用の実効税率解決（カテゴリ優先→メニュー既定） ----------------
def resolve_effective_tax_rate_for_menu(session_db, menu_id: int, menu_default_rate: float) -> float:
    """表示時に使う実効税率。カテゴリ→メニュー既定の順で拾い、どちらも正規化して返す。"""
    from decimal import Decimal

    def _to_rate(val):
        try:
            r = Decimal(str(val))
        except Exception:
            return Decimal('0.10')
        if r > 1:
            r = r / Decimal('100')
        if r < 0:
            r = Decimal('0')
        if r > 1:
            r = Decimal('1')
        return r

    links = (session_db.query(ProductCategoryLink)
             .filter(ProductCategoryLink.product_id == menu_id)
             .order_by(ProductCategoryLink.display_order.asc(), ProductCategoryLink.category_id.asc())
             .all())
    for ln in links:
        if ln.tax_rate is not None:
            return float(_to_rate(ln.tax_rate))
    return float(_to_rate(menu_default_rate or 0.0))




# ---------------------------------------------------------------------
# システム管理者（アプリ全体・TenantScoped ではない）
# ---------------------------------------------------------------------
# --- [モデル] システム管理者（SysAdmin） -------------------------------------------------
class SysAdmin(Base):
    __tablename__ = "M_システム管理者"
    id = Column(Integer, primary_key=True, autoincrement=True)
    login_id = Column("ログインID", String, nullable=False, unique=True)
    password_hash = Column("パスワードハッシュ", String, nullable=False)
    name = Column("氏名", String, nullable=False)
    active = Column("有効", Integer, nullable=False, default=1)
    last_login = Column("最終ログイン", String)
    created_at = Column("作成日時", String, nullable=False, default=now_str)
    updated_at = Column("更新日時", String, nullable=False, default=now_str)


# ---------------------------------------------------------------------
# テナント管理者（TenantScoped = テナント単位）
# ---------------------------------------------------------------------
# --- [モデル] テナント管理者（TenantAdmin） ----------------------------------------------
class TenantAdmin(TenantScoped, Base):
    __tablename__ = "M_テナント管理者"
    id = Column(Integer, primary_key=True, autoincrement=True)
    login_id = Column("ログインID", String, nullable=False)
    password_hash = Column("パスワードハッシュ", String, nullable=False)
    name = Column("氏名", String, nullable=False)
    active = Column("有効", Integer, nullable=False, default=1)
    last_login = Column("最終ログイン", String)
    created_at = Column("作成日時", String, nullable=False, default=now_str)
    updated_at = Column("更新日時", String, nullable=False, default=now_str)
    __table_args__ = (UniqueConstraint("tenant_id", "ログインID", name="uq_tenant_admin_login"),)


# ---------------------------------------------------------------------
# 店舗IDマスター（店舗IDの一意性を保証し、自動採番を行う）
# ---------------------------------------------------------------------
# --- [モデル] 店舗IDマスター（M_店舗IDマスター） --------------------------------------------
class M_店舗IDマスター(Base):
    """店舗IDマスター：店舗IDの一意性を保証し、自動採番を行う"""
    __tablename__ = "M_店舗IDマスター"
    
    店舗ID = Column("店舗ID", Integer, primary_key=True, autoincrement=True)
    店舗コード = Column("店舗コード", String(50), unique=True, nullable=False)
    店舗名 = Column("店舗名", String(200), nullable=False)
    有効フラグ = Column("有効フラグ", Integer, nullable=False, default=1)  # 1:有効, 0:無効
    作成日時 = Column("作成日時", DateTime(timezone=True), server_default=func.now())
    更新日時 = Column("更新日時", DateTime(timezone=True), onupdate=func.now())
    備考 = Column("備考", Text)


# --- [モデル] 店舗（Store） ---------------------------------------------------------------
class Store(TenantScoped, Base):
    __tablename__ = "M_店舗"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column("店舗コード", String, unique=True, nullable=False)  # 例: "tokyo-main"
    name = Column("名称", String, nullable=False)
    active = Column("有効", Integer, nullable=False, default=1)

    # --- 合流PIN必須フラグ -----------------------------------------------------------
    # このフラグが 1 の場合、来店客が注文に合流する際に 4 桁の PIN コードを入力する必要があります。
    # 0 の場合は PIN 入力をスキップし、最初の来店客以外も自動的に注文に参加できます。
    # デフォルトは 1 (必須) としており、従来の動作との後方互換を維持します。
    require_join_pin = Column("合流PIN必須", Integer, nullable=False, default=1)

    # --- レシート・領収書用情報 -----------------------------------------------------------
    address = Column("住所", Text, nullable=True)  # 店舗の住所
    phone = Column("電話番号", String, nullable=True)  # 店舗の電話番号
    registration_number = Column("登録番号", String, nullable=True)  # インボイス登録番号
    business_hours = Column("営業時間", String, nullable=True)  # 営業時間
    receipt_footer = Column("レシートフッター", Text, nullable=True)  # レシート下部のメッセージ

    created_at = Column("作成日時", String, nullable=False, default=now_str)
    updated_at = Column("更新日時", String, nullable=False, default=now_str)

    admins = relationship("Admin", back_populates="store", cascade="all,delete-orphan")
    employees = relationship("Employee", back_populates="store", cascade="all,delete-orphan")


# Store.code にユニークインデックスを貼る（クラスの外に書く必要がある）
Index("idx_store_code", Store.code, unique=True)



# --- [モデル] 店舗管理者（Admin） ---------------------------------------------------------
class Admin(TenantScoped, Base):
    # 既存の "管理者" は「店舗管理者（store_admin）」として運用
    __tablename__ = "M_管理者"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    login_id = Column("ログインID", String, nullable=False)
    password_hash = Column("パスワードハッシュ", String, nullable=False)
    name = Column("氏名", String, nullable=False)
    active = Column("有効", Integer, nullable=False, default=1)
    last_login = Column("最終ログイン", String)
    created_at = Column("作成日時", String, nullable=False, default=now_str)
    updated_at = Column("更新日時", String, nullable=False, default=now_str)
    __table_args__ = (UniqueConstraint("店舗ID", "ログインID", name="uq_admin_store_login"),)
    store = relationship("Store", back_populates="admins")

Index("idx_admin_store", Admin.store_id)


# --- [モデル] 従業員（Employee） ----------------------------------------------------------
class Employee(TenantScoped, Base):
    __tablename__ = "M_従業員"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    login_id = Column("ログインID", String, nullable=False)
    password_hash = Column("パスワードハッシュ", String, nullable=False)
    name = Column("氏名", String, nullable=False)
    active = Column("有効", Integer, nullable=False, default=1)
    role = Column("ロール", String, nullable=False, default="staff")  # staff / lead など
    last_login = Column("最終ログイン", String)
    created_at = Column("作成日時", String, nullable=False, default=now_str)
    updated_at = Column("更新日時", String, nullable=False, default=now_str)
    __table_args__ = (UniqueConstraint("店舗ID", "ログインID", name="uq_emp_store_login"),)
    store = relationship("Store", back_populates="employees")

Index("idx_emp_store", Employee.store_id)


# --- [モデル] メニュー（Menu） ------------------------------------------------------------
class Menu(TenantScoped, Base):
    __tablename__ = "M_メニュー"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    name = Column("名称", String, nullable=False)
    price = Column("価格", Integer, nullable=False)  # 税抜で保存
    price_incl = Column("税込価格", Integer)  # 修正: 税込価格を保存する列を追加
    photo_url = Column("写真URL", Text)
    description = Column("説明", Text)
    available = Column("提供可否", Integer, nullable=False, default=1)  # 1/0
    tax_rate = Column("税率", Float, nullable=False, default=0.10)     # フォールバック税率
    is_market_price = Column("時価", Integer, nullable=False, default=0)  # 0=通常価格, 1=時価
    display_order = Column("表示順", Integer, nullable=False, default=0)
    created_at = Column("作成日時", String, nullable=False, default=now_str)
    updated_at = Column("更新日時", String, nullable=False, default=now_str)
    store = relationship("Store")
    options = relationship("ProductOption", secondary="M_商品オプション適用", back_populates="products")

    # 論理削除フラグ: 0=未削除, 1=削除済
    # 提供停止とは別に管理し、削除済みメニューは注文画面や通常の管理一覧に表示されません。
    # DB側に is_deleted 列が存在している前提で定義します。
    is_deleted = Column(Integer, nullable=False, default=0)

    # 削除日時: 削除操作が行われた時刻を保存します。復活時には None に戻します。
    deleted_at = Column(DateTime, nullable=True)


# --- [モデル] テーブル（TableSeat） -------------------------------------------------------
class TableSeat(TenantScoped, Base):
    __tablename__ = "M_テーブル"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    table_no = Column("テーブル番号", String, nullable=False)
    status = Column("状態", String, nullable=False, default="空席")  # 空席/着席/会計中/清掃中
    note = Column("備考", Text)
    store = relationship("Store")
    orders = relationship("OrderHeader", back_populates="table", cascade="all,delete-orphan")
    __table_args__ = (UniqueConstraint("店舗ID", "テーブル番号"),)


# --- [モデル] QRトークン（QrToken） -------------------------------------------------------
class QrToken(TenantScoped, Base):
    __tablename__ = "T_QRトークン"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    table_id = Column("テーブルID", Integer, ForeignKey("M_テーブル.id", ondelete="SET NULL"), nullable=True)
    token = Column("トークン", String, nullable=False, unique=True)
    expires_at = Column("有効期限", String)  # '%Y-%m-%d %H:%M:%S'
    issued_at = Column("発行日時", String, nullable=False, default=now_str)
    store = relationship("Store")
    table = relationship("TableSeat")


# --- [モデル] 注文ヘッダ（OrderHeader） ---------------------------------------------------
class OrderHeader(TenantScoped, Base):
    __tablename__ = "T_注文"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    session_token = Column("セッショントークン", String, unique=True, index=True, nullable=True)

    table_id = Column("テーブルID", Integer, ForeignKey("M_テーブル.id"), nullable=False)
    status = Column("状態", String, nullable=False, default="新規")  # 新規/調理中/提供済/会計中/会計済/取消
    subtotal = Column("小計", Integer, nullable=False, default=0)
    tax = Column("税額", Integer, nullable=False, default=0)
    total = Column("合計", Integer, nullable=False, default=0)
    opened_at = Column("開始日時", String, nullable=False, default=now_str)
    closed_at = Column("会計日時", String)
    note = Column("備考", Text)
    # ★ 合流PIN（4桁）と有効期限（文字列）
    join_pin = Column("合流PIN", String, nullable=True)
    join_pin_expires_at = Column("PIN有効期限", String, nullable=True)
    store = relationship("Store")
    table = relationship("TableSeat", back_populates="orders")
    items = relationship("OrderItem", back_populates="order", cascade="all,delete-orphan")

Index("idx_order_table", OrderHeader.table_id)


# --- [モデル] 注文明細（OrderItem） -------------------------------------------------------
class OrderItem(TenantScoped, Base):
    __tablename__ = "T_注文明細"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    order_id = Column("注文ID", Integer, ForeignKey("T_注文.id", ondelete="CASCADE"), nullable=False)
    menu_id = Column("メニューID", Integer, ForeignKey("M_メニュー.id"), nullable=False)
    qty = Column("数量", Integer, nullable=False)
    unit_price = Column("単価", Integer, nullable=False)  # 税抜
    tax_rate = Column("税率", Float, nullable=False)     # 実際に適用した税率
    actual_price = Column("実際価格", Integer, nullable=True)  # 時価商品の場合、会計時に入力された実際の価格
    memo = Column("メモ", Text)
    status = Column("状態", String, nullable=False, default="新規")  # 新規/調理中/提供済/取消
    added_at = Column("追加日時", DateTime(timezone=True), nullable=False, default=lambda: datetime.utcnow())
    store = relationship("Store")
    order = relationship("OrderHeader", back_populates="items")
    menu = relationship("Menu")

Index("idx_order_detail_order", OrderItem.order_id)


# --- [モデル] 商品カテゴリ（Category） ----------------------------------------------------
class Category(TenantScoped, Base):
    __tablename__ = "T_商品カテゴリ"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    parent_id = Column("親カテゴリID", Integer, ForeignKey("T_商品カテゴリ.id"))
    name = Column("名称", String, nullable=False)
    display_order = Column("表示順", Integer, default=0)
    active = Column("有効", Integer, nullable=False, default=1)
    created_at = Column("作成日時", String, nullable=False, default=now_str)
    updated_at = Column("更新日時", String, nullable=False, default=now_str)
    store = relationship("Store")
    parent = relationship("Category", remote_side=[id])
    __table_args__ = (UniqueConstraint("店舗ID", "親カテゴリID", "名称"),)

Index("idx_cat_parent", Category.parent_id)


# --- [モデル] 商品カテゴリ付与（ProductCategoryLink） -------------------------------------
class ProductCategoryLink(TenantScoped, Base):
    __tablename__ = "T_商品カテゴリ付与"
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    product_id = Column("商品ID", Integer, ForeignKey("M_メニュー.id", ondelete="CASCADE"), primary_key=True)
    category_id = Column("カテゴリID", Integer, ForeignKey("T_商品カテゴリ.id", ondelete="CASCADE"), primary_key=True)
    display_order = Column("表示順", Integer, nullable=False, default=0)        # そのカテゴリ内での表示順
    tax_rate = Column("税率", Float)                                            # そのカテゴリでの税率（NULL=未指定）
    assigned_at = Column("付与日時", String, nullable=False, default=now_str)
    store = relationship("Store")

Index("idx_prodcat_cat", ProductCategoryLink.category_id)


# --- [モデル] プリンタ（Printer） ---------------------------------------------------------
class Printer(TenantScoped, Base):
    __tablename__ = "M_プリンタ"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    name = Column("名称", String, nullable=False)
    kind = Column("種別", String, nullable=False)  # 'escpos_tcp' | 'cups' | 'windows'
    connection = Column("接続情報", String, nullable=False)
    width = Column("幅文字", Integer, default=42)
    enabled = Column("有効", Integer, nullable=False, default=1)
    store = relationship("Store")


# --- [モデル] 印刷ルール（PrintRule） ------------------------------------------------------
class PrintRule(TenantScoped, Base):
    __tablename__ = "T_印刷ルール"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    category_id = Column("カテゴリID", Integer, ForeignKey("T_商品カテゴリ.id"))
    menu_id = Column("メニューID", Integer, ForeignKey("M_メニュー.id"))
    printer_id = Column("プリンタID", Integer, ForeignKey("M_プリンタ.id"), nullable=False)
    store = relationship("Store")
    __table_args__ = (UniqueConstraint("店舗ID", "カテゴリID", "メニューID", "プリンタID"),)


# --- [モデル] 支払方法（PaymentMethod） ----------------------------------------------------
class PaymentMethod(TenantScoped, Base):
    __tablename__ = "M_支払方法"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    code = Column("コード", String, nullable=False)  # 例: CASH / CARD / QR / IC
    name = Column("名称", String, nullable=False)                 # 表示名
    active = Column("有効", Integer, nullable=False, default=1)
    display_order = Column("表示順", Integer, nullable=False, default=0)
    created_at = Column("作成日時", String, nullable=False, default=now_str)
    updated_at = Column("更新日時", String, nullable=False, default=now_str)
    store = relationship("Store")
    __table_args__ = (UniqueConstraint("店舗ID", "コード"),)

Index("idx_payment_method_order", PaymentMethod.display_order)


# --- [モデル] 支払記録（PaymentRecord） ----------------------------------------------------
class PaymentRecord(TenantScoped, Base):
    __tablename__ = "T_支払記録"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    order_id = Column("注文ID", Integer, ForeignKey("T_注文.id", ondelete="CASCADE"), nullable=False)
    method_id = Column("支払方法ID", Integer, ForeignKey("M_支払方法.id", ondelete="RESTRICT"), nullable=False)
    amount = Column("金額", Integer, nullable=False)
    paid_at = Column("支払日時", String, nullable=False, default=now_str)
    note = Column("メモ", Text)
    store = relationship("Store")


# --- [モデル] 商品オプション（ProductOption） -------------------------------------------
class ProductOption(TenantScoped, Base):
    __tablename__ = "M_商品オプション"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    option_name = Column("オプション名", String, nullable=False)
    display_order = Column("表示順", Integer, nullable=False, default=0)
    required = Column("必須", Integer, nullable=False, default=0)  # 0=任意, 1=必須
    multiple = Column("複数選択可", Integer, nullable=False, default=0)  # 0=単一選択, 1=複数選択可
    active = Column("有効", Integer, nullable=False, default=1)
    created_at = Column("作成日時", String, nullable=False, default=now_str)
    updated_at = Column("更新日時", String, nullable=False, default=now_str)
    store = relationship("Store")
    choices = relationship("OptionChoice", back_populates="option", cascade="all,delete-orphan")
    products = relationship("Menu", secondary="M_商品オプション適用", back_populates="options")


# --- [モデル] 商品オプション適用（中間テーブル） -------------------------------------------
class ProductOptionApply(TenantScoped, Base):
    __tablename__ = "M_商品オプション適用"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    option_id = Column("オプションID", Integer, ForeignKey("M_商品オプション.id", ondelete="CASCADE"), nullable=False)
    product_id = Column("商品ID", Integer, ForeignKey("M_メニュー.id", ondelete="CASCADE"), nullable=False)
    store = relationship("Store")

Index("idx_product_option_apply_option", ProductOptionApply.option_id)
Index("idx_product_option_apply_product", ProductOptionApply.product_id)


# --- [モデル] オプション選択肢（OptionChoice） -------------------------------------------
class OptionChoice(TenantScoped, Base):
    __tablename__ = "M_オプション選択肢"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    option_id = Column("オプションID", Integer, ForeignKey("M_商品オプション.id", ondelete="CASCADE"), nullable=False)
    choice_name = Column("選択肢名", String, nullable=False)
    extra_price = Column("追加料金", Integer, nullable=False, default=0)
    display_order = Column("表示順", Integer, nullable=False, default=0)
    active = Column("有効", Integer, nullable=False, default=1)
    created_at = Column("作成日時", String, nullable=False, default=now_str)
    updated_at = Column("更新日時", String, nullable=False, default=now_str)
    store = relationship("Store")
    option = relationship("ProductOption", back_populates="choices")

Index("idx_option_choice_option", OptionChoice.option_id)


# --- [モデル] 注文オプション（OrderOption） -----------------------------------------------
class OrderOption(TenantScoped, Base):
    __tablename__ = "T_注文オプション"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id", ondelete="CASCADE"), nullable=False)
    order_item_id = Column("注文明細ID", Integer, ForeignKey("T_注文明細.id", ondelete="CASCADE"), nullable=False)
    option_id = Column("オプションID", Integer, ForeignKey("M_商品オプション.id"), nullable=False)
    choice_id = Column("選択肢ID", Integer, ForeignKey("M_オプション選択肢.id"), nullable=False)
    extra_price = Column("追加料金", Integer, nullable=False, default=0)  # 注文時の追加料金（履歴保持）
    store = relationship("Store")
    order_item = relationship("OrderItem")
    option = relationship("ProductOption")
    choice = relationship("OptionChoice")

Index("idx_order_option_item", OrderOption.order_item_id)


# -----------------------------------------------------------------------------
# カテゴリ ユーティリティ
# -----------------------------------------------------------------------------

# --- 階層付きカテゴリ一覧の取得（DFS整形） --------------------------------------
def fetch_categories_with_depth(session_db):
    cats = session_db.query(Category).order_by(Category.parent_id, Category.display_order, Category.name).all()
    children_map, roots = {}, []
    for c in cats:
        children_map.setdefault(c.parent_id, []).append(c)
        if not c.parent_id:
            roots.append(c)
    out = []
    def dfs(node, depth):
        out.append({"id": node.id, "name": node.name, "depth": int(depth)})
        for ch in sorted(children_map.get(node.id, []), key=lambda x:(x.display_order, x.name)):
            dfs(ch, depth+1)
    for r in sorted(roots, key=lambda x:(x.display_order, x.name)):
        dfs(r, 1)
    return out


# --- 祖先/子孫判定（ancestor_id の子孫か？） -----------------------------------
def is_descendant(session_db, ancestor_id: int, node_id: int) -> bool:
    cur = session_db.get(Category, node_id)
    while cur and cur.parent_id:
        if cur.parent_id == ancestor_id:
            return True
        cur = session_db.get(Category, cur.parent_id)
    return False


# --- カテゴリの深さを取得（ルートからの階層数） ---------------------------------
def get_depth(session_db, category_id: int) -> int:
    """
    指定されたカテゴリIDの深さ（階層レベル）を返す。
    ルートカテゴリ（parent_id=None）の深さは1。
    """
    depth = 0
    cur = session_db.get(Category, category_id)
    while cur:
        depth += 1
        if cur.parent_id:
            cur = session_db.get(Category, cur.parent_id)
        else:
            break
    return depth


# --- 商品に紐づくカテゴリのパス一覧を構築（ルート→葉の名称列） -------------------
def build_category_paths_for_product(session_db, product_id: int) -> list[list[str]]:
    all_cats = session_db.query(Category).all()
    by_id = {c.id: c for c in all_cats}
    def path_for(cat_id: int) -> list[str]:
        names, cur = [], by_id.get(cat_id)
        while cur:
            names.append(cur.name)
            cur = by_id.get(cur.parent_id) if cur.parent_id else None
        names.reverse()
        return names
    links = (session_db.query(ProductCategoryLink)
             .filter(ProductCategoryLink.product_id == product_id)
             .order_by(ProductCategoryLink.display_order.asc(), ProductCategoryLink.category_id.asc())
             .all())
    paths = []
    for ln in links:
        if ln.category_id:
            paths.append(path_for(int(ln.category_id)))
    return paths


# -----------------------------------------------------------------------------
# 商品オプション ユーティリティ
# -----------------------------------------------------------------------------

# --- 商品に紐づくオプション一覧を取得 -----------------------------------------
def get_product_options(session_db, product_id: int, store_id: int) -> list[dict]:
    """
    指定された商品に適用されるオプション一覧を取得する。
    中間テーブル（M_商品オプション適用）を使用して、商品に紐付けられたオプションを取得。
    
    Returns:
        [
            {
                "id": 1,
                "name": "割り方",
                "required": True,
                "multiple": False,
                "choices": [
                    {"id": 1, "name": "水割り", "price": 0},
                    {"id": 2, "name": "お湯割り", "price": 0},
                    ...
                ]
            },
            ...
        ]
    """
    # 中間テーブル経由で商品に適用されているオプションを取得
    options = (session_db.query(ProductOption)
               .join(ProductOptionApply, ProductOption.id == ProductOptionApply.option_id)
               .filter(
                   ProductOptionApply.product_id == product_id,
                   ProductOption.store_id == store_id,
                   ProductOption.active == 1
               )
               .order_by(ProductOption.display_order.asc(), ProductOption.id.asc())
               .all())
    
    result = []
    for opt in options:
        # 有効な選択肢のみ取得
        choices = (session_db.query(OptionChoice)
                   .filter(
                       OptionChoice.option_id == opt.id,
                       OptionChoice.active == 1
                   )
                   .order_by(OptionChoice.display_order.asc(), OptionChoice.id.asc())
                   .all())
        
        if choices:  # 選択肢がある場合のみ追加
            result.append({
                "id": opt.id,
                "name": opt.option_name,
                "required": bool(opt.required),
                "multiple": bool(opt.multiple),
                "choices": [
                    {
                        "id": ch.id,
                        "name": ch.choice_name,
                        "price": ch.extra_price
                    }
                    for ch in choices
                ]
            })
    
    return result


# --- 注文明細に紐づくオプション情報を取得 -------------------------------------
def get_order_item_options(session_db, order_item_id: int) -> list[dict]:
    """
    注文明細に紐づくオプション情報を取得する。
    
    Returns:
        [
            {
                "option_name": "割り方",
                "choice_name": "お湯割り",
                "extra_price": 0
            },
            ...
        ]
    """
    order_options = (session_db.query(OrderOption)
                     .filter(OrderOption.order_item_id == order_item_id)
                     .all())
    
    result = []
    for oo in order_options:
        opt = session_db.get(ProductOption, oo.option_id)
        choice = session_db.get(OptionChoice, oo.choice_id)
        if opt and choice:
            result.append({
                "option_name": opt.option_name,
                "choice_name": choice.choice_name,
                "extra_price": oo.extra_price
            })
    
    return result


# --- オプションの追加料金合計を計算 -------------------------------------------
def calculate_option_total_price(session_db, option_selections: list[dict]) -> int:
    """
    選択されたオプションの追加料金合計を計算する。
    
    Args:
        option_selections: [{"option_id": 1, "choice_id": 2}, ...]
    
    Returns:
        追加料金の合計
    """
    total = 0
    for sel in option_selections:
        choice = session_db.get(OptionChoice, sel.get("choice_id"))
        if choice:
            total += choice.extra_price
    return total


# -----------------------------------------------------------------------------
# スキーマ検証 / 自動作成（テーブル存在のみ確認）
# -----------------------------------------------------------------------------
REQUIRED_TABLES = [
    "M_テナント",
    "M_メニュー", "M_テーブル", "T_QRトークン", "T_注文", "T_注文明細",
    "T_商品カテゴリ", "T_商品カテゴリ付与", "M_プリンタ", "T_印刷ルール",
    "M_店舗", "M_管理者", "M_従業員",
    "M_支払方法", "T_支払記録",
    # 追加（4階層）
    "M_システム管理者",
    "M_テナント管理者",
    # 店舗IDマスター
    "M_店舗IDマスター",
    # 商品オプション機能
    "M_商品オプション", "M_商品オプション適用", "M_オプション選択肢", "T_注文オプション",
]

# --- 必要テーブルの検証と自動作成（モード別） -----------------------------------
def verify_schema_or_create():
    eng = _shared_engine_or_none()
    if eng is None:
        return  # db-per-tenant はテナント作成時に個別実行する想定
    insp = inspect(eng)
    existing = set(insp.get_table_names())
    missing = [t for t in REQUIRED_TABLES if t not in existing]
    if missing:
        if POS_CREATE_TABLES:
            Base.metadata.create_all(bind=eng)
        elif POS_VERIFY_SCHEMA:
            raise RuntimeError(
                "必要なテーブルが見つかりません。既存DB参照モードのため起動を停止します。\n"
                f"不足テーブル: {missing}\n"
                f"DATABASE_URL: ***hidden***\n"
                "※自動作成したい場合は環境変数 POS_CREATE_TABLES=1 を設定してください。"
            )


# -----------------------------------------------------------------------------
# KDSカテゴリ用テーブル作成（IF NOT EXISTS）＋存在確認ログ
# -----------------------------------------------------------------------------

# --- KDSカテゴリ関連テーブルの作成＆存在ログ出力 --------------------------------
def ensure_kds_category_tables():
    with engine.begin() as conn:
        # M_KDSカテゴリ
        # データベース種類を判定してAUTOINCREMENT構文を切り替え
        dialect = conn.dialect.name
        if dialect == 'sqlite':
            id_col = "id INTEGER PRIMARY KEY AUTOINCREMENT"
        else:
            # PostgreSQL: SERIAL または GENERATED ALWAYS AS IDENTITY
            id_col = "id SERIAL PRIMARY KEY"
        
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS "M_KDSカテゴリ" (
            {id_col},
            名称 TEXT NOT NULL,
            表示順 INTEGER DEFAULT 0,
            有効 INTEGER DEFAULT 1,
            tenant_id INTEGER,
            "店舗ID" INTEGER,
            登録日時 TEXT,
            更新日時 TEXT
        )"""))
        conn.execute(text(
            'CREATE INDEX IF NOT EXISTS idx_kds_cat_store '
            'ON "M_KDSカテゴリ"("店舗ID", 有効, 表示順)'
        ))

        # R_KDSカテゴリ_メニュー（多対多）
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS "R_KDSカテゴリ_メニュー" (
            {id_col},
            kds_category_id INTEGER NOT NULL,
            menu_id INTEGER NOT NULL,
            tenant_id INTEGER,
            "店舗ID" INTEGER,
            登録日時 TEXT,
            UNIQUE (kds_category_id, menu_id, "店舗ID", tenant_id)
        )"""))
        conn.execute(text(
            'CREATE INDEX IF NOT EXISTS idx_kds_map_store '
            'ON "R_KDSカテゴリ_メニュー"("店舗ID", kds_category_id, menu_id)'
        ))

        # 存在確認ログ
        if dialect == 'sqlite':
            exists_cat = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='M_KDSカテゴリ'"
            )).scalar()
            exists_map = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='R_KDSカテゴリ_メニュー'"
            )).scalar()
        else:
            # PostgreSQL
            exists_cat = conn.execute(text(
                "SELECT table_name FROM information_schema.tables WHERE table_name='M_KDSカテゴリ'"
            )).scalar()
            exists_map = conn.execute(text(
                "SELECT table_name FROM information_schema.tables WHERE table_name='R_KDSカテゴリ_メニュー'"
            )).scalar()
        try:
            current_app.logger.info(
                f'KDS tables: M_KDSカテゴリ={bool(exists_cat)} / R_KDSカテゴリ_メニュー={bool(exists_map)}'
            )
        except Exception:
            # appコンテキスト外でも落ちないように
            pass



# -----------------------------------------------------------------------------
# 軽量マイグレーション（不足カラム/新テーブルの追加）
# -----------------------------------------------------------------------------

# --- 軽量マイグレーション本体（不足カラム/新テーブルの追加） -------------------
def migrate_schema_if_needed():
    eng = _shared_engine_or_none()
    if eng is None:
        return

    insp = inspect(eng)
    tables = set(insp.get_table_names())

    with eng.begin() as conn:
        # T_商品カテゴリ付与の不足カラム
        if "T_商品カテゴリ付与" in tables:
            cols = {c["name"] for c in insp.get_columns("T_商品カテゴリ付与")}
            if "表示順" not in cols:
                conn.exec_driver_sql('ALTER TABLE "T_商品カテゴリ付与" ADD COLUMN "表示順" INTEGER DEFAULT 0')
            if "税率" not in cols:
                conn.exec_driver_sql('ALTER TABLE "T_商品カテゴリ付与" ADD COLUMN "税率" FLOAT')
        
        # M_店舗のレシート・領収書用情報の不足カラム
        if "M_店舗" in tables:
            cols = {c["name"] for c in insp.get_columns("M_店舗")}
            if "住所" not in cols:
                conn.exec_driver_sql('ALTER TABLE "M_店舗" ADD COLUMN "住所" TEXT')
            if "電話番号" not in cols:
                conn.exec_driver_sql('ALTER TABLE "M_店舗" ADD COLUMN "電話番号" TEXT')
            if "登録番号" not in cols:
                conn.exec_driver_sql('ALTER TABLE "M_店舗" ADD COLUMN "登録番号" TEXT')
            if "営業時間" not in cols:
                conn.exec_driver_sql('ALTER TABLE "M_店舗" ADD COLUMN "営業時間" TEXT')
            if "レシートフッター" not in cols:
                conn.exec_driver_sql('ALTER TABLE "M_店舗" ADD COLUMN "レシートフッター" TEXT')

        # 既存ロジック：主要マスター群が1つでも無ければ metadata 全体を作成
        need_new = False
        for t in ["M_店舗", "M_管理者", "M_従業員", "M_テナント", "M_支払方法", "T_支払記録",
                  "M_システム管理者", "M_テナント管理者", "M_店舗IDマスター"]:
            if t not in tables:
                need_new = True
        if need_new:
            Base.metadata.create_all(bind=eng)

        # ---- 追加：中核テーブルを “個別” に補完（存在しないものだけ）----
        # ※ Model 定義の __tablename__ と一致するキー名で参照されます
        #    例: class M_メニュー(Base): __tablename__ = "M_メニュー"
        core_needed = ["M_メニュー", "T_注文", "T_注文明細", "M_テーブル"]
        # 最新のテーブル一覧を取り直す（今 create_all したかもしれないため）
        tables = set(inspect(eng).get_table_names())

        # 欠けているものだけ個別に create（checkfirst=True で安全）
        for t in core_needed:
            if t not in tables:
                tbl = Base.metadata.tables.get(t)
                if tbl is not None:
                    try:
                        tbl.create(bind=eng, checkfirst=True)
                        try:
                            current_app.logger.info(f"created missing core table: {t}")
                        except Exception:
                            pass
                    except Exception as _e:
                        try:
                            current_app.logger.warning(f"create table {t} failed: {_e}")
                        except Exception:
                            pass

        # （デバッグ）今の metadata に載っているテーブル名を一度だけログ
        try:
            current_app.logger.info(f"metadata tables = {list(Base.metadata.tables.keys())}")
        except Exception:
            pass


# --- 起動時スキーマ整備（Gunicorn import時にも実行） ---------------------------
try:
    verify_schema_or_create()     # 必要なテーブルが無ければ作成（POS_CREATE_TABLES=1 の時のみ）
    migrate_schema_if_needed()    # 既存DBに対して不足テーブル/カラムを追加
    ensure_tenant_columns()       # 許可されている場合のみ、tenant_id列の自動追加（SCHEMA_AUTOGEN=1）
    ensure_store_scoping()
    ensure_kds_category_tables()  # ★ KDSカテゴリ用テーブルを必ず用意

    # 参照しているSQLiteファイルの絶対パスをログ（相対パス取り違え対策）
    try:
        current_app.logger.info(f"DB url = {engine.url}")
        if engine.url.database:
            import os
            current_app.logger.info(f"DB file = {os.path.abspath(engine.url.database)}")
    except Exception:
        pass

except Exception as e:
    app.logger.warning(f"Schema init warning: {e}")



# ===== 起動時: T_お客様詳細履歴 の不足カラムを自動追加 =====
from sqlalchemy import inspect, text

def ensure_customer_detail_history_columns(engine):
    insp = inspect(engine)
    tbl = "T_お客様詳細履歴"
    names = set(insp.get_table_names())
    # テーブル自体が無ければ、モデル定義がある前提で create_all で作成
    if tbl not in names:
        try:
            T_お客様詳細履歴.__table__.create(bind=engine, checkfirst=True)  # type: ignore[name-defined]
        except Exception:
            pass

    # もう一度列を取得
    cols = {c["name"] for c in inspect(engine).get_columns(tbl)}
    with engine.begin() as conn:
        # ★今回のエラー: 合計人数 が無い
        if "合計人数" not in cols:
            conn.execute(text('ALTER TABLE "T_お客様詳細履歴" ADD COLUMN "合計人数" INTEGER DEFAULT 0'))

        # 念のため 他の列も不足なら追加（任意）
        if "version" not in cols:
            conn.execute(text('ALTER TABLE "T_お客様詳細履歴" ADD COLUMN "version" INTEGER NOT NULL DEFAULT 1'))
        if "変更理由" not in cols:
            conn.execute(text('ALTER TABLE "T_お客様詳細履歴" ADD COLUMN "変更理由" TEXT'))
        if "作成者" not in cols:
            conn.execute(text('ALTER TABLE "T_お客様詳細履歴" ADD COLUMN "作成者" TEXT'))
        if "created_at" not in cols:
            conn.execute(text('ALTER TABLE "T_お客様詳細履歴" ADD COLUMN "created_at" DATETIME'))

# ===== 起動時: 不足カラムを自動追加 =====
def auto_add_missing_columns(engine, Base):
    """
    Compare SQLAlchemy model definitions with the actual database schema and
    automatically add any missing columns using ALTER TABLE statements.

    Args:
        engine: SQLAlchemy Engine bound to the target database.
        Base: Declarative base containing model metadata.
    """
    insp = inspect(engine)
    # Iterate through all tables defined in metadata
    for table_name, table in Base.metadata.tables.items():
        # Skip if the table does not exist in the database
        if table_name not in insp.get_table_names():
            continue
        # Determine the set of existing column names
        db_columns = {c["name"] for c in insp.get_columns(table_name)}
        for column in table.columns:
            # If a column defined in the model is missing in the database, add it
            if column.name not in db_columns:
                col_name = column.name
                col_type = column.type.compile(engine.dialect)
                default_clause = ""
                if column.default is not None:
                    try:
                        default_value = column.default.arg
                        if isinstance(default_value, str):
                            default_clause = f" DEFAULT '{default_value}'"
                        else:
                            default_clause = f" DEFAULT {default_value}"
                    except Exception:
                        pass
                nullable_clause = " NOT NULL" if not column.nullable else ""
                sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_type}{default_clause}{nullable_clause}'
                try:
                    with engine.begin() as conn:
                        conn.execute(text(sql))
                except Exception as e:
                    # ログ出力は current_app が利用可能な場合のみ試みる
                    try:
                        current_app.logger.warning(f"[auto_add_missing_columns] Failed to add column {table_name}.{col_name}: {e}")
                    except Exception:
                        pass

# Execute the auto migration on startup for the shared engine if available
try:
    # Only perform auto migration if we have a global engine (shared mode)
    if 'engine' in globals() and engine is not None:
        auto_add_missing_columns(engine, Base)
except Exception as _e:
    try:
        current_app.logger.warning(f"auto_add_missing_columns execution failed: {_e}")
    except Exception:
        pass



# -----------------------------------------------------------------------------
# 印刷関連ユーティリティ
# -----------------------------------------------------------------------------

# --- 注文伝票テキストの生成（KITCHEN等のタイトル・幅・テーブル番号を含む） -----
def build_ticket(header: 'OrderHeader', details: list['OrderItem'], table: 'TableSeat', width: int = 42, title: str = "KITCHEN") -> str:
    """
    注文伝票のテキストを生成する。
    """
    pad = lambda s: (s[:width]).ljust(width)
    hr  = "-" * width
    lines = []

    # --- 詳細デバッグログの追加 ---
    app.logger.info(f"DEBUG: build_ticketに渡されたtableオブジェクト: {table}")
    
    header_id = getattr(header, 'id', 'N/A')
    
    # 💡 table_noの値が0やNoneの場合に備えてフォールバックを強化
    table_no = getattr(table, 'table_no', None)
    
    # 取得したtable_noの値が0である場合に、Noneとして扱うことでフォールバックロジックを強制的に実行させる
    if table_no is not None and (isinstance(table_no, int) and table_no == 0):
        table_no = None
    elif table_no is not None and (isinstance(table_no, str) and table_no.strip() == "0"):
        table_no = None

    app.logger.info(f"DEBUG: tableオブジェクトから取得されたtable_no: {table_no}")
    
    if not table_no:
        table_id_for_debug = getattr(header, 'table_id', 'N/A')
        app.logger.warning(f"WARNING: table_noが不正なため、フォールバックを使用します。header.table_id: {table_id_for_debug}")
        table_no_str = f"不明なテーブル (ID:{table_id_for_debug})"
    else:
        table_no_str = str(table_no)
    
    opened_at = getattr(header, 'opened_at', 'N/A')

    lines.append(pad(f"[{title}] ORDER #{header_id}  TABLE {table_no_str}"))
    lines.append(pad(f"TIME: {opened_at}"))
    lines.append(hr)

    # 注文明細の追加
    if details:
        for d in details:
            # メニュー名、数量、メモを安全に取得
            menu_name = getattr(getattr(d, 'menu', None), 'name', f"不明なメニュー (ID:{getattr(d, 'menu_id', 'N/A')})")
            qty = int(getattr(d, 'qty', 0))
            memo = getattr(d, 'memo', "")

            lines.append(pad(f"{menu_name}  x{qty}"))
            if memo:
                lines.append(pad(f"  * {memo}"))
    else:
        # 明細が存在しない場合に表示するメッセージ
        lines.append(pad("--- 注文明細はありません ---"))

    lines.append(hr)
    lines.append(pad("Printed by Simple POS"))
    
    return "\n".join(lines) + "\n\n"


# --- ESC/POS（TCP）プリンタへの印刷 --------------------------------------------
def print_escpos_tcp(text, conn_str):
    host, port = re.sub(r'^tcp://', '', conn_str).split(':')
    port = int(port)
    data = text.encode('cp932', errors='replace')
    ESC_INIT = b'\x1b\x40'
    CUT_FULL = b'\x1d\x56\x00'
    with socket.create_connection((host, port), timeout=2) as s:
        s.sendall(ESC_INIT + data + b'\n\n\n' + CUT_FULL)


# --- CUPS 経由の印刷 -----------------------------------------------------------
def print_cups(text, conn_str):
    printer = conn_str.replace('cups://','')
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt", mode="w", encoding="utf-8") as f:
        f.write(text)
        path = f.name
    try:
        subprocess.run(["lp", "-d", printer, path], check=True)
    finally:
        try: os.unlink(path)
        except: pass


# --- Windows プリンタ（win32print）での印刷 ------------------------------------
def print_windows(text, conn_str):
    printer = conn_str.replace('win://','')
    try:
        import win32print
        h = win32print.OpenPrinter(printer)
        try:
            hJob = win32print.StartDocPrinter(h, 1, ("POS Ticket", None, "RAW"))
            win32print.StartPagePrinter(h)
            win32print.WritePrinter(h, text.encode('cp932', errors='replace'))
            win32print.EndPagePrinter(h)
            win32print.EndDocPrinter(h)
        finally:
            win32print.ClosePrinter(h)
    except Exception:
        write_print_fallback(text, f"windows_{printer}")


# --- 失敗時のフォールバック：テキストをファイルに保存 --------------------------
def write_print_fallback(text: str, name_prefix="ticket"):
    os.makedirs(PRINT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    path = os.path.join(PRINT_DIR, f"{name_prefix}_{ts}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


# --- プリンタ種別に応じた印刷ディスパッチ（常に内容をファイル保存も実施） -------
def dispatch_print(printer_row: 'Printer' | None, text: str):
    """
    指定されたプリンタ情報に基づいて、適切な印刷処理を呼び出す。
    印刷が失敗した場合は、フォールバックとして内容をファイルに保存する。
    """
    # 💡 常にプリンタへ送信されるデータをファイルに保存する
    write_print_fallback(text, "sent_to_printer")

    if not printer_row:
        write_print_fallback(text, "noprinter")
        return

    kind = getattr(printer_row, 'kind', 'unknown')
    conn_str = getattr(printer_row, 'connection', 'N/A')

    try:
        if kind == "escpos_tcp":
            print_escpos_tcp(text, conn_str)
        elif kind == "cups":
            print_cups(text, conn_str)
        elif kind == "windows":
            print_windows(text, conn_str)
        else:
            logging.warning(f"未知のプリンタ種別です: '{kind}'. プリンタID: {printer_row.id}, 名称: {printer_row.name}")
            # ここで正しいtextを渡す
            write_print_fallback(text, f"unknown_{kind}")
            
    except Exception as e:
        error_prefix = f"error_{kind}"
        logging.error(f"プリンタへの印刷中にエラーが発生しました。プリンタID: {printer_row.id}, 種別: '{kind}', 接続情報: '{conn_str}', エラー: {e}")
        logging.error(traceback.format_exc())
        # ここで正しいtextを渡す
        write_print_fallback(text, error_prefix)


# --- 明細ごとのプリンタ解決（メニュー→カテゴリ→デフォルトの優先順） -----------
def resolve_printers_for_item(session_db, menu_id: int) -> list['Printer']:
    # メニュー個別 → カテゴリ → デフォルト の順に解決
    pr = session_db.query(Printer).join(PrintRule, Printer.id == PrintRule.printer_id)\
        .filter(PrintRule.menu_id == menu_id, Printer.enabled == 1).all()
    if pr:
        return pr
    pr = session_db.query(Printer).join(PrintRule, Printer.id == PrintRule.printer_id)\
        .join(ProductCategoryLink, ProductCategoryLink.category_id == PrintRule.category_id)\
        .filter(ProductCategoryLink.product_id == menu_id, Printer.enabled == 1).distinct().all()
    if pr:
        return pr
    pr = session_db.query(Printer).join(PrintRule, Printer.id == PrintRule.printer_id)\
        .filter(PrintRule.category_id == None, PrintRule.menu_id == None, Printer.enabled == 1).all()
    return pr


# --- 印刷ジョブの生成とディスパッチ（明細のバケツ分け→チケット生成→出力） ------
def trigger_print_job(order_id: int, items_to_print: list = None):
    s = SessionLocal()
    try:
        header = s.get(OrderHeader, order_id)
        if not header:
            app.logger.warning(f"注文ヘッダーが見つかりませんでした: {order_id}")
            return
        
        # 💡 TableSeatオブジェクトをデータベースから取得
        table = s.get(TableSeat, getattr(header, 'table_id', None))
        
        # --- 詳細なデバッグログを追加 ---
        app.logger.info(f"DEBUG: 注文ID {order_id} に紐づくテーブルID: {getattr(header, 'table_id', 'N/A')}")
        if table:
            app.logger.info(f"DEBUG: データベースからTableSeatオブジェクトを正常に取得しました。")
            app.logger.info(f"DEBUG: 取得されたテーブルID: {getattr(table, 'id', 'N/A')}")
            app.logger.info(f"DEBUG: 取得されたテーブル番号: {getattr(table, 'table_no', 'N/A')}")
            if not getattr(table, 'table_no', None):
                app.logger.warning("WARNING: Table object found, but 'table_no' attribute is missing or empty.")
        else:
            app.logger.warning(f"テーブルが見つかりませんでした。header.table_id: {getattr(header, 'table_id', 'N/A')}")
            
        app.logger.info(f"注文ID {order_id} の印刷ジョブを開始します。")

        # items_to_print引数が渡された場合は、そのリストを使用
        if items_to_print is not None:
            details = items_to_print
        else:
            # 引数が渡されない場合は、データベースから明細をすべて取得（従来の動作）
            details = s.query(OrderItem).filter(
                OrderItem.order_id == order_id, 
                OrderItem.status != "取消"
            ).order_by(OrderItem.id).all()
        
        app.logger.info(f"注文ID {order_id} に紐づく明細が {len(details)} 件見つかりました。")

        # プリンタIDまたはNoneをキーとして、明細のリストを保持する辞書
        # 値は (プリンタオブジェクト or None, 明細リスト) のタプル
        printer_buckets = {}
        
        for item in details:
            app.logger.info(f"明細ID {item.id} (メニューID: {item.menu_id}) のプリンタを解決します。")
            printers = resolve_printers_for_item(s, item.menu_id)
            app.logger.info(f"明細ID {item.id} には {len(printers) if printers else 0} 件のプリンタが割り当てられました。")
            
            if not printers:
                if None not in printer_buckets:
                    printer_buckets[None] = (None, [])
                printer_buckets[None][1].append(item)
            else:
                for p in printers:
                    if p.id not in printer_buckets:
                        printer_buckets[p.id] = (p, [])
                    printer_buckets[p.id][1].append(item)

        app.logger.info(f"合計 {len(printer_buckets)} 件のプリンタバケットが作成されました。")

        for key, vals in printer_buckets.items():
            printer_obj, items_to_print_in_bucket = vals
            
            if items_to_print_in_bucket:
                title = (getattr(printer_obj, 'name', "DEFAULT")).upper()
                width = getattr(printer_obj, 'width', 42)
                
                app.logger.info(f"プリンタID {key if key else 'DEFAULT'} に {len(items_to_print_in_bucket)} 件の明細を印刷します。")
                
                # 💡 build_ticket関数にテーブルオブジェクトを渡す
                ticket = build_ticket(header, items_to_print_in_bucket, table=table, width=width, title=title)
                dispatch_print(printer_obj, ticket)
            else:
                app.logger.warning(f"プリンタID {key if key else 'DEFAULT'} に印刷する明細がありません。")

    finally:
        s.close()


# --- 非同期印刷トリガ（コンテキスト継承して軽量スレッドで実行） -----------------
def trigger_print_async(order_id: int) -> None:
    """
    コミット完了後に、軽量スレッドで印刷処理を走らせるヘルパ。
    スレッド内で app_context / request_context を明示的に張る。
    """

    # 呼び出し元リクエストのコンテキストからテナント/店舗IDを引き継ぐ（あれば）
    try:
        tenant_id = session.get("tenant_id")
        store_id  = session.get("store_id")
    except Exception:
        tenant_id = None
        store_id  = None

    app = current_app._get_current_object()

    def _run(oid: int) -> None:
        try:
            # アプリコンテキスト → ダミーのリクエストコンテキストを張る
            with app.app_context():
                with app.test_request_context("/__print_async__"):
                    # マルチテナントのフィルタが g / session を見る想定に合わせて注入
                    if tenant_id is not None:
                        g.tenant_id = tenant_id
                        session["tenant_id"] = tenant_id
                    if store_id is not None:
                        session["store_id"] = store_id

                    # 既存の同期版をそのまま呼ぶ（内部で SessionLocal を開閉する実装）
                    trigger_print_job(oid)

        except Exception as e:
            app.logger.error("[print] async failed: %s", e, exc_info=True)

    threading.Thread(target=_run, args=(order_id,), daemon=True).start()




# -----------------------------------------------------------------------------
# 静的: アップロード画像の配信
# -----------------------------------------------------------------------------
# --- [ルート] アップロードファイル配信（/uploads/<filename>） ------------------------------
@app.route("/uploads/<path:filename>")
def uploaded_file(filename):
    return send_from_directory(UPLOAD_DIR, filename, as_attachment=False)


# -----------------------------------------------------------------------------
# 認証ヘルパ & デコレータ（厳密一致版を追加）
# -----------------------------------------------------------------------------

# --- ログイン確立（セッション再構築／店舗IDマスター整合も実施） ------------------
def login_session(role: str, user_id: int, user_name: str,
                  store_id: int | None, store_name: str | None,
                  login_id: str | None = None):
    """ログイン確立：セッション再構築 + 店舗IDマスター整合（主キー=store_id を保証）"""
    # 既存のテナント情報を退避してから session を再構築
    slug = session.get("tenant_slug")
    tid  = session.get("tenant_id")
    session.clear()
    if slug is not None:
        session["tenant_slug"] = slug
    if tid is not None:
        session["tenant_id"] = tid

    # ★ 後方互換: 'admin' を 'store_admin' に正規化
    if role == "admin":
        role = "store_admin"

    session["logged_in"]  = True
    session["role"]       = role
    session["role_level"] = ROLE_LEVELS.get(role, 0)
    session["user_id"]    = int(user_id)
    session["user_name"]  = user_name

    # --- 店舗情報をセッションに格納し、マスター整合を強制 ---
    if store_id is not None:
        try:
            sid = int(store_id)
        except Exception:
            sid = None

        if sid is not None:
            session["store_id"] = sid
            # 1) 従来のコード基準での登録（後方互換）
            try:
                ensure_store_id_in_master(f"store_{sid}", store_name)
            except Exception as e:
                app.logger.warning(f"[login_session] 店舗IDマスター登録(コード)に失敗: {e}")

            # 2) 主キー=店舗ID==sid の行を必ず用意（無ければINSERT）
            try:
                _ensure_master_pk_equals_store_id(sid, store_name)
            except Exception as e:
                app.logger.error(f"[login_session] 店舗IDマスター(主キー整合)に失敗: {e}")

            # 3) 最終確認ログ
            try:
                ok = validate_store_id(sid)
                app.logger.info(f"[login_session] validate_store_id({sid})={ok}")
            except Exception as e:
                app.logger.warning(f"[login_session] validate_store_id 例外: {e}")

    if store_name is not None:
        session["store_name"] = store_name
    if login_id is not None:
        session["login_id"] = login_id
    session["login_at"] = now_str()


# --- 店舗IDマスター：主キー=店舗ID の行を必ず用意（なければINSERT） ---------------
def _ensure_master_pk_equals_store_id(sid: int, store_name: str | None):
    """
    M_店舗IDマスター に 主キー=店舗ID==sid の行が無ければ作る。
    既にあれば何もしない（有効化と更新日時だけ整える）。
    """
    s = SessionLocal()
    try:
        row = s.execute(
            text('SELECT 1 FROM "M_店舗IDマスター" WHERE "店舗ID"=:sid'),
            {"sid": sid}
        ).first()

        if row is None:
            code = f"store_{sid}"
            name = store_name or f"店舗{sid}"
            nowv = now_str()
            s.execute(text('''
                INSERT INTO "M_店舗IDマスター"(
                    "店舗ID","店舗コード","店舗名","有効フラグ","登録日時","更新日時"
                ) VALUES (:sid, :code, :name, 1, :nowv, :nowv)
            '''), {"sid": sid, "code": code, "name": name, "nowv": nowv})
            s.commit()
            app.logger.info(f"[_ensure_master_pk_equals_store_id] inserted sid={sid}")
        else:
            s.execute(text('''
                UPDATE "M_店舗IDマスター"
                   SET "有効フラグ"=1,
                       "更新日時"=:nowv
                 WHERE "店舗ID"=:sid
            '''), {"sid": sid, "nowv": now_str()})
            s.commit()
    finally:
        s.close()


# --- ログアウト（セッション全クリア） --------------------------------------------
def logout_session():
    session.clear()


# --- ログイン必須（誰でも可） ---------------------------------------------------
def require_any(viewfunc):
    @wraps(viewfunc)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login_choice", next=request.path))
        return viewfunc(*args, **kwargs)
    return wrapper


# --- 権限“以上”ガード用の内部デコレータ -----------------------------------------
def _role_guard(min_role: str, redirect_endpoint: str):
    def deco(viewfunc):
        @wraps(viewfunc)
        def wrapper(*args, **kwargs):
            if not session.get("logged_in") or not has_role_at_least(min_role):
                return redirect(url_for(redirect_endpoint, next=request.path))
            return viewfunc(*args, **kwargs)
        return wrapper
    return deco


# --- 権限“厳密一致”ガード用の内部デコレータ -------------------------------------
def _role_exact(required_role: str, redirect_endpoint: str):
    def deco(viewfunc):
        @wraps(viewfunc)
        def wrapper(*args, **kwargs):
            if not session.get("logged_in") or session.get("role") != required_role:
                return redirect(url_for(redirect_endpoint, next=request.path))
            return viewfunc(*args, **kwargs)
        return wrapper
    return deco


# --- システム管理者のみ許可（厳密一致） ------------------------------------------
def require_sysadmin(viewfunc):
    return _role_exact("sysadmin", "sysadmin_login")(viewfunc)


# --- テナント管理者のみ許可（厳密一致） ------------------------------------------
def require_tenant_admin(viewfunc):
    return _role_exact("tenant_admin", "tenant_admin_login_entry")(viewfunc)


# --- 店舗管理者以上を許可（昇格ロジック考慮のカスタム判定） ----------------------
def require_store_admin(viewfunc):
    @wraps(viewfunc)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in") or not is_store_admin_or_higher():
            return redirect(url_for("admin_login", next=request.path))
        return viewfunc(*args, **kwargs)
    return wrapper


# --- 後方互換：require_admin を「店舗管理者以上」にエイリアス --------------------
require_admin = require_store_admin


# --- スタッフ以上を許可（従来通り） ----------------------------------------------
def require_staff(viewfunc):
    @wraps(viewfunc)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in") or role_level() < ROLE_LEVELS["staff"]:
            return redirect(url_for("staff_login", next=request.path))
        return viewfunc(*args, **kwargs)
    return wrapper


# -----------------------------------------------------------------------------
# 価格入力モード 切替（税込/税抜）
# -----------------------------------------------------------------------------
# --- [ルート] 価格入力モード設定（cookie に incl/excl を1年保持して元画面へ戻す） ---------------
@app.route("/admin/price-mode", methods=["POST"])
def set_price_mode():
    """[価格入力モード設定] cookie に incl/excl を1年保持してリダイレクト"""
    mode = (request.form.get("mode") or "").lower()
    if mode not in ("incl", "excl"):
        abort(400)
    resp = make_response(redirect(request.referrer or url_for("floor")))
    expire = datetime.utcnow() + timedelta(days=365)
    resp.set_cookie("price_mode", mode, expires=expire, samesite="Lax")
    return resp



# --- テンプレートに価格入力モードとアプリタイトルを注入 ---------------------------
@app.context_processor
def inject_price_mode():
    return {"price_input_mode": get_price_input_mode(), "APP_TITLE": APP_TITLE}


# --- CSRFトークン生成（セッションに無ければ生成して返す） ------------------------
def _generate_csrf_token():
    """セッションに CSRF トークンが無ければ作って返す"""
    tok = session.get("_csrf_token")
    if not tok:
        tok = secrets.token_urlsafe(32)
        session["_csrf_token"] = tok
    return tok


# --- テンプレートに CSRF 取得関数を注入 ------------------------------------------
@app.context_processor
def inject_csrf():
    # テンプレート内で {{ get_csrf() }} が使えるようになる
    return {"get_csrf": _generate_csrf_token}


# --- CSRF 検証（POST時に呼び出し／不一致なら 400） ------------------------------
def check_csrf():
    """必要なら POST で呼んで検証。失敗したら 400。"""
    sent = (request.form.get("csrf_token")
            or (request.get_json(silent=True) or {}).get("csrf_token"))
    real = session.get("_csrf_token")
    if not (sent and real and hmac.compare_digest(sent, real)):
        abort(400, description="CSRF token invalid")


# -----------------------------------------------------------------------------
# ログイン/ログアウト & ブートストラップ
# -----------------------------------------------------------------------------

# --- ログイン選択画面 -----------------------------------------------------------
@app.route("/login")
def login_choice():
    return render_template("login_choice.html")


# --- システム管理者ログイン -----------------------------------------------------
@app.route("/login/sysadmin", methods=["GET", "POST"])
def sysadmin_login():
    s = SessionLocal()
    try:
        if request.method == "POST":
            login_id = (request.form.get("login_id") or "").strip()
            password = (request.form.get("password") or "")
            user = s.query(SysAdmin).filter(SysAdmin.login_id == login_id, SysAdmin.active == 1).first()
            if not (user and check_password_hash(user.password_hash, password)):
                return render_template("sysadmin_login.html", error="ログインIDまたはパスワードが違います。")
            user.last_login = now_str(); s.commit()
            login_session("sysadmin", user.id, user.name, None, None)
            # ↓ ここを floor → dev_tools に変更
            return redirect(request.args.get("next") or url_for("dev_tools"))
        return render_template("sysadmin_login.html")
    finally:
        s.close()


# --- システム管理者 初回ブートストラップ ----------------------------------------
@app.route("/sysadmin/bootstrap", methods=["GET", "POST"])
def sysadmin_bootstrap():
    s = SessionLocal()
    try:
        exists = s.query(SysAdmin.id).first()
        if exists:
            return redirect(url_for("sysadmin_login"))
        if request.method == "POST":
            login_id = (request.form.get("login_id") or "").strip()
            password = (request.form.get("password") or "")
            password_confirm = (request.form.get("password_confirm") or "")
            name     = (request.form.get("name") or "").strip()

            if not (login_id and password and password_confirm and name):
                return render_template("sysadmin_bootstrap.html", error="全ての項目を入力してください。")

            if password != password_confirm:
                return render_template("sysadmin_bootstrap.html", error="パスワードが一致しません。")

            s.add(SysAdmin(login_id=login_id,
                           password_hash=generate_password_hash(password),
                           name=name, active=1,
                           created_at=now_str(), updated_at=now_str()))
            s.commit()
            return redirect(url_for("sysadmin_login"))
        return render_template("sysadmin_bootstrap.html")
    finally:
        s.close()


# --- テナント管理者ログイン入口（テナント選択→正式URLへ） ----------------------
@app.route("/login/tenant", methods=["GET", "POST"])
def tenant_admin_login_entry():
    if request.method == "POST":
        slug = (request.form.get("tenant_slug") or "").strip()
        if not slug:
            return render_template("tenant_admin_login_select.html",
                                   error="テナントを入力してください。")
        return redirect(url_for("tenant_admin_login", tenant_slug=slug))
    return render_template("tenant_admin_login_select.html")


# --- テナント管理者ログイン（正式：/t/<slug>/login/tenant） ---------------------
@app.route("/t/<tenant_slug>/login/tenant", methods=["GET", "POST"])
def tenant_admin_login():
    rid = uuid.uuid4().hex[:8]
    app.logger.info(f"[tenant_login {rid}] {request.method} from {request.remote_addr} ua={request.user_agent}")

    s = SessionLocal()
    try:
        if request.method == "POST":
            login_id_in = (request.form.get("login_id") or "").strip()
            password    = (request.form.get("password") or "")
            login_id_q  = login_id_in.lower()

            user = (s.query(TenantAdmin)
                     .filter(func.lower(TenantAdmin.login_id) == login_id_q)
                     .order_by(TenantAdmin.active.desc()).first())
            if not user:
                return render_template("tenant_admin_login.html", error="アカウントが見つかりません。")
            if not user.active:
                return render_template("tenant_admin_login.html", error="このアカウントは無効です。")
            if not user.password_hash:
                return render_template("tenant_admin_login.html", error="パスワード未設定です（管理者に連絡してください）。")
            try:
                ok = check_password_hash(user.password_hash, password)
            except Exception:
                app.logger.exception(f"[tenant_login {rid}] check_password_hash failed")
                ok = False
            if not ok:
                return render_template("tenant_admin_login.html", error="パスワードが違います。")

            tenant = s.get(M_テナント, user.tenant_id) if getattr(user, "tenant_id", None) else None
            user.last_login = now_str(); s.commit()

            login_session("tenant_admin", user.id, user.name, None, None, login_id=login_id_in)
            if tenant:
                session["tenant_id"] = tenant.id
                session["tenant_slug"] = tenant.slug

            return redirect(request.form.get("next") or request.args.get("next") or url_for("tenant_portal"))

        return render_template("tenant_admin_login.html")
    finally:
        s.close()  # コメント必須


# --- シス管：テナント管理者の有効化/削除/パスワード更新 -------------------------
@app.route("/sysadmin/tenants/<int:tid>/admins/update", methods=["POST"])
@require_sysadmin
def sys_tenant_admins_update(tid):
    try:
        check_csrf()
    except Exception:
        pass

    s = SessionLocal()
    f = request.form
    op = (f.get("op") or "").strip()
    try:
        u = None
        if op in ("toggle", "resetpw", "delete"):
            uid = int(f.get("id") or 0)
            u = s.get(TenantAdmin, uid)
            if not u or int(u.tenant_id or 0) != int(tid):
                flash("対象ユーザーが見つかりません。")
                return redirect(url_for("sys_tenant_admins", tid=tid))

        if op == "toggle":
            u.active = 0 if u.active == 1 else 1
            u.updated_at = now_str()
            s.commit(); flash("有効/無効を更新しました。")

        elif op == "resetpw":
            pw = f.get("password") or ""
            if not pw:
                flash("パスワードを入力してください。")
            else:
                u.password_hash = generate_password_hash(pw)
                u.updated_at = now_str()
                s.commit(); flash("パスワードを更新しました。")

        elif op == "delete":
            s.delete(u); s.commit(); flash("削除しました。")

        else:
            flash("不正な操作です。")

        return redirect(url_for("sys_tenant_admins", tid=tid))
    finally:
        s.close()


# --- テナント管理：店舗編集 ------------------------------------------------------
@app.route("/tenant/stores/<int:sid>/edit", methods=["GET","POST"])
@require_tenant_admin
def tenant_store_edit(sid):
    s = SessionLocal()
    try:
        st = s.get(Store, sid)
        if not st: abort(404)
        if request.method == "POST":
            name = (request.form.get("name") or "").strip()
            code = (request.form.get("code") or "").strip()
            active = 1 if (request.form.get("active") or "1") == "1" else 0
            
            # レシート・領収書用情報
            address = (request.form.get("address") or "").strip()
            phone = (request.form.get("phone") or "").strip()
            registration_number = (request.form.get("registration_number") or "").strip()
            business_hours = (request.form.get("business_hours") or "").strip()
            receipt_footer = (request.form.get("receipt_footer") or "").strip()
            
            if not code or not name:
                flash("店舗コード・名称は必須です。")
                return redirect(request.url)
            # コード重複チェック（自分以外）
            dup = s.query(Store.id).filter(Store.code == code, Store.id != st.id).first()
            if dup:
                flash("同じ店舗コードが既に存在します。"); return redirect(request.url)
            
            st.code = code
            st.name = name
            st.active = active
            st.address = address or None
            st.phone = phone or None
            st.registration_number = registration_number or None
            st.business_hours = business_hours or None
            st.receipt_footer = receipt_footer or None
            st.updated_at = now_str()
            
            s.commit()
            flash("更新しました。")
            return redirect(url_for("tenant_stores"))
        # GET
        return render_template("tenant_store_edit.html", store=st)
    finally:
        s.close()


# --- テナント管理：店舗削除 ------------------------------------------------------
@app.route("/tenant/stores/<int:sid>/delete", methods=["POST"])
@require_tenant_admin
def tenant_store_delete(sid):
    s = SessionLocal()
    try:
        st = s.get(Store, sid)
        if st:
            # 付随データの扱いは要件次第。最低限、関連の Admin/Employee があればCASCADEされます。
            s.delete(st); s.commit(); flash("削除しました。")
        return redirect(url_for("tenant_stores"))
    finally:
        s.close()


# --- 共通ヘルパ：現在の店舗IDを取得（session から int へ） ----------------------
def current_store_id() -> int | None:
    try:
        sid = session.get("store_id")
        return int(sid) if sid not in (None, "", "null") else None
    except Exception:
        return None



# --- 来客数取得ヘルパ（_get_guests_for_order） ----------------------
def _get_guests_for_order(session, order_id: int) -> dict:
    """
    【タイトル】来客数取得ヘルパ
    現在値テーブル -> 伝票ヘッダ -> 履歴（必要なら upsert） の順で防御的に人数を取得する。
    戻り値: {"men":0, "women":0, "boys":0, "girls":0, "total":0}
    """
    out = {"men":0, "women":0, "boys":0, "girls":0, "total":0}

    Cur = (globals().get("GuestDetail") or globals().get("T_お客様詳細"))
    if Cur is not None:
        cur = session.query(Cur).filter(getattr(Cur, "order_id") == order_id).first()
        if cur:
            def gv(obj, *names):
                for nm in names:
                    if hasattr(obj, nm):
                        try:
                            return int(getattr(obj, nm) or 0)
                        except Exception:
                            pass
                return 0
            out["men"]   = gv(cur, "大人男性", "adult_male", "men")
            out["women"] = gv(cur, "大人女性", "adult_female", "women")
            out["boys"]  = gv(cur, "子ども男", "boys", "子供男")
            out["girls"] = gv(cur, "子ども女", "girls", "子供女")
            out["total"] = gv(cur, "合計人数", "total", "人数")

    if out["total"] == 0:
        H = globals().get("OrderHeader")
        h = session.get(H, order_id) if H else None
        if h:
            def hv(obj, *names):
                for nm in names:
                    if hasattr(obj, nm):
                        try:
                            return int(getattr(obj, nm) or 0)
                        except Exception:
                            pass
                return 0
            out["total"] = hv(h, "guest_count","guests","guests_total","num_guests","人数","合計人数","来客数")

    if out["total"] == 0:
        # 最後の手段：履歴から再集計して現在値へ upsert（ヘルパがあれば）
        if "_upsert_guest_detail_from_history" in globals():
            try:
                _up = _upsert_guest_detail_from_history(session, order_id) or {}
                for k in ("men","women","boys","girls","total"):
                    try:
                        out[k] = int(_up.get(k, out[k]) or 0)
                    except Exception:
                        pass
            except Exception:
                pass

    return out




# --- 共通ヘルパ：現在店舗のカテゴリ親候補を取得 --------------------------------
def category_options_of_current_store(sess):
    sid = current_store_id()
    q = sess.query(Category)
    if sid is not None and hasattr(Category, "store_id"):
        q = q.filter(Category.store_id == sid)
    cats = q.order_by(Category.parent_id, Category.display_order, Category.name).all()
    return [{"id": c.id, "name": c.name} for c in cats]


# --- 共通ヘルパ：テーブル番号ラベル（ID→先頭ゼロ保持の番号） --------------------
def get_table_no_str(db, table_id: int) -> str:
    """[テーブル番号ラベル取得] テーブルIDから表示用のテーブル番号を返す（先頭ゼロ保持）。見つからなければID文字列を返す。"""
    try:
        t = db.get(TableSeat, int(table_id))
        if t:
            v = getattr(t, "table_no", None) or getattr(t, "テーブル番号", None)
            if v is not None and str(v).strip():
                return str(v)
    except Exception:
        pass
    return str(table_id)  # フォールバック


# --- 店舗管理者ログイン（admin_login → store_admin 化） -------------------------
@app.route("/login/admin", methods=["GET", "POST"])
def admin_login():
    s = SessionLocal()
    try:
        if request.method == "POST":
            store_code = (request.form.get("store_code") or "").strip()
            login_id   = (request.form.get("login_id") or "").strip()
            password   = (request.form.get("password") or "")
            
            # 生のSQLで店舗を検索（テナントフィルタを回避）
            from sqlalchemy import text
            store_result = s.execute(
                text('SELECT id, "店舗コード", "名称", tenant_id FROM "M_店舗" WHERE "店舗コード" = :code AND "有効" = 1'),
                {"code": store_code}
            ).mappings().first()
            
            if not store_result:
                return render_template("admin_login.html", error="店舗コードが正しくありません。")
            
            store_id = store_result["id"]
            tenant_id = store_result["tenant_id"]
            store_name = store_result["名称"]
            
            # テナントコンテキストを設定
            g.tenant_id = tenant_id
            session["tenant_id"] = tenant_id
            
            # 管理者を検索
            user = (s.query(Admin)
                    .filter(Admin.store_id == store_id,
                            Admin.login_id == login_id,
                            Admin.active == 1).first())
            
            if not (user and check_password_hash(user.password_hash, password)):
                return render_template("admin_login.html", 
                                     error="ログインIDまたはパスワードが違います。", 
                                     store_code=store_code)
            
            user.last_login = now_str()
            s.commit()
            
            # テナント情報もセッションに保存
            tenant = s.get(M_テナント, tenant_id)
            if tenant:
                session["tenant_slug"] = tenant.slug
            
            login_session("store_admin", user.id, user.name, store_id, store_name, login_id=login_id)
            return redirect(request.args.get("next") or url_for("floor"))
            
        return render_template("admin_login.html")
    finally:
        s.close()


# --- 従業員ログイン --------------------------------------------------------------
@app.route("/login/staff", methods=["GET", "POST"])
def staff_login():
    s = SessionLocal()
    try:
        if request.method == "POST":
            store_code = (request.form.get("store_code") or "").strip()
            login_id   = (request.form.get("login_id") or "").strip()
            password   = (request.form.get("password") or "")

            # ① テナント自動フィルタを回避して店舗を取得（/login/admin と同じ方針）
            from sqlalchemy import text
            store_row = s.execute(
                text('SELECT id, "店舗コード", "名称", tenant_id FROM "M_店舗" '
                     'WHERE "店舗コード" = :code AND "有効" = 1'),
                {"code": store_code}
            ).mappings().first()

            if not store_row:
                return render_template("staff_login.html", error="店舗コードが正しくありません。")

            store_id   = int(store_row["id"])
            store_name = store_row["名称"]
            tenant_id  = int(store_row["tenant_id"])

            # ② ここでテナントコンテキストを切り替える
            g.tenant_id = tenant_id
            session["tenant_id"] = tenant_id
            tenant = s.get(M_テナント, tenant_id)
            if tenant:
                session["tenant_slug"] = tenant.slug

            # ③ 従業員を検索（ここからは ORM の自動テナントフィルタが正しく効く）
            emp = (
                s.query(Employee)
                 .filter(Employee.store_id == store_id,
                         Employee.login_id == login_id,
                         Employee.active == 1)
                 .first()
            )

            if not (emp and check_password_hash(emp.password_hash, password)):
                return render_template(
                    "staff_login.html",
                    error="ログインIDまたはパスワードが違います。",
                    store_code=store_code
                )

            emp.last_login = now_str()
            s.commit()

            # ④ セッション確立
            login_session("staff", emp.id, emp.name, store_id, store_name, login_id=login_id)
            return redirect(request.args.get("next") or url_for("staff_floor"))

        return render_template("staff_login.html")
    finally:
        s.close()


# --- ログアウト ---------------------------------------------------------------
@app.route("/logout")
def logout():
    logout_session()
    return redirect(url_for("login_choice"))


# --- 初回セットアップ（店舗が無い時だけ許可） -----------------------------------
@app.route("/admin/bootstrap", methods=["GET", "POST"])
def bootstrap_first_store():
    s = SessionLocal()
    try:
        store_exists = s.query(Store.id).first()
        if store_exists:
            return redirect(url_for("login_choice"))
        if request.method == "POST":
            st_code = (request.form.get("store_code") or "").strip()
            st_name = (request.form.get("store_name") or "").strip()
            ad_id   = (request.form.get("admin_login_id") or "").strip()
            ad_pw   = (request.form.get("admin_password") or "")
            ad_name = (request.form.get("admin_name") or "").strip()
            if not (st_code and st_name and ad_id and ad_pw and ad_name):
                return render_template("bootstrap.html", error="全ての項目を入力してください。")
            if s.query(Store).filter(Store.code == st_code).first():
                return render_template("bootstrap.html", error="その店舗コードは既に使われています。")
            store = Store(code=st_code, name=st_name, active=1, created_at=now_str(), updated_at=now_str())
            s.add(store); s.flush()
            admin = Admin(
                store_id=store.id, login_id=ad_id,
                password_hash=generate_password_hash(ad_pw),
                name=ad_name, active=1,
                created_at=now_str(), updated_at=now_str())
            s.add(admin); s.commit()
            return redirect(url_for("admin_login"))
        return render_template("bootstrap.html")
    finally:
        s.close()



# -----------------------------------------------------------------------------
# システム管理者: ポータル / メンテ / テナント一覧・作成 / テナント管理者作成 / シス管CRUD
# -----------------------------------------------------------------------------

# --- システム管理者ポータルへリダイレクト（/dev-tools → /dev_tools） -------------
@app.route("/dev-tools")
@require_sysadmin
def dev_tools_redirect():
    import os
    from flask import abort
    if os.getenv("ENABLE_DEV_TOOLS") != "1":
        abort(404)
    # 本体は /dev_tools の dev_tools() を使う
    return redirect(url_for("dev_tools"))


# --- スキーマ軽量マイグレーション実行（ensure + tenant列追加） -------------------
@app.route("/sys/migrate", methods=["POST"])
@require_sysadmin
def sys_migrate():
    # 既存ユーティリティの実行
    migrate_schema_if_needed()
    ensure_tenant_columns()
    flash("スキーマの軽量マイグレーションを実行しました。")
    return redirect(url_for("dev_tools"))


# --- テナント一覧表示 ------------------------------------------------------------
@app.route("/sysadmin/tenants")
@require_sysadmin
def sys_tenants():
    s = SessionLocal()
    try:
        rows = s.query(M_テナント).order_by(M_テナント.id.desc()).all()
        return render_template("sys_tenants.html", rows=rows)
    finally:
        s.close()


# --- テナント新規作成（GET:フォーム / POST:作成） --------------------------------
@app.route("/sysadmin/tenants/new", methods=["GET", "POST"])
@require_sysadmin
def sys_tenants_new():
    if request.method == "GET":
        # 入力フォームを表示
        return render_template("sys_tenant_new.html")

    # POST: 作成処理
    name = (request.form.get("name") or "").strip()
    slug = (request.form.get("slug") or "").strip()

    s = SessionLocal()
    try:
        if not name or not slug:
            flash("名称とslugは必須です")
            return render_template("sys_tenant_new.html", name=name, slug=slug)

        # slug 重複チェック
        if s.query(M_テナント).filter(M_テナント.slug == slug).first():
            flash("同じslugが既に存在します")
            return render_template("sys_tenant_new.html", name=name, slug=slug)

        s.add(M_テナント(名称=name, slug=slug))
        s.commit()
        flash("テナントを作成しました")
        return redirect(url_for("sys_tenants"))
    finally:
        s.close()


# --- テナント管理者の新規作成 ----------------------------------------------------
@app.route("/sysadmin/tenants/<int:tid>/admins/new", methods=["GET", "POST"])
@require_sysadmin
def sys_tenant_admins_new(tid):
    s = SessionLocal()
    try:
        t = s.get(M_テナント, tid)
        if not t:
            abort(404)

        # Default Tenant には追加不可
        if t.slug == "default":
            flash("Default Tenant には管理者を追加できません。")
            return redirect(url_for("sys_tenants"))

        if request.method == "POST":
            f = request.form
            login_id = (f.get("login_id") or "").strip().lower()
            password = (f.get("password") or "")
            password_confirm = (f.get("password_confirm") or "")
            name = (f.get("name") or "").strip()
            active = 1 if (f.get("active") or "1") == "1" else 0

            # 入力チェック
            if not (login_id and password and name):
                flash("必須項目が不足しています。")
                return redirect(request.url)
            if password != password_confirm:
                flash("パスワードが一致しません。")
                return redirect(request.url)

            # 同一テナント内で login_id の重複チェック
            exists = (s.query(TenantAdmin.id)
                        .filter(TenantAdmin.tenant_id == t.id,
                                TenantAdmin.login_id == login_id)
                        .first())
            if exists:
                flash("同じログインIDが既に存在します。")
                return redirect(request.url)

            # ★ store_id は渡さない。tenant_id のみ！
            s.add(TenantAdmin(
                tenant_id=t.id,
                login_id=login_id,
                password_hash=generate_password_hash(password),
                name=name,
                active=active,
                created_at=now_str(), updated_at=now_str()
            ))
            s.commit()
            flash("テナント管理者を追加しました。")
            return redirect(url_for("sys_tenant_admins", tid=t.id))

        # GET: フォーム
        return render_template("sys_tenant_admin_new.html", tenant=t)
    finally:
        s.close()


# --- テナント管理者：自分のパスワード変更 ---------------------------------------
@app.route("/tenant/me/resetpw", methods=["POST"])
@require_tenant_admin
def tenant_me_resetpw():
    try:
        check_csrf()
    except Exception:
        pass
    s = SessionLocal()
    try:
        uid = int(session.get("user_id") or 0)
        pw  = (request.form.get("password") or "").strip()
        if not (uid and pw):
            flash("パスワードを入力してください。")
            return redirect(url_for("tenant_portal"))
        u = s.get(TenantAdmin, uid)
        if not u:
            flash("ユーザーが見つかりません。")
            return redirect(url_for("tenant_portal"))
        u.password_hash = generate_password_hash(pw)
        u.updated_at = now_str()
        s.commit()
        flash("パスワードを更新しました。")
        return redirect(url_for("tenant_portal"))
    finally:
        s.close()


# --- 安全なURL判定ヘルパ --------------------------------------------------------
def _is_safe_url(target: str) -> bool:
    try:
        ref = urlparse(request.host_url)
        test = urlparse(target)
        return (test.scheme in ("http", "https")) and (test.netloc == ref.netloc)
    except Exception:
        return False


# --- 既存エンドポイントのうち最初に存在するURLを返す ----------------------------
def _first_existing_url(*endpoint_names, default="/"):
    for name in endpoint_names:
        try:
            if name and name in current_app.view_functions:
                return url_for(name)
        except Exception:
            pass
    return default


# --- テナント管理者：自分のテナント編集（名称・slug） ----------------------------
@app.route("/tenant/me/edit", methods=["GET", "POST"])
@require_tenant_admin
def tenant_me_edit():
    s = SessionLocal()
    try:
        t = s.execute(
            text('SELECT id, "名称", slug FROM "M_テナント" WHERE id=:id'),
            {"id": session.get("tenant_id")}
        ).mappings().first()

        # --- 決定的な戻り先（POST後にも使う） ---
        # 1) ?next=xxx
        next_qs = request.args.get("next")
        if next_qs and _is_safe_url(next_qs):
            back_url = next_qs
        else:
            # 2) referrer（同一オリジンのみ）
            ref = request.referrer
            back_url = ref if (ref and _is_safe_url(ref)) else None

        # 3) 既存エンドポイント候補（存在する最初を使用）
        if not back_url:
            back_url = _first_existing_url(
                # Blueprint あり/なし両対応の候補
                "tenant_me",
                "tenant.portal",
                "tenant.dashboard",
                "tenant.index",
                "tenant_home",
                "tenant_portal",
                "tenant_dashboard",
                "tenant_index",
                "index",   # トップ
                default="/"
            )

        if request.method == "POST":
            name = request.form.get("名称", "").strip()
            slug = request.form.get("slug", "").strip()
            s.execute(
                text('UPDATE "M_テナント" SET "名称"=:n, slug=:slug WHERE id=:id'),
                {"n": name, "slug": slug, "id": t["id"]}
            )
            s.commit()
            return redirect(back_url)

        # GET
        return render_template("tenant_me_edit.html", t=t, back_url=back_url)
    finally:
        s.close()


# --- テナント管理者：自分のテナントを削除（行のみ） ------------------------------
@app.route("/tenant/me/delete", methods=["POST"])
@require_tenant_admin
def tenant_me_delete():
    try:
        check_csrf()
    except Exception:
        pass

    s = SessionLocal()
    try:
        tid = int(getattr(g, "tenant_id", 0) or session.get("tenant_id") or 0)
        t = s.get(M_テナント, tid)
        if not t:
            abort(404)

        # デフォルトは削除禁止
        if t.slug == DEFAULT_TENANT_SLUG:
            flash("デフォルトテナントは削除できません。")
            return redirect(url_for("tenant_portal"))

        # 自分がログイン中のテナントを削除 → セッションをクリアしてログアウトさせる
        s.delete(t)
        s.commit()

        flash("テナントを削除しました。（注意：配下テーブルのデータは残っています）")
        logout_session()
        return redirect(url_for("login_choice"))
    finally:
        s.close()


# --- テナント管理者一覧（指定テナント） -----------------------------------------
@app.route("/sysadmin/tenants/<int:tid>/admins")
@require_sysadmin
def sys_tenant_admins(tid):
    s = SessionLocal()
    try:
        t = s.get(M_テナント, tid)
        if not t:
            abort(404)
        admins = s.query(TenantAdmin).filter(TenantAdmin.tenant_id == tid).all()
        return render_template("sys_tenant_admins.html", tenant=t, admins=admins)
    finally:
        s.close()


# -----------------------------------------------------------------------------
# テナント管理者ポータル
# -----------------------------------------------------------------------------

# --- テナント管理者ポータル（店舗と管理者の一覧を同時表示） ----------------------
@app.route("/tenant/portal")
@require_tenant_admin
def tenant_portal():
    s = SessionLocal()
    try:
        # セッションのログインユーザーID（＝TenantAdmin.id）
        uid = session.get("user_id")
        ta = s.get(TenantAdmin, uid) if uid else None

        tenant = s.get(M_テナント, ta.tenant_id) if ta else None
        stores_with_admins = []
        
        if tenant:
            # テナントに紐づく店舗を取得
            stores = (s.query(Store)
                        .filter(Store.tenant_id == tenant.id)
                        .order_by(Store.id.asc())
                        .all())
            
            # 各店舗に関連する管理者情報を取得してデータ構造を構築
            for store in stores:
                admins = (s.query(Admin)
                           .filter(Admin.store_id == store.id)
                           .order_by(Admin.name.asc())
                           .all())
                stores_with_admins.append({
                    "store": store,
                    "admins": admins
                })

        # テンプレートが期待する変数名と構造で渡す
        return render_template("tenant_portal.html",
                               tenant=tenant,
                               admin=ta,
                               stores_with_admins=stores_with_admins)
    finally:
        s.close()


# -----------------------------------------------------------------------------
# テナント管理者: 店舗一覧 & 新規作成 / 店舗管理者作成
# -----------------------------------------------------------------------------

# --- 店舗一覧（テナント内） ------------------------------------------------------
@app.route("/tenant/stores")
@require_tenant_admin
def tenant_stores():
    s = SessionLocal()
    try:
        rows = s.query(Store).order_by(Store.id.desc()).all()
        return render_template("tenant_stores.html", rows=rows)
    finally:
        s.close()


# --- 店舗新規作成（POST） --------------------------------------------------------
@app.route("/tenant/stores/new", methods=["POST"])
@require_tenant_admin
def tenant_stores_new():
    code = (request.form.get("code") or "").strip()
    name = (request.form.get("name") or "").strip()
    s = SessionLocal()
    try:
        if not code or not name:
            flash("店舗コード・名称は必須です")
            return redirect(url_for("tenant_stores"))
        if s.query(Store).filter(Store.code == code).first():
            flash("同じ店舗コードが既に存在します")
            return redirect(url_for("tenant_stores"))
        s.add(Store(code=code, name=name, active=1,
                    created_at=now_str(), updated_at=now_str()))
        s.commit()
        flash("店舗を作成しました")
        return redirect(url_for("tenant_stores"))
    finally:
        s.close()


# --- 店舗管理者の新規作成（指定店舗） -------------------------------------------
@app.route("/tenant/stores/<int:sid>/admins/new", methods=["GET", "POST"])
@require_tenant_admin
def tenant_store_admin_new(sid):
    s = SessionLocal()
    try:
        st = s.get(Store, sid)
        if not st:
            abort(404)
        if request.method == "POST":
            login_id = (request.form.get("login_id") or "").strip()
            name     = (request.form.get("name") or "").strip()
            password = (request.form.get("password") or "")
            if not (login_id and name and password):
                flash("全て入力してください")
                return redirect(request.url)
            s.add(Admin(
                store_id=st.id, login_id=login_id,
                password_hash=generate_password_hash(password),
                name=name, active=1,
                created_at=now_str(), updated_at=now_str()
            ))
            s.commit()
            flash("店舗管理者を作成しました")
            return redirect(url_for("tenant_stores"))
        return render_template("tenant_store_admin_new.html", store=st)
    finally:
        s.close()


# --- テナント編集（名称・slug 更新） ---------------------------------------------
@app.route("/sysadmin/tenants/<int:tid>/edit", methods=["GET", "POST"])
@require_sysadmin
def sys_tenant_edit(tid):
    s = SessionLocal()
    try:
        t = s.get(M_テナント, tid)
        if not t:
            abort(404)

        if request.method == "POST":
            # デフォルトテナントは編集禁止（テンプレ側だけでなくサーバ側でも防御）
            if t.slug == "default":
                flash("デフォルトテナントは編集できません。")
                return redirect(request.url)

            name = (request.form.get("name") or "").strip()
            slug = (request.form.get("slug") or "").strip()

            if not name or not slug:
                flash("名称とslugは必須です。")
                return redirect(request.url)

            # スラッグの形式チェック（英小文字/数字/ハイフン）
            if not re.fullmatch(r"[a-z0-9\-]+", slug):
                flash("slug は英小文字・数字・ハイフンのみ利用できます。")
                return redirect(request.url)

            # slug の重複チェック（自分以外）
            dup = s.query(M_テナント.id).filter(
                M_テナント.slug == slug, M_テナント.id != t.id
            ).first()
            if dup:
                flash("同じ slug が既に存在します。")
                return redirect(request.url)

            t.名称 = name
            t.slug = slug
            t.更新日時 = func.now()  # または t.更新日時 = now_str()
            s.commit()
            flash("テナント情報を更新しました。")
            return redirect(url_for("sys_tenants"))

        # GET: テンプレートが t を参照しているので t=t で渡す
        return render_template("sys_tenant_edit.html", t=t)
    finally:
        s.close()


# --- テナント削除（M_テナント の行のみ；配下データは残す） -----------------------
@app.route("/sysadmin/tenants/<int:tid>/delete", methods=["POST"])
@require_sysadmin
def sys_tenant_delete(tid):
    s = SessionLocal()
    try:
        t = s.get(M_テナント, tid)
        if not t:
            abort(404)

        # デフォルトや現在選択中テナントは防御
        if t.slug == DEFAULT_TENANT_SLUG:
            flash("デフォルトテナントは削除できません。")
            return redirect(url_for("sys_tenants"))
        if getattr(g, "tenant_id", None) == t.id:
            flash("現在選択中のテナントは削除できません。別のテナントを選択してから実行してください。")
            return redirect(url_for("sys_tenants"))

        s.delete(t)
        s.commit()
        flash("テナントを削除しました。（注意：テナント配下の各テーブルのデータは残っています）")
        return redirect(url_for("sys_tenants"))
    finally:
        s.close()


# --- テナント完全削除（TenantScoped 全テーブルから該当データ一括削除） ----------
@app.route("/sysadmin/tenants/<int:tid>/purge", methods=["POST"])
@require_sysadmin
def sys_tenant_purge(tid):
    s = SessionLocal()
    try:
        t = s.get(M_テナント, tid)
        if not t:
            abort(404)
        if t.slug == DEFAULT_TENANT_SLUG:
            flash("デフォルトテナントはパージできません。")
            return redirect(url_for("sys_tenants"))
        if getattr(g, "tenant_id", None) == t.id:
            flash("現在選択中のテナントはパージできません。")
            return redirect(url_for("sys_tenants"))

        # TenantScoped を継承しているモデルを列挙して DELETE
        from sqlalchemy.orm import with_loader_criteria
        deleted_total = 0
        for mapper in list(Base.registry.mappers):
            cls = mapper.class_
            try:
                if issubclass(cls, TenantScoped):
                    deleted = s.query(cls).filter(cls.tenant_id == tid).delete(synchronize_session=False)
                    deleted_total += int(deleted or 0)
            except TypeError:
                pass

        # 最後に M_テナント 自体を削除
        s.delete(t)
        s.commit()
        flash(f"テナントと配下データを削除しました。（削除行数の合計: {deleted_total}）")
        return redirect(url_for("sys_tenants"))
    finally:
        s.close()


# --- テナント管理者：店舗管理者の削除 -------------------------------------------
@app.route("/tenant/stores/<int:sid>/admins/<int:aid>/delete", methods=["POST"])
@require_tenant_admin
def tenant_store_admin_delete(sid, aid):
    s = SessionLocal()
    try:
        try:
            check_csrf()
        except Exception:
            pass
        # 対象店舗に属する管理者かを確認してから削除
        ad = s.query(Admin).filter(Admin.id == aid, Admin.store_id == sid).first()
        if not ad:
            abort(404)
        s.delete(ad)
        s.commit()
        flash("店舗管理者を削除しました。")
        return redirect(url_for("tenant_portal"))
    finally:
        s.close()


# -----------------------------------------------------------------------------
# システム管理者: マイページ
# -----------------------------------------------------------------------------

# --- システム管理者の個人ページ表示 ---------------------------------------------
@app.route("/sys/mypage")
@require_sysadmin
def sys_mypage():
    s = SessionLocal()
    try:
        me = s.get(SysAdmin, int(session.get("user_id") or 0))
        return render_template("sys_mypage.html", me=me)
    finally:
        s.close()


# -----------------------------------------------------------------------------
# システム管理者: SysAdmin 一覧/管理 画面
# -----------------------------------------------------------------------------

# --- システム管理者一覧 ----------------------------------------------------------
@app.route("/sys/admins")
@require_sysadmin
def sys_admins():
    s = SessionLocal()
    try:
        users = s.query(SysAdmin).order_by(SysAdmin.id.desc()).all()
        return render_template("sys_admins.html", users=users)
    finally:
        s.close()


# --- システム管理者の追加/有効切替/削除/パスワード更新（POST） -------------------
@app.route("/sys/admins/update", methods=["POST"])
@require_sysadmin
def sys_admins_update():
    try:
        check_csrf()
    except Exception:
        pass

    f = request.form
    op = (f.get("op") or "").strip()  # add | toggle | delete | resetpw
    s = SessionLocal()
    try:
        if op == "add":
            login_id = (f.get("login_id") or "").strip()
            name     = (f.get("name") or "").strip()
            password = (f.get("password") or "")
            if not (login_id and name and password):
                flash("必須項目が不足しています。"); return redirect(url_for("sys_admins"))
            if s.query(SysAdmin.id).filter(SysAdmin.login_id == login_id).first():
                flash("同じログインIDが既に存在します。"); return redirect(url_for("sys_admins"))
            s.add(SysAdmin(login_id=login_id,
                           password_hash=generate_password_hash(password),
                           name=name, active=1,
                           created_at=now_str(), updated_at=now_str()))
            s.commit(); flash("システム管理者を追加しました。")

        elif op == "toggle":
            uid = int(f.get("id") or 0); u = s.get(SysAdmin, uid)
            if u: u.active = 0 if u.active == 1 else 1; u.updated_at = now_str(); s.commit(); flash("有効/無効を更新しました。")

        elif op == "resetpw":
            uid = int(f.get("id") or 0); newpw = f.get("password") or ""; u = s.get(SysAdmin, uid)
            if u and newpw: u.password_hash = generate_password_hash(newpw); u.updated_at = now_str(); s.commit(); flash("パスワードを更新しました。")
            else: flash("パスワード更新に必要な情報が不足しています。")

        elif op == "delete":
            uid = int(f.get("id") or 0); u = s.get(SysAdmin, uid)
            if s.query(SysAdmin).count() <= 1:
                flash("最後のシステム管理者は削除できません。")
            elif u:
                s.delete(u); s.commit(); flash("削除しました。")

        else:
            flash("不正な操作です。")

        return redirect(url_for("sys_admins"))
    finally:
        s.close()



# ---------------------------------------------------------------------
# 共通ユーティリティ：安全な next URL 判定
# ---------------------------------------------------------------------

# --- 安全URL判定（同一オリジン or ルート相対のみ許可） -----------------------------
def _is_safe_url(target: str) -> bool:
    if not target:
        return False
    try:
        ref = urlparse(request.host_url)
        test = urlparse(target)
        # 絶対URL: 同一オリジンのみ許可
        if test.netloc:
            return (test.scheme in ("http", "https")) and (test.netloc == ref.netloc)
        # 相対URL: "/" 始まりかつ "//" 始まりは不可
        return target.startswith("/") and not target.startswith("//")
    except Exception:
        return False


# --- 戻り先URLの決定（?next → referrer → 既定） ---------------------------------
def _choose_next(default_url: str) -> str:
    cand = request.args.get("next")
    if cand and _is_safe_url(cand):
        return cand
    ref = request.referrer
    if ref and _is_safe_url(ref):
        return ref
    return default_url



# --- 動的カラム合成：テーブル直参照（COALESCE/別名付与） -------------------------
def _coalesce_name_expr(sess, table: str,
                        candidates=("名称", "店舗名", "店名", "name"),
                        alias="name") -> str:
    """
    テーブルに存在する列だけを利用して AS alias を返す（テーブル直参照用）
    SQLite/PostgreSQL両対応
      - 候補0:  NULL AS alias
      - 候補1:  "<col>" AS alias
      - 候補2+: COALESCE("<col1>", "<col2>", ...) AS alias
    """
    try:
        # データベースの種類を判定
        dialect = sess.bind.dialect.name if hasattr(sess.bind, 'dialect') else 'sqlite'
        
        if dialect == 'sqlite':
            # SQLite: PRAGMA table_info を使用
            cols = {r["name"] for r in sess.execute(text(f'PRAGMA table_info("{table}")')).mappings().all()}
        else:
            # PostgreSQL: information_schema.columns を使用
            cols = {
                r["column_name"] for r in sess.execute(
                    text("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = :table_name
                    """),
                    {"table_name": table}
                ).mappings().all()
            }
    except Exception as e:
        # エラー時はトランザクションをロールバックして空のセットを返す
        try:
            sess.rollback()
            app.logger.warning(f"[_coalesce_name_expr] カラム取得失敗 table={table}: {e}")
            # ロールバック後、新しいトランザクションを開始
            sess.begin()
        except Exception as rollback_error:
            app.logger.error(f"[_coalesce_name_expr] ロールバック失敗: {rollback_error}")
        cols = set()

    parts = [f'"{c}"' for c in candidates if c in cols]

    if len(parts) == 0:
        return f"NULL AS {alias}"
    elif len(parts) == 1:
        return f"{parts[0]} AS {alias}"
    else:
        return f"COALESCE({', '.join(parts)}) AS {alias}"


# --- 動的カラム合成：JOIN時（テーブル別名付き） -----------------------------------
def _coalesce_name_expr_alias(sess, table: str, table_alias="s",
                              candidates=("名称", "店舗名", "店名", "name"),
                              alias="name") -> str:
    """
    JOIN 時などテーブル別名付きで使う版
    SQLite/PostgreSQL両対応
      - 候補0:  NULL AS alias
      - 候補1:  s."<col>" AS alias
      - 候補2+: COALESCE(s."<col1>", s."<col2>", ...) AS alias
    """
    try:
        # データベースの種類を判定
        dialect = sess.bind.dialect.name if hasattr(sess.bind, 'dialect') else 'sqlite'
        
        if dialect == 'sqlite':
            # SQLite: PRAGMA table_info を使用
            cols = {r["name"] for r in sess.execute(text(f'PRAGMA table_info("{table}")')).mappings().all()}
        else:
            # PostgreSQL: information_schema.columns を使用
            cols = {
                r["column_name"] for r in sess.execute(
                    text("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = :table_name
                    """),
                    {"table_name": table}
                ).mappings().all()
            }
    except Exception as e:
        # エラー時はトランザクションをロールバックして空のセットを返す
        try:
            sess.rollback()
            app.logger.warning(f"[_coalesce_name_expr_alias] カラム取得失敗 table={table}: {e}")
            # ロールバック後、新しいトランザクションを開始
            sess.begin()
        except Exception as rollback_error:
            app.logger.error(f"[_coalesce_name_expr_alias] ロールバック失敗: {rollback_error}")
        cols = set()

    parts = [f'{table_alias}."{c}"' for c in candidates if c in cols]

    if len(parts) == 0:
        return f"NULL AS {alias}"
    elif len(parts) == 1:
        return f"{parts[0]} AS {alias}"
    else:
        return f"COALESCE({', '.join(parts)}) AS {alias}"





# ---------------------------------------------------------------------
# 売上集計で「統合済み」を除外する共通定義
# ---------------------------------------------------------------------
from sqlalchemy import or_, not_

EXCLUDE_MERGED_STATUSES = {
    "会計済(統合)", "統合済", "統合(会計済)",
    "merged", "merged_closed", "closed(merged)",
}

def exclude_merged_headers(q, OrderHeader):
    """
    OrderHeader を扱う SQLAlchemy Query に
    「統合済み系ステータス除外」を付与して返す。
    """
    if hasattr(OrderHeader, "status"):
        q = q.filter(
            or_(
                OrderHeader.status.is_(None),
                not_(OrderHeader.status.in_(list(EXCLUDE_MERGED_STATUSES)))
            )
        )
    return q




# ---------------------------------------------------------------------
# 取消を除外して伝票合計を算出するヘルパ
# ---------------------------------------------------------------------
def _is_item_cancelled(it) -> bool:
    """
    OrderItem 相当のオブジェクトについて「取消」かどうかを防御的に判定。
    - 真偽フラグ: is_cancel, is_cancelled, cancelled
    - 文字列: status, item_status, serve_status, state などに '取消', 'キャンセル', 'cancel', 'void' 等
    - qty が 0/負 でも取消とみなしたい場合は必要に応じて条件を追加
    """
    try:
        # フラグ系
        for nm in ("is_cancel", "is_cancelled", "cancelled"):
            if getattr(it, nm, None):
                return True

        # 文字列系
        raw = None
        for nm in ("status", "item_status", "serve_status", "state", "progress"):
            val = getattr(it, nm, None)
            if val not in (None, ""):
                raw = str(val).lower()
                break
        if raw:
            if ("取消" in raw) or ("ｷｬﾝｾﾙ" in raw) or ("キャンセル" in raw) or ("cancel" in raw) or ("void" in raw):
                return True

        # 数量が 0 の行を除外したいなら（任意）
        q = getattr(it, "qty", None)
        if q is not None:
            try:
                if int(q) <= 0:
                    return True
            except Exception:
                pass
    except Exception:
        pass
    return False


def _num_int(x, default=0):
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return default


def _num_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def _order_financials_excluding_cancels(session, order_id: int) -> dict:
    """
    取消行を除外して「小計(税抜) / 税額 / 合計 / 既払 / 残額」を返す。
    既払は PaymentRecord を参照（void/refund は除外、カラムが無ければ可能な範囲で集計）。
    """
    import math

    # --- 明細（取消除外）
    items = session.query(OrderItem).filter(getattr(OrderItem, "order_id") == order_id).all()
    sub_excl = 0
    tax_sum  = 0
    for it in items:
        if _is_item_cancelled(it):
            continue
        unit = _num_int(getattr(it, "unit_price", None))
        qty  = _num_int(getattr(it, "qty", None), 1)
        rate = _num_float(getattr(it, "tax_rate", None), 0.0)
        sub_excl += unit * qty
        tax_per_unit = int(math.floor(unit * rate))
        tax_sum  += tax_per_unit * qty

    total = int(sub_excl + tax_sum)

    # --- 支払（void/refund を除外できるだけ除外）
    paid = 0
    try:
        qpay = session.query(PaymentRecord).filter(getattr(PaymentRecord, "order_id") == order_id)

        # void/cancel/refund らしきフラグ/ステータスがあれば除外
        if hasattr(PaymentRecord, "is_void"):
            qpay = qpay.filter(PaymentRecord.is_void.is_(False))
        if hasattr(PaymentRecord, "status"):
            qpay = qpay.filter(~PaymentRecord.status.in_(["void", "cancelled", "refunded", "refund"]))

        # method で返金コードがあるなら除外（例：REFUND, RETURN など・必要に応じて増やしてOK）
        if hasattr(PaymentRecord, "method_code"):
            qpay = qpay.filter(~PaymentRecord.method_code.in_(["REFUND", "RETURN", "VOID"]))

        for p in qpay.all():
            paid += _num_int(getattr(p, "amount", None))
    except Exception:
        # PaymentRecord が無い/形が違う場合は 0 とする
        pass

    remaining = max(0, total - paid)

    return {
        "subtotal": int(sub_excl),
        "tax": int(tax_sum),
        "total": int(total),
        "paid": int(paid),
        "remaining": int(remaining),
    }




# ---------------------------------------------------------------------
# お客様詳細の復元ヘルパ
# ---------------------------------------------------------------------
def restore_customer_detail_from_history(s, order_id: int) -> bool:
    """
    T_お客様詳細履歴(例: CustomerDetailHistory) から該当 order_id の最新行を拾い、
    現在の "お客様詳細"（例: CustomerDetail や OrderHeader の来客カラム）へ復元する。
    モデル名やカラムが無い場合はスキップ（防御的に実装）。
    戻り値: 復元できたら True / 何もせず False
    """
    # 推定モデル名（あなたの環境に合わせて片方/両方存在）
    Hist = globals().get("CustomerDetailHistory") or globals().get("T_お客様詳細履歴")
    Curr = globals().get("CustomerDetail")       or globals().get("T_お客様詳細")

    if not Hist:
        # 履歴テーブルが無い場合は何もできない
        return False

    # 履歴の最新1件（降順）
    try:
        q = s.query(Hist).filter(getattr(Hist, "order_id") == order_id)
        # created_at が文字列の可能性/無い場合も考慮して複合ソート
        if hasattr(Hist, "created_at"):
            q = q.order_by(getattr(Hist, "created_at").desc())
        if hasattr(Hist, "id"):
            q = q.order_by(getattr(Hist, "id").desc())
        latest = q.first()
    except Exception:
        latest = None

    if not latest:
        return False

    # 履歴 → 値の抽出（カラム名のゆらぎを吸収）
    def _get(obj, *names, default=0):
        for n in names:
            if hasattr(obj, n):
                return getattr(obj, n) or 0
            if isinstance(obj, dict) and n in obj:
                return obj.get(n) or 0
        return default

    adult_male   = int(_get(latest, "adult_male", "大人男性", "男性", default=0))
    adult_female = int(_get(latest, "adult_female", "大人女性", "女性", default=0))
    boy          = int(_get(latest, "child_boy", "子ども男", "子ども男子", "男児", default=0))
    girl         = int(_get(latest, "child_girl", "子ども女", "子ども女子", "女児", default=0))
    total        = int(_get(latest, "total", "合計人数", default=adult_male+adult_female+boy+girl))

    restored = False

    # 1) 専用の現在テーブルがあるならそこへ反映
    if Curr:
        try:
            cur = (s.query(Curr)
                    .filter(getattr(Curr, "order_id") == order_id)
                    .order_by(getattr(Curr, "id").desc() if hasattr(Curr, "id") else None)
                    .first())
            if not cur:
                # 無ければ新規作成
                kwargs = {"order_id": order_id}
                if hasattr(Curr, "store_id") and hasattr(latest, "store_id"):
                    kwargs["store_id"] = getattr(latest, "store_id")
                cur = Curr(**kwargs)
                s.add(cur)

            # あれば入るカラムだけ埋める
            for name, val in [
                ("adult_male", adult_male), ("大人男性", adult_male),
                ("adult_female", adult_female), ("大人女性", adult_female),
                ("child_boy", boy), ("子ども男", boy), ("子ども男子", boy),
                ("child_girl", girl), ("子ども女", girl), ("子ども女子", girl),
                ("total", total), ("合計人数", total),
            ]:
                if hasattr(cur, name):
                    setattr(cur, name, val)
                    restored = True
        except Exception:
            pass

    # 2) OrderHeader 側にも来客系カラムがあれば反映（ダッシュボード等で参照していれば便利）
    OH = globals().get("OrderHeader")
    if OH:
        try:
            h = s.get(OH, order_id)
            if h:
                for name, val in [
                    ("guest_count", total), ("guests", total), ("合計人数", total),
                    ("adult_male", adult_male), ("adult_female", adult_female),
                    ("child_boy", boy), ("child_girl", girl),
                ]:
                    if hasattr(h, name):
                        setattr(h, name, val)
                        restored = True
        except Exception:
            pass

    return restored



# ---------------------------------------------------------------------
# guests current upsert helper (履歴 -> 現在値テーブル)
# ---------------------------------------------------------------------
def _upsert_guest_detail_from_history(session, order_id: int) -> dict:
    """
    T_お客様詳細履歴(= GuestDetailHistory) から order_id の最新人数を集計し、
    T_お客様詳細(= GuestDetail 現在値テーブル) へ upsert する。
    返り値: {"total":合計, "men":男大人, "women":女大人, "boys":男子, "girls":女子}
    ※ 実際のカラム名は環境ごとに異なるため、防御的に複数候補を見ます。
    """
    Hist = (globals().get("GuestDetailHistory")
            or globals().get("T_お客様詳細履歴")
            or globals().get("GuestDetailLog"))
    if Hist is None:
        return {"total": 0, "men": 0, "women": 0, "boys": 0, "girls": 0}

    Cur = (globals().get("GuestDetail")
           or globals().get("T_お客様詳細")
           or globals().get("CustomerDetail"))
    # 現在値テーブルが無い運用もあるため、その場合は no-op
    if Cur is None:
        return {"total": 0, "men": 0, "women": 0, "boys": 0, "girls": 0}

    from sqlalchemy import desc

    q = session.query(Hist).filter(getattr(Hist, "order_id") == order_id)
    # created_at / updated_at / id などで「最後」を決める
    if hasattr(Hist, "created_at"):
        q = q.order_by(desc(Hist.created_at))
    elif hasattr(Hist, "updated_at"):
        q = q.order_by(desc(Hist.updated_at))
    else:
        q = q.order_by(desc(Hist.id))
    rows = q.all()
    if not rows:
        return {"total": 0, "men": 0, "women": 0, "boys": 0, "girls": 0}

    def _ival(x): 
        try: return int(x or 0)
        except: return 0

    men = women = boys = girls = 0
    total_field_sum = 0
    for r in rows:
        men   += _ival(getattr(r, "大人男性", None) or getattr(r, "adult_male", None) or getattr(r, "men", None))
        women += _ival(getattr(r, "大人女性", None) or getattr(r, "adult_female", None) or getattr(r, "women", None))
        boys  += _ival(getattr(r, "子ども男", None) or getattr(r, "boys", None) or getattr(r, "子供男", None))
        girls += _ival(getattr(r, "子ども女", None) or getattr(r, "girls", None) or getattr(r, "子供女", None))
        total_field_sum += _ival(getattr(r, "合計人数", None) or getattr(r, "total", None) or getattr(r, "人数", None))

    parts_sum = men + women + boys + girls
    total = parts_sum if parts_sum > 0 else total_field_sum

    # 既存の現在値レコードを取得 or 新規作成
    cur = session.query(Cur).filter(getattr(Cur, "order_id") == order_id).first()
    if cur is None:
        cur = Cur()
        # よくある外部キー
        if hasattr(cur, "order_id"): cur.order_id = order_id
        # store_id / table_id が必要なら OrderHeader から補完
        H = globals().get("OrderHeader")
        if H:
            h = session.get(H, order_id)
            if h:
                if hasattr(cur, "store_id") and hasattr(h, "store_id"):
                    cur.store_id = getattr(h, "store_id", None)
                if hasattr(cur, "table_id") and hasattr(h, "table_id"):
                    cur.table_id = getattr(h, "table_id", None)
        session.add(cur)

    # 候補名に対して順に代入（存在するものだけ）
    def _set(obj, names, val):
        for nm in names:
            if hasattr(obj, nm):
                try:
                    setattr(obj, nm, val)
                    return True
                except Exception:
                    pass
        return False

    _set(cur, ("大人男性", "adult_male", "men"), men)
    _set(cur, ("大人女性", "adult_female", "women"), women)
    _set(cur, ("子ども男", "boys", "子供男"), boys)
    _set(cur, ("子ども女", "girls", "子供女"), girls)
    _set(cur, ("合計人数", "total", "人数"), total)

    # タイムスタンプ更新
    for f in ("updated_at", "更新日時"):
        if hasattr(cur, f):
            from datetime import datetime, timezone
            try:
                setattr(cur, f, datetime.now(timezone.utc))
            except Exception:
                pass

    return {"total": total, "men": men, "women": women, "boys": boys, "girls": girls}




# ---------------------------------------------------------------------
# 返金に使う支払方法IDを選ぶユーティリティ
# ---------------------------------------------------------------------
def _pick_refund_method_id(s, store_id, order_id):
    Pay = globals().get("PaymentRecord")
    PM  = globals().get("PaymentMethod")
    if Pay is None or PM is None:
        return None

    # 1) 同一注文の直近支払を流用
    try:
        last = (
            s.query(Pay)
             .filter(getattr(Pay, "order_id") == order_id)
             .order_by(getattr(Pay, "id").desc())
             .first()
        )
        if last is not None:
            mid = getattr(last, "method_id", None)
            if mid:
                return mid
    except Exception:
        pass

    # 2) 店舗のアクティブな支払方法から「REFUND or CASH」を優先
    q = s.query(PM).filter(
        getattr(PM, "store_id") == store_id,
        getattr(PM, "active") == 1
    )
    # 優先候補
    for code in ("REFUND", "CASH"):
        cand = q.filter(getattr(PM, "code") == code).order_by(getattr(PM, "display_order").asc()).first()
        if cand:
            return cand.id

    # 3) 先頭のアクティブ方法（表示順）
    any_pm = q.order_by(getattr(PM, "display_order").asc(), getattr(PM, "id").asc()).first()
    return any_pm.id if any_pm else None




# ---------------------------------------------------------------------
# 互換ラッパ：3引数/4引数どちらの _pick_refund_method_id でも呼べる
# ---------------------------------------------------------------------
def _call_pick_refund_method_id(s, store_id: int, order_id: int, method_code: str | None = None):
    pick = globals().get("_pick_refund_method_id")
    if not callable(pick):
        return None
    try:
        import inspect
        argc = len(inspect.signature(pick).parameters)
    except Exception:
        argc = 3  # 取得失敗時は3引数想定

    try:
        if argc >= 4:
            return pick(s, store_id, order_id, method_code)
        else:
            return pick(s, store_id, order_id)
    except TypeError:
        # 念のためフォールバック
        try:
            return pick(s, store_id, order_id)
        except Exception:
            return None




# ---------------------------------------------------------------------
# ヘッダ金額を再計算（負数量も合算してネット額を出す／内税）
# ---------------------------------------------------------------------
def _recalc_order_totals_with_negatives(order):
    import math

    subtotal_excl = 0  # 税抜小計（ネット）
    tax_total    = 0
    total_incl   = 0

    for d in getattr(order, "items", []) or []:
        # 数量（負も許容）
        qty = int(getattr(d, "qty", None) or getattr(d, "数量", None) or 0)
        if qty == 0:
            continue

        unit_excl = int(getattr(d, "unit_price", None) or getattr(d, "税抜単価", None) or 0)
        rate = float(getattr(d, "tax_rate", None) or 0.10)

        unit_tax  = math.floor(unit_excl * rate)
        unit_incl = unit_excl + unit_tax

        subtotal_excl += unit_excl * qty
        tax_total     += unit_tax  * qty
        total_incl    += unit_incl * qty

    # ヘッダへ反映
    if hasattr(order, "subtotal"): order.subtotal = int(subtotal_excl)
    if hasattr(order, "小計"):     setattr(order, "小計", int(subtotal_excl))
    if hasattr(order, "tax"):      order.tax = int(tax_total)
    if hasattr(order, "税額"):     setattr(order, "税額", int(tax_total))
    if hasattr(order, "total"):    order.total = int(total_incl)
    if hasattr(order, "合計"):     setattr(order, "合計", int(total_incl))

    for a in ("updated_at", "更新日時"):
        if hasattr(order, a):
            from datetime import datetime, timezone
            setattr(order, a, datetime.now(timezone.utc))
            break



# ---------------------------------------------------------------------
# 伝票ヘッダ再集計（DBベース／取消＝負数量を合算／内税）
# ---------------------------------------------------------------------
def _recalc_order_totals_with_negatives_db(s, order_id: int, order_obj=None):
    """DBから対象伝票の全明細を取得してネット額で再計算（内税）"""
    import math
    Item = globals().get("OrderItem")
    if Item is None:
        return

    debug_on = bool(getattr(current_app, "config", {}).get("DEBUG_TOTALS", False))

    items = (
        s.query(Item)
         .filter(getattr(Item, "order_id") == order_id)
         .all()
    )

    if debug_on:
        # 明細のサマリを出力（id, qty, unit_price, tax_rate など）
        try:
            dump_items = [{
                "id": getattr(d, "id", None),
                "qty": int(getattr(d, "qty", None) or getattr(d, "数量", None) or 0),
                "unit_excl": int(getattr(d, "unit_price", None) or getattr(d, "税抜単価", None) or 0),
                "tax_rate": float(getattr(d, "tax_rate", None) or 0.10),
                "status": getattr(d, "status", None) or getattr(d, "状態", None),
            } for d in items]
            current_app.logger.debug("[recalc.items] order_id=%s count=%s items=%s",
                                     order_id, len(items), dump_items)
        except Exception:
            pass

    subtotal_excl = 0
    tax_total     = 0
    total_incl    = 0

    for d in items or []:
        qty = int(getattr(d, "qty", None) or getattr(d, "数量", None) or 0)
        if qty == 0:
            continue
        unit_excl = int(getattr(d, "unit_price", None) or getattr(d, "税抜単価", None) or 0)
        rate = float(getattr(d, "tax_rate", None) or 0.10)
        unit_tax  = math.floor(unit_excl * rate)
        unit_incl = unit_excl + unit_tax
        subtotal_excl += unit_excl * qty
        tax_total     += unit_tax  * qty
        total_incl    += unit_incl * qty

    # ヘッダを取得
    Header = globals().get("OrderHeader")
    order = order_obj or (s.get(Header, order_id) if Header else None)
    if not order:
        return

    if debug_on:
        try:
            before = {
                "subtotal": getattr(order, "subtotal", getattr(order, "小計", None)),
                "tax": getattr(order, "tax", getattr(order, "税額", None)),
                "total": getattr(order, "total", getattr(order, "合計", None)),
            }
            current_app.logger.debug("[recalc.before] order_id=%s %s", order_id, before)
        except Exception:
            pass

    if hasattr(order, "subtotal"): order.subtotal = int(subtotal_excl)
    if hasattr(order, "小計"):     setattr(order, "小計", int(subtotal_excl))
    if hasattr(order, "tax"):      order.tax = int(tax_total)
    if hasattr(order, "税額"):     setattr(order, "税額", int(tax_total))
    if hasattr(order, "total"):    order.total = int(total_incl)
    if hasattr(order, "合計"):     setattr(order, "合計", int(total_incl))

    for a in ("updated_at", "更新日時"):
        if hasattr(order, a):
            from datetime import datetime, timezone
            setattr(order, a, datetime.now(timezone.utc))
            break

    if debug_on:
        try:
            after = {
                "subtotal": getattr(order, "subtotal", getattr(order, "小計", None)),
                "tax": getattr(order, "tax", getattr(order, "税額", None)),
                "total": getattr(order, "total", getattr(order, "合計", None)),
            }
            current_app.logger.debug("[recalc.after] order_id=%s %s", order_id, after)
        except Exception:
            pass


# ---------------------------------------------------------------------
# staff/admin 両方で使う：注文ID群の明細を“取消行・負数量も含めて”取得
# ---------------------------------------------------------------------
def _fetch_items_for_display(s, order_ids, include_cancel=True, include_negative=True):
    Item = globals().get("OrderItem")
    if not Item or not order_ids:
        return {}

    q = (s.query(Item)
           .filter(getattr(Item, "order_id").in_(order_ids))
           .order_by(getattr(Item, "id").asc()))

    # 取消除外 or 正数量のみ、の指定があれば適用（デフォルトは両方とも含める）
    if not include_cancel:
        # “status=取消 等”を除外する時だけ使う（今回は False のまま）
        q = q.filter((getattr(Item, "status") != "取消") | (getattr(Item, "status") == None))
    if not include_negative:
        # マイナス数量を除外する時だけ使う（今回は False のまま）
        q = q.filter((getattr(Item, "qty") > 0) | (getattr(Item, "数量") > 0))

    items_map = {oid: [] for oid in order_ids}
    for d in q.all():
        items_map.setdefault(getattr(d, "order_id"), []).append(d)
    return items_map



# ---------------------------------------------------------------------
# ★ヘルパ：メニュー別売上（負数量は必ず反映、正数量の取消は除外）
# ---------------------------------------------------------------------
def aggregate_menu_sales_by_menu(s, store_id, dt_from=None, dt_to=None, table_id=None):
    from sqlalchemy import and_
    import math

    Header = OrderHeader
    Item   = OrderItem
    M      = Menu

    # 期間・店舗のベース条件
    q = (
        s.query(Item, Header, M)
         .join(Header, Header.id == Item.order_id)
         .join(M, M.id == Item.menu_id)
    )
    if store_id is not None and hasattr(Header, "store_id"):
        q = q.filter(Header.store_id == store_id)
    if dt_from:
        q = q.filter(Header.opened_at >= dt_from)
    if dt_to:
        q = q.filter(Header.opened_at <  dt_to)
    if table_id:
        q = q.filter(Header.table_id == table_id)

    rows = q.all()

    out = {}  # key: menu_id
    for it, h, m in rows:
        qty  = int(getattr(it, "qty", None) or getattr(it, "数量", None) or 0)
        if qty == 0:
            continue

        # 取消ラベル判定（日本語/英語）
        st = (getattr(it, "status", None) or getattr(it, "状態", None) or "")
        st_low = str(st).lower()
        is_cancel_label = (
            ("取消" in st_low) or ("ｷｬﾝｾﾙ" in st_low) or ("キャンセル" in st_low)
            or ("cancel" in st_low) or ("void" in st_low)
        )

        # 正数量かつ取消ラベルは集計除外。負数量は “取消” でも必ず集計。
        if qty > 0 and is_cancel_label:
            continue

        unit_excl = int(getattr(it, "unit_price", None) or getattr(it, "税抜単価", None) or 0)
        # メニュー側/リンクの税率も考慮して取得する自前ヘルパがあればそれで置換
        rate = float(getattr(it, "tax_rate", None)
                     or getattr(getattr(it, "menu", None), "tax_rate", None)
                     or getattr(m, "tax_rate", None)
                     or 0.10)

        unit_tax  = math.floor(unit_excl * rate)
        unit_incl = unit_excl + unit_tax

        key = getattr(m, "id")
        name = getattr(m, "name")

        if key not in out:
            out[key] = {
                "menu_id": key,
                "name": name,
                "qty": 0,
                "excl": 0,
                "tax": 0,
                "incl": 0,
            }

        out[key]["qty"]  += qty                       # ← 負数量はそのままマイナス加算
        out[key]["excl"] += unit_excl * qty
        out[key]["tax"]  += unit_tax  * qty           # ← 単価ごとにfloor→数量
        out[key]["incl"] += unit_incl * qty

    # 表示用リスト（名前順などでソート）
    result = sorted(out.values(), key=lambda r: (r["name"], r["menu_id"]))
    return result



# ---------------------------------------------------------------------
# 調理中は進捗テーブルへ、提供済は明細へ、取消はマイナス行
# ---------------------------------------------------------------------
from sqlalchemy import text
from datetime import datetime

def _now_iso():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def progress_seed_if_needed(s, item):
    """
    進捗レコードが存在しない、またはすべてのカウンタが0の場合、
    明細の現在の状態に応じて適切なカウンタに初期化する
    """
    item_id = int(getattr(item, "id"))
    
    # ★ 既存レコードの確認を改善
    cur = s.execute(text("""
        SELECT qty_new, qty_cooking, qty_served, qty_canceled 
        FROM "T_明細進捗" WHERE item_id=:id
    """), {"id": item_id}).first()
    
    # レコードが存在し、かついずれかのカウンタが0より大きい場合はスキップ
    if cur and (cur[0] + cur[1] + cur[2] + cur[3]) > 0:
        return
    
    # レコードが存在しない、またはすべてのカウンタが0の場合は初期化
    qty = int(_get_any(item, "qty", "数量", default=0))
    if qty <= 0:
        return  # 数量0の明細は初期化しない
    
    # 明細の現在の状態に応じて適切なカウンタに振り分ける
    item_status = str(_get_any(item, "status", "状態", default="新規"))
    n, c, sv, cx = 0, 0, 0, 0
    
    if item_status in ("提供済", "served"):
        sv = qty  # ★ 提供済の場合は qty_served に入れる
    elif item_status in ("調理中", "cooking"):
        c = qty   # ★ 調理中の場合は qty_cooking に入れる
    elif item_status in ("取消", "cancel", "キャンセル"):
        cx = qty  # ★ 取消の場合は qty_canceled に入れる
    else:
        n = qty   # ★ それ以外（新規など）は qty_new に入れる
    
    s.execute(text("""
        INSERT INTO "T_明細進捗"(item_id, qty_new, qty_cooking, qty_served, qty_canceled, status, updated_at)
        VALUES (:id, :n, :c, :sv, :cx, :st, :ts)
        ON CONFLICT(item_id) DO UPDATE SET 
          qty_new = :n,
          qty_cooking = :c,
          qty_served = :sv,
          qty_canceled = :cx,
          status = :st,
          updated_at = :ts
    """), {"id": item_id, "n": n, "c": c, "sv": sv, "cx": cx, "st": item_status, "ts": _now_iso()})



def progress_set(s, item_id:int, n=None, c=None, sv=None, cx=None):
    # 任意のカラムだけ更新。新規作成時は status='新規' をセット
    sets, params = [], {"id": item_id, "ts": _now_iso()}
    if n  is not None: sets.append("qty_new=:n");       params["n"]=int(n)
    if c  is not None: sets.append("qty_cooking=:c");   params["c"]=int(c)
    if sv is not None: sets.append("qty_served=:sv");   params["sv"]=int(sv)
    if cx is not None: sets.append("qty_canceled=:cx"); params["cx"]=int(cx)
    if not sets:
        return

    s.execute(text(f"""
        INSERT INTO "T_明細進捗"(item_id, qty_new, qty_cooking, qty_served, qty_canceled, status, updated_at)
        VALUES (:id, COALESCE(:n,0), COALESCE(:c,0), COALESCE(:sv,0), COALESCE(:cx,0), '新規', :ts)
        ON CONFLICT(item_id) DO UPDATE SET {", ".join(sets)}, updated_at=:ts
    """), params)



def progress_finalize_if_done(s, item):
    """提供済+取消 == 元数量 なら、注文明細.status を『提供済』に確定。"""
    item_id = int(getattr(item, "id"))
    qty_orig = int(_get_any(item, "qty", "数量", default=0))
    p = progress_get(s, item_id)
    if p["qty_served"] + p["qty_canceled"] >= qty_orig:
        _set_first(item, ["status","状態"], "提供済")
        if hasattr(item, "updated_at"):
            item.updated_at = datetime.utcnow()
        # 調理中表示の掃除（任意）
        s.execute(text('UPDATE "T_明細進捗" SET qty_new=0, qty_cooking=0, updated_at=:ts WHERE item_id=:id'),
                  {"id": item_id, "ts": _now_iso()})
        return True
    return False


# ---------------------------------------------------------------------
# 部分移動を許可・実移動数を返す 
# ---------------------------------------------------------------------
def progress_move(s, item, to: str, count: int):
    """
    個数を『元→先』へ移動する（部分移動OK）。
      cooking : 新規→調理中
      served  : 調理中→提供済（なければ新規→提供済）
      cancel  : 新規→取消（なければ調理中→取消、さらに無ければ提供済→取消）
      new     : 提供済→新規（なければ調理中→新規）
    moved==0 のときのみエラー。
    ★ 進捗更新後、明細の status を進捗カウンタに同期
    """
    item_id = int(getattr(item, "id"))
    t = _norm_status(to)  # ★ 正規化

    p = progress_get(s, item_id)
    n, c, sv, cx = int(p["qty_new"]), int(p["qty_cooking"]), int(p["qty_served"]), int(p["qty_canceled"])

    def _move(from_key, to_key, k):
        nonlocal n, c, sv, cx
        stock = {"n": n, "c": c, "sv": sv, "cx": cx}[from_key]
        v = min(int(k), int(stock))
        if v <= 0:
            return 0
        if from_key == "n": n -= v
        elif from_key == "c": c -= v
        elif from_key == "sv": sv -= v
        elif from_key == "cx": cx -= v
        if to_key == "n": n += v
        elif to_key == "c": c += v
        elif to_key == "sv": sv += v
        elif to_key == "cx": cx += v
        return v

    k = int(count)
    moved = 0

    current_app.logger.debug("[PROG-MOVE] item_id=%s to=%s req=%s before(n=%s,c=%s,sv=%s,cx=%s)",
                             item_id, t, k, n, c, sv, cx)

    if t == "cooking":
        moved += _move("n", "c", k - moved)    # 新規→調理中
        moved += _move("sv", "c", k - moved)   # ★ 提供済→調理中（追加）
        if moved == 0:
            current_app.logger.debug("[PROG-MOVE][DENY] item_id=%s reason=new/served shortage", item_id)
            raise ValueError("移動可能数を超えています（新規/提供済不足）")

    elif t == "served":
        moved += _move("c", "sv", k - moved)
        moved += _move("n", "sv", k - moved)
        if moved == 0:
            current_app.logger.debug("[PROG-MOVE][DENY] item_id=%s reason=new/cooking shortage", item_id)
            raise ValueError("移動可能数を超えています（新規/調理中不足）")

    elif t == "cancel":
        moved += _move("n",  "cx", k - moved)
        moved += _move("c",  "cx", k - moved)
        moved += _move("sv", "cx", k - moved)   # ★ 提供済→取消 を許可
        if moved == 0:
            current_app.logger.debug("[PROG-MOVE][DENY] item_id=%s reason=new/cooking/served shortage", item_id)
            raise ValueError("移動可能数を超えています（新規/調理中/提供済不足）")

    elif t == "new":
        moved += _move("sv", "n", k - moved)
        moved += _move("c",  "n", k - moved)
        if moved == 0:
            current_app.logger.debug("[PROG-MOVE][DENY] item_id=%s reason=served/cooking shortage", item_id)
            raise ValueError("移動可能数を超えています（提供済/調理中不足）")

    else:
        raise ValueError("invalid to status")

    progress_set(s, item_id, n=n, c=c, sv=sv, cx=cx)

    # ★★★ 追加：明細の status を進捗カウンタに同期 ★★★
    try:
        # 優先順位：取消 > 提供済 > 調理中 > 新規
        new_status = None
        if cx > 0:
            new_status = "取消"
        elif sv > 0:
            new_status = "提供済"
        elif c > 0:
            new_status = "調理中"
        elif n > 0:
            new_status = "新規"
        
        if new_status and hasattr(item, "status"):
            old_status = getattr(item, "status", None)
            item.status = new_status
            current_app.logger.debug("[PROG-MOVE][SYNC] item_id=%s status: %s -> %s", 
                                   item_id, old_status, new_status)
    except Exception as e:
        current_app.logger.warning("[PROG-MOVE][SYNC] failed to sync status: %s", e)
        # status 同期失敗は致命的ではないので続行

    current_app.logger.debug("[PROG-MOVE][OK] item_id=%s to=%s moved=%s after(n=%s,c=%s,sv=%s,cx=%s)",
                             item_id, t, moved, n, c, sv, cx)
    return {"qty_new": n, "qty_cooking": c, "qty_served": sv, "qty_canceled": cx}, moved



# ---------------------------------------------------------------------
# 既存テーブルの救済 
# ---------------------------------------------------------------------
def migrate_progress_table_fix():
    eng = _shared_engine_or_none()
    if eng is None:
        return
    with eng.begin() as conn:
        # テーブルが無ければ作成
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS "T_明細進捗" (
                item_id INTEGER PRIMARY KEY,
                qty_new INTEGER NOT NULL DEFAULT 0,
                qty_cooking INTEGER NOT NULL DEFAULT 0,
                qty_served INTEGER NOT NULL DEFAULT 0,
                qty_canceled INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT '新規',
                updated_at TEXT
            )
        """))
        # status 列が無い古いDB向けに追加
        cols = [r[1] for r in conn.execute(text("PRAGMA table_info('T_明細進捗')")).fetchall()]
        if "status" not in cols:
            conn.execute(text('ALTER TABLE "T_明細進捗" ADD COLUMN status TEXT'))
            conn.execute(text('UPDATE "T_明細進捗" SET status=\'新規\' WHERE status IS NULL'))
            # SQLite は既存列に NOT NULL 付与が難しいため、運用で常に値を入れる方針
        else:
            # 既存の NULL を救済
            conn.execute(text('UPDATE "T_明細進捗" SET status=\'新規\' WHERE status IS NULL'))



# ---------------------------------------------------------------------
# 起動時マイグレーション：T_明細進捗 を用意して不足列を追加＆初期シード 
# ---------------------------------------------------------------------
from sqlalchemy import text

def migrate_progress_table():
    """
    T_明細進捗 を作成/拡張し、未作成の明細について初期シードを行う。
      - カラム：item_id / qty_new / qty_cooking / qty_served / qty_canceled / updated_at
      - 既存の T_注文明細 から、qty>0 の行だけ対象
        * status(状態)が「提供済」のものは qty_served に入れる
        * それ以外は qty_new に入れる
      - すでに T_明細進捗 に存在する item_id は INSERT OR IGNORE でスキップ
      - SQLite と PostgreSQL の両方に対応
    """
    eng = _shared_engine_or_none()
    if eng is None:
        print("[MIGRATE] engine not available; skip progress table migration")
        return

    # データベースの種類を判定
    is_sqlite = _dialect_is_sqlite(eng)
    
    with eng.begin() as conn:
        # 1) テーブル本体（無ければ作成）
        if is_sqlite:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS "T_明細進捗" (
                  item_id      INTEGER PRIMARY KEY,
                  qty_new      INTEGER NOT NULL DEFAULT 0,
                  qty_cooking  INTEGER NOT NULL DEFAULT 0,
                  qty_served   INTEGER NOT NULL DEFAULT 0,
                  qty_canceled INTEGER NOT NULL DEFAULT 0,
                  updated_at   TEXT
                )
            """))
        else:
            # PostgreSQL
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS "T_明細進捗" (
                  item_id      INTEGER PRIMARY KEY,
                  qty_new      INTEGER NOT NULL DEFAULT 0,
                  qty_cooking  INTEGER NOT NULL DEFAULT 0,
                  qty_served   INTEGER NOT NULL DEFAULT 0,
                  qty_canceled INTEGER NOT NULL DEFAULT 0,
                  updated_at   TIMESTAMP
                )
            """))

        # 2) 足りない列を順次追加（後方互換）
        if is_sqlite:
            cols = [r[1] for r in conn.execute(text('PRAGMA table_info("T_明細進捗")')).fetchall()]
            def ensure(col, ddl_type):
                nonlocal cols
                if col not in cols:
                    conn.execute(text(f'ALTER TABLE "T_明細進捗" ADD COLUMN {col} {ddl_type}'))
                    cols.append(col)

            ensure("qty_new",      "INTEGER NOT NULL DEFAULT 0")
            ensure("qty_cooking",  "INTEGER NOT NULL DEFAULT 0")
            ensure("qty_served",   "INTEGER NOT NULL DEFAULT 0")
            ensure("qty_canceled", "INTEGER NOT NULL DEFAULT 0")
            ensure("updated_at",   "TEXT")
        else:
            # PostgreSQL: information_schema で列の存在確認
            def col_exists(col_name):
                r = conn.execute(text("""
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='T_明細進捗' AND column_name=:col
                """), {"col": col_name}).first()
                return r is not None
            
            def ensure_pg(col, ddl_type):
                if not col_exists(col):
                    conn.execute(text(f'ALTER TABLE "T_明細進捗" ADD COLUMN {col} {ddl_type}'))
            
            ensure_pg("qty_new",      "INTEGER NOT NULL DEFAULT 0")
            ensure_pg("qty_cooking",  "INTEGER NOT NULL DEFAULT 0")
            ensure_pg("qty_served",   "INTEGER NOT NULL DEFAULT 0")
            ensure_pg("qty_canceled", "INTEGER NOT NULL DEFAULT 0")
            ensure_pg("updated_at",   "TIMESTAMP")

        # 3) 初期シード（qty>0 かつ 進捗未登録の item_id を対象）
        #    * 提供済は qty_served、それ以外は qty_new に立てる
        if is_sqlite:
            conn.execute(text("""
                INSERT OR IGNORE INTO "T_明細進捗"
                  (item_id, qty_new, qty_cooking, qty_served, qty_canceled, updated_at)
                SELECT
                  oi.id AS item_id,
                  CASE WHEN COALESCE(oi.status, COALESCE(oi."状態", '')) = '提供済'
                       THEN 0 ELSE COALESCE(oi.qty, COALESCE(oi."数量", 0)) END AS qty_new,
                  0 AS qty_cooking,
                  CASE WHEN COALESCE(oi.status, COALESCE(oi."状態", '')) = '提供済'
                       THEN COALESCE(oi.qty, COALESCE(oi."数量", 0)) ELSE 0 END AS qty_served,
                  0 AS qty_canceled,
                  strftime('%Y-%m-%d %H:%M:%S','now') AS updated_at
                FROM "T_注文明細" AS oi
                WHERE COALESCE(oi.qty, COALESCE(oi."数量", 0)) > 0
                  AND oi.id NOT IN (SELECT item_id FROM "T_明細進捗")
            """))
        else:
            # PostgreSQL
            conn.execute(text("""
                INSERT INTO "T_明細進捗"
                  (item_id, qty_new, qty_cooking, qty_served, qty_canceled, updated_at)
                SELECT
                  oi.id AS item_id,
                  CASE WHEN COALESCE(oi.status, COALESCE(oi."状態", '')) = '提供済'
                       THEN 0 ELSE COALESCE(oi.qty, COALESCE(oi."数量", 0)) END AS qty_new,
                  0 AS qty_cooking,
                  CASE WHEN COALESCE(oi.status, COALESCE(oi."状態", '')) = '提供済'
                       THEN COALESCE(oi.qty, COALESCE(oi."数量", 0)) ELSE 0 END AS qty_served,
                  0 AS qty_canceled,
                  NOW() AS updated_at
                FROM "T_注文明細" AS oi
                WHERE COALESCE(oi.qty, COALESCE(oi."数量", 0)) > 0
                  AND oi.id NOT IN (SELECT item_id FROM "T_明細進捗")
                ON CONFLICT (item_id) DO NOTHING
            """))

    print("[MIGRATE] T_明細進捗 ready (created/altered/seeded)")




# ---------------------------------------------------------------------
# Progress helpers (put this ABOVE any routes that use them)
# ---------------------------------------------------------------------
from flask import current_app
from sqlalchemy import text
from datetime import datetime

def _now_iso():
    # UTCでOK（アプリ全体UTC基準）
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def progress_get(s, item_id:int):
    row = s.execute(text("""
        SELECT qty_new, qty_cooking, qty_served, qty_canceled
        FROM "T_明細進捗" WHERE item_id=:id
    """), {"id": item_id}).mappings().first()
    if not row:
        return {"qty_new":0,"qty_cooking":0,"qty_served":0,"qty_canceled":0}
    return {
        "qty_new": int(row["qty_new"] or 0),
        "qty_cooking": int(row["qty_cooking"] or 0),
        "qty_served": int(row["qty_served"] or 0),
        "qty_canceled": int(row["qty_canceled"] or 0),
    }

def progress_seed_if_needed(s, item):
    # ★ 既存レコードの確認を改善
    cur = s.execute(text("""
        SELECT qty_new, qty_cooking, qty_served, qty_canceled 
        FROM "T_明細進捗" WHERE item_id=:id
    """), {"id": item.id}).first()
    
    # レコードが存在し、かついずれかのカウンタが0より大きい場合はスキップ
    if cur and (cur[0] + cur[1] + cur[2] + cur[3]) > 0:
        return
    
    # レコードが存在しない、またはすべてのカウンタが0の場合は初期化
    qty = int(_get_any(item, "qty", "数量", default=0))
    if qty <= 0:
        return  # 数量0の明細は初期化しない
    
    # 明細の現在の状態に応じて適切なカウンタに振り分ける
    item_status = str(_get_any(item, "status", "状態", default="新規"))
    n, c, sv, cx = 0, 0, 0, 0
    
    if item_status in ("提供済", "served"):
        sv = qty  # ★ 提供済の場合は qty_served に入れる
    elif item_status in ("調理中", "cooking"):
        c = qty   # ★ 調理中の場合は qty_cooking に入れる
    elif item_status in ("取消", "cancel", "キャンセル"):
        cx = qty  # ★ 取消の場合は qty_canceled に入れる
    else:
        n = qty   # ★ それ以外（新規など）は qty_new に入れる
    
    s.execute(text("""
        INSERT INTO "T_明細進捗"(item_id, qty_new, qty_cooking, qty_served, qty_canceled, status, updated_at)
        VALUES (:id, :n, :c, :sv, :cx, :st, :ts)
        ON CONFLICT(item_id) DO UPDATE SET 
          qty_new = :n,
          qty_cooking = :c,
          qty_served = :sv,
          qty_canceled = :cx,
          status = :st,
          updated_at = :ts
    """), {"id": item.id, "n": n, "c": c, "sv": sv, "cx": cx, "st": item_status, "ts": _now_iso()})

def progress_set(s, item_id:int, n=None, c=None, sv=None, cx=None):
    # 任意のカラムだけ更新（status は別管理。ここでは触らない）
    sets, params = [], {"id": item_id, "ts": _now_iso()}
    if n  is not None: sets.append("qty_new=:n");       params["n"]=int(n)
    if c  is not None: sets.append("qty_cooking=:c");   params["c"]=int(c)
    if sv is not None: sets.append("qty_served=:sv");   params["sv"]=int(sv)
    if cx is not None: sets.append("qty_canceled=:cx"); params["cx"]=int(cx)
    if not sets: 
        return
    s.execute(text(f"""
        UPDATE "T_明細進捗"
           SET {", ".join(sets)}, updated_at=:ts
         WHERE item_id=:id
    """), params)


def progress_finalize_if_done(s, item):
    """
    提供済 + 取消 == 元数量 なら、注文明細.status を『提供済』に確定し、
    進捗の新規/調理中を0にリセット
    """
    item_id = int(getattr(item, "id"))
    qty_orig = int(_get_any(item, "qty", "数量", default=0))
    p = progress_get(s, item_id)
    if (p["qty_served"] + p["qty_canceled"]) >= qty_orig:
        _set_first(item, ["status","状態"], "提供済")
        if hasattr(item, "updated_at"):
            item.updated_at = datetime.utcnow()
        s.execute(
            text('UPDATE "T_明細進捗" SET qty_new=0, qty_cooking=0, updated_at=:ts WHERE item_id=:id'),
            {"id": item_id, "ts": _now_iso()}
        )
        return True
    return False



# ---------------------------------------------------------------------
# 管理者：マイページ
# ---------------------------------------------------------------------

# --- 管理者マイページ（プロフィール編集／所属店舗一覧／現在店舗表示） -------------
@app.route("/admin/mypage", methods=["GET", "POST"])
@require_admin
def admin_mypage():
    s = SessionLocal()
    try:
        admin_id = (
            session.get("admin_id")
            or session.get("store_admin_id")
            or session.get("user_id")
        )
        if not admin_id:
            flash("管理者としてログインしてください。")
            return redirect(url_for("admin_login", next=_choose_next(url_for("index"))))

        me = s.execute(
            text('SELECT * FROM "M_管理者" WHERE id = :id'),
            {"id": admin_id}
        ).mappings().first()
        if not me:
            flash("管理者情報が見つかりません。")
            return redirect(url_for("admin_login", next=_choose_next(url_for("index"))))

        # --- 店舗ミニ情報の取得（動的名称カラム対応） -----------------------------
        def _fetch_store_by_id(store_id: int | None):
            if not store_id:
                return None
            name_expr = _coalesce_name_expr(s, "M_店舗")
            row = s.execute(
                text(f'SELECT id, {name_expr} FROM "M_店舗" WHERE id = :id'),
                {"id": int(store_id)}
            ).mappings().first()
            if not row:
                return None
            return {"id": int(row["id"]), "name": row.get("name") or f"店舗#{row['id']}"}

        # --- 所属店舗収集（セッション／管理者レコード／中間表） -------------------
        stores_raw = []

        # 1) 現在店舗（セッション）
        if session.get("store_id"):
            st = _fetch_store_by_id(int(session["store_id"]))
            if st: stores_raw.append(st)

        # 2) 管理者レコード側の店舗ID
        for key in ("store_id", "店舗ID", "店舗_id", "StoreId", "StoreID"):
            try:
                sid = me.get(key) if hasattr(me, "get") else None
            except Exception:
                sid = None
            if sid:
                st = _fetch_store_by_id(int(sid))
                if st: stores_raw.append(st)
                break

        # 3) 中間表からの一覧（存在する方だけ通す）
        name_expr_s = _coalesce_name_expr_alias(s, "M_店舗", table_alias="s")
        for sql in [
            f'''
              SELECT s.id, {name_expr_s}
              FROM "M_店舗" s
              JOIN "M_店舗管理者" a ON a."店舗ID" = s.id
              WHERE a."管理者ID" = :admin_id
              ORDER BY s.id
            ''',
            f'''
              SELECT s.id, {name_expr_s}
              FROM "M_店舗" s
              JOIN "M_管理者店舗" a ON a."店舗ID" = s.id
              WHERE a."管理者ID" = :admin_id
              ORDER BY s.id
            ''',
        ]:
            try:
                rows = s.execute(text(sql), {"admin_id": admin_id}).mappings().all()
                for r in rows:
                    stores_raw.append({"id": int(r["id"]), "name": r.get("name") or f"店舗#{r['id']}"})
                break
            except Exception:
                pass

        # --- 重複除去 & 現在店舗優先で並び替え -----------------------------------
        uniq = {}
        for st in stores_raw:
            if st and st.get("id"):
                uniq[int(st["id"])] = {"id": int(st["id"]), "name": st["name"]}
        stores = list(uniq.values())

        current_store_id = int(session.get("store_id") or 0)
        stores.sort(key=lambda x: (0 if x["id"] == current_store_id else 1, x["name"] or ""))

        store = _fetch_store_by_id(current_store_id) if current_store_id else None

        # --- POST：プロフィール更新／パスワード（未実装） --------------------------
        if request.method == "POST":
            action = request.form.get("action")
            if action == "profile":
                name = (request.form.get("name") or "").strip()
                login_id = (request.form.get("login_id") or "").strip()
                # ★ スキーマに応じて name/login_id の列名を調整してください
                s.execute(
                    text('UPDATE "M_管理者" SET name=:n, login_id=:l, updated_at=CURRENT_TIMESTAMP WHERE id=:id'),
                    {"n": name, "l": login_id, "id": admin_id}
                )
                s.commit()
                flash("プロフィールを更新しました。")
                return redirect(url_for("admin_mypage"))
            elif action == "password":
                flash("パスワード変更は未実装です。")
                return redirect(url_for("admin_mypage"))

        # --- 画面描画 ------------------------------------------------------------
        return render_template(
            "admin_mypage.html",
            title=f"{APP_TITLE} | 管理者マイページ",
            me=me,
            store=store,
            stores=stores,
            csrf_token=session.get("csrf_token")
        )
    finally:
        s.close()


# ---------------------------------------------------------------------
# 店舗切替（管理者・従業員 共通ルート）
# ---------------------------------------------------------------------

# --- 店舗切替（権限検証→セッション更新→元画面へ遷移） ----------------------------
ALLOW_IF_LINK_TABLE_MISSING = True

@app.route("/switch_store/<int:store_id>")
def switch_store(store_id: int):
    role = session.get("role")
    default_next = (
        url_for("staff_mypage") if role == "staff"
        else url_for("admin_mypage") if role in ("store_admin", "tenant_admin", "sysadmin")
        else url_for("index")
    )
    next_url = _choose_next(default_next)

    # 未ログインは役割別ログインへ
    if not session.get("logged_in"):
        if role == "staff":
            return redirect(url_for("staff_login", next=next_url))
        elif role in ("store_admin", "tenant_admin", "sysadmin"):
            return redirect(url_for("admin_login", next=next_url))
        else:
            return redirect(url_for("login_choice"))

    s = SessionLocal()
    try:
        # 店舗存在チェック（動的 name 式）
        name_expr = _coalesce_name_expr(s, "M_店舗")
        store = s.execute(
            text(f'SELECT id, {name_expr} FROM "M_店舗" WHERE id = :id'),
            {"id": int(store_id)}
        ).mappings().first()
        if not store:
            flash("指定の店舗が見つかりません。")
            return redirect(next_url)

        # 権限チェック（管理者系／スタッフ系）
        allowed = False
        if role in ("store_admin", "tenant_admin", "sysadmin"):
            admin_id = session.get("admin_id") or session.get("store_admin_id") or session.get("user_id")
            for sql in [
                'SELECT 1 FROM "M_店舗管理者" WHERE "管理者ID" = :uid AND "店舗ID" = :sid LIMIT 1',
                'SELECT 1 FROM "M_管理者店舗" WHERE "管理者ID" = :uid AND "店舗ID" = :sid LIMIT 1',
            ]:
                try:
                    ok = s.execute(text(sql), {"uid": admin_id, "sid": int(store_id)}).first()
                    if ok: allowed = True; break
                except Exception:
                    allowed = ALLOW_IF_LINK_TABLE_MISSING
                    if allowed: break

        elif role == "staff":
            staff_id = session.get("staff_id") or session.get("user_id")
            for sql in [
                'SELECT 1 FROM "M_従業員店舗" WHERE "従業員ID" = :uid AND "店舗ID" = :sid LIMIT 1',
                'SELECT 1 FROM "M_店舗従業員" WHERE "従業員ID" = :uid AND "店舗ID" = :sid LIMIT 1',
                'SELECT 1 FROM "M_店舗スタッフ" WHERE "従業員ID" = :uid AND "店舗ID" = :sid LIMIT 1',
            ]:
                try:
                    ok = s.execute(text(sql), {"uid": staff_id, "sid": int(store_id)}).first()
                    if ok: allowed = True; break
                except Exception:
                    allowed = ALLOW_IF_LINK_TABLE_MISSING
                    if allowed: break

        if not allowed:
            flash("この店舗へ切り替える権限がありません。")
            return redirect(next_url)

        # セッション切替
        session["store_id"] = int(store["id"])
        session["store_name"] = store.get("name") or f"店舗#{store['id']}"
        flash(f'店舗を「{session["store_name"]}」に切り替えました。')
        return redirect(next_url)
    finally:
        s.close()


# ---------------------------------------------------------------------
# 従業員：マイページ（所属店舗一覧＋切替対応）
# ---------------------------------------------------------------------

# --- 従業員マイページ（プロフィール編集／所属店舗一覧／現在店舗表示） -------------
@app.route("/staff/mypage", methods=["GET", "POST"])
@require_staff
def staff_mypage():
    s = SessionLocal()
    try:
        staff_id = session.get("staff_id") or session.get("user_id")
        if not staff_id:
            flash("従業員としてログインしてください。")
            return redirect(url_for("staff_login", next=_choose_next(url_for("index"))))

        me = s.execute(
            text('SELECT * FROM "M_従業員" WHERE id = :id'),
            {"id": staff_id}
        ).mappings().first()
        if not me:
            flash("従業員情報が見つかりません。")
            return redirect(url_for("staff_login", next=_choose_next(url_for("index"))))

        # --- 店舗ミニ情報の取得（動的名称カラム対応） -----------------------------
        def _fetch_store_min(store_id: int | None):
            if not store_id:
                return None
            name_expr = _coalesce_name_expr(s, "M_店舗")
            row = s.execute(
                text(f'SELECT id, {name_expr} FROM "M_店舗" WHERE id = :id'),
                {"id": int(store_id)}
            ).mappings().first()
            if not row:
                return None
            return {"id": int(row["id"]), "name": row.get("name") or f"店舗#{row['id']}"}

        # --- 所属店舗収集（セッション／従業員レコード／中間表） -------------------
        stores_raw = []

        # 1) 現在店舗（セッション）
        if session.get("store_id"):
            st = _fetch_store_min(int(session["store_id"]))
            if st: stores_raw.append(st)

        # 2) 従業員レコード側の店舗ID
        store_id_from_me = None
        for key in ("store_id", "店舗ID", "店舗_id", "StoreId", "StoreID"):
            try:
                store_id_from_me = me.get(key) if hasattr(me, "get") else None
            except Exception:
                store_id_from_me = None
            if store_id_from_me:
                break
        if store_id_from_me:
            st = _fetch_store_min(int(store_id_from_me))
            if st: stores_raw.append(st)

        # 3) 中間表（存在する方だけ通す）
        name_expr_s = _coalesce_name_expr_alias(s, "M_店舗", table_alias="s")
        for sql in [
            f'''
              SELECT s.id, {name_expr_s}
              FROM "M_店舗" s
              JOIN "M_従業員店舗" j ON j."店舗ID" = s.id
              WHERE j."従業員ID" = :staff_id
              ORDER BY s.id
            ''',
            f'''
              SELECT s.id, {name_expr_s}
              FROM "M_店舗" s
              JOIN "M_店舗従業員" j ON j."店舗ID" = s.id
              WHERE j."従業員ID" = :staff_id
              ORDER BY s.id
            ''',
            f'''
              SELECT s.id, {name_expr_s}
              FROM "M_店舗" s
              JOIN "M_店舗スタッフ" j ON j."店舗ID" = s.id
              WHERE j."従業員ID" = :staff_id
              ORDER BY s.id
            ''',
        ]:
            try:
                rows = s.execute(text(sql), {"staff_id": staff_id}).mappings().all()
                if rows:
                    for r in rows:
                        stores_raw.append({"id": int(r["id"]), "name": r.get("name") or f"店舗#{r['id']}"})
                    break
            except Exception:
                pass

        # --- 重複除去 & 現在店舗優先で並び替え -----------------------------------
        uniq = {}
        for st in stores_raw:
            if st and st.get("id"):
                uniq[int(st["id"])] = {"id": int(st["id"]), "name": st["name"]}
        stores = list(uniq.values())

        current_store_id = int(session.get("store_id") or 0)
        stores.sort(key=lambda x: (0 if x["id"] == current_store_id else 1, x["name"] or ""))

        store = _fetch_store_min(current_store_id) if current_store_id else None

        # --- POST：プロフィール更新／パスワード（未実装） --------------------------
        if request.method == "POST":
            action = request.form.get("action")

            if action == "profile":
                name = (request.form.get("name") or "").strip()
                login_id = (request.form.get("login_id") or "").strip()
                # ★ スキーマに応じて列名を調整してください
                s.execute(
                    text('UPDATE "M_従業員" SET name=:n, login_id=:l, updated_at=CURRENT_TIMESTAMP WHERE id=:id'),
                    {"n": name, "l": login_id, "id": staff_id}
                )
                s.commit()
                flash("プロフィールを更新しました。")
                return redirect(url_for("staff_mypage"))

            elif action == "password":
                flash("パスワード変更は未実装です。")
                return redirect(url_for("staff_mypage"))

        # --- 画面描画 ------------------------------------------------------------
        return render_template(
            "staff_mypage.html",
            title=f"{APP_TITLE} | 従業員マイページ",
            me=me,
            store=store,
            stores=stores,
            csrf_token=session.get("csrf_token")
        )

    finally:
        s.close()



# ---------------------------------------------------------------------
# 既存データの修復スクリプト
# ---------------------------------------------------------------------
@app.route("/admin/fix_progress_data", methods=["POST"])
@require_admin
def admin_fix_progress_data():
    """既存の注文明細の進捗カウンタを修復"""
    s = SessionLocal()
    try:
        # すべての注文明細を取得
        items = s.query(OrderItem).filter(OrderItem.qty > 0).all()
        fixed_count = 0
        
        for item in items:
            item_id = item.id
            qty = int(getattr(item, "qty", 0))
            status = str(getattr(item, "status", "新規"))
            
            # 既存の進捗レコードを確認
            prog = s.execute(text("""
                SELECT qty_new, qty_cooking, qty_served, qty_canceled 
                FROM "T_明細進捗" WHERE item_id=:id
            """), {"id": item_id}).first()
            
            # レコードが存在しない、またはすべてのカウンタが0の場合のみ修復
            if not prog or (prog[0] + prog[1] + prog[2] + prog[3]) == 0:
                n, c, sv, cx = 0, 0, 0, 0
                
                if status in ("提供済", "served"):
                    sv = qty
                elif status in ("調理中", "cooking"):
                    c = qty
                elif status in ("取消", "cancel"):
                    cx = qty
                else:
                    n = qty
                
                s.execute(text("""
                    INSERT INTO "T_明細進捗"(item_id, qty_new, qty_cooking, qty_served, qty_canceled, status, updated_at)
                    VALUES (:id, :n, :c, :sv, :cx, :st, :ts)
                    ON CONFLICT(item_id) DO UPDATE SET 
                      qty_new = :n,
                      qty_cooking = :c,
                      qty_served = :sv,
                      qty_canceled = :cx,
                      status = :st,
                      updated_at = :ts
                """), {"id": item_id, "n": n, "c": c, "sv": sv, "cx": cx, "st": status, "ts": _now_iso()})
                
                fixed_count += 1
        
        s.commit()
        return jsonify({"ok": True, "fixed_count": fixed_count})
    
    except Exception as e:
        s.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        s.close()




# ---------------------------------------------------------------------
# 注文明細（table_detail）からの取消処理
# ---------------------------------------------------------------------
# ---- ヘルパ：属性/日本語カラムの動的アクセス ----
def _get_any(o, *names, default=None):
    for k in names:
        if hasattr(o, k):
            return getattr(o, k)
        # dict 互換にも対応（query結果をdict化している場合に備え）
        if isinstance(o, dict) and k in o:
            return o.get(k)
    return default

def _set_first(o, names, value):
    """names で最初に存在する属性へ set。無ければ names[0] を setattr する。"""
    for k in names:
        if hasattr(o, k):
            setattr(o, k, value)
            return k
    # 既存属性が見つからない場合は強制生成（SQLAlchemyなら通常発生しない）
    setattr(o, names[0], value)
    return names[0]

def _copy_if_exists(dst, src, pairs):
    """pairs=[(dst候補群, src候補群)] をなぞって値コピー"""
    for dst_names, src_names in pairs:
        val = _get_any(src, *src_names, default=None)
        if val is not None:
            _set_first(dst, dst_names, val)

# ---- モデル別名の吸い上げ（英語/日本語どちらでも動くように） ----
def _models():
    OrderItem = (globals().get("T_注文明細")
                 or globals().get("T_注文詳細")
                 or globals().get("OrderItem"))
    Menu = (globals().get("M_メニュー")
            or globals().get("M_商品")
            or globals().get("Menu"))
    return OrderItem, Menu

def _get_any(o, *names, default=None):
    for k in names:
        if hasattr(o, k):
            return getattr(o, k)
        if isinstance(o, dict) and k in o:
            return o.get(k)
    return default

def _set_first(o, names, value):
    for k in names:
        if hasattr(o, k):
            setattr(o, k, value)
            return k
    setattr(o, names[0], value)
    return names[0]

def _copy_if_exists(dst, src, pairs):
    for dst_names, src_names in pairs:
        val = _get_any(src, *src_names, default=None)
        if val is not None:
            _set_first(dst, dst_names, val)

def _models():
    OrderItem = (globals().get("T_注文明細")
                 or globals().get("T_注文詳細")
                 or globals().get("OrderItem"))
    Menu = (globals().get("M_メニュー")
            or globals().get("M_商品")
            or globals().get("Menu"))
    return OrderItem, Menu

def _guess_tax_rate(src_item=None, menu=None):
    """必ず float を返す。優先順位: item -> menu -> (税込/税抜から推計) -> 既定値"""
    cand = None
    if src_item is not None:
        cand = _get_any(src_item, "税率", "tax_rate", "消費税率", "tax", default=None)
    if cand is None and menu is not None:
        cand = _get_any(menu, "税率", "tax_rate", "消費税率", "tax", default=None)
    if cand is None and src_item is not None:
        excl = _get_any(src_item, "税抜単価", "単価", "unit_price", default=None)
        incl = _get_any(src_item, "税込単価", "price_incl", default=None)
        try:
            if excl is not None and incl is not None and float(excl) > 0:
                cand = max(0.0, (float(incl) - float(excl)) / float(excl))
        except Exception:
            cand = None
    if cand is None:
        cand = current_app.config.get("TAX_RATE_DEFAULT", 0.10)
    try:
        return float(cand)
    except Exception:
        return 0.10


# ---- 取消API：数量nだけの部分取消で「マイナス監査行」を新規作成 ----
@app.post("/staff/api/order_item/<int:item_id>/status")
@require_staff
def staff_api_order_item_status(item_id: int):
    """
    JSON: { "status": "調理中|提供済|取消", "count": <int>=1 }
      - 進捗は T_明細進捗 の qty_* を「個数移動」で表現
      - 取消は進捗移動に加えて、監査用のマイナス行も T_注文明細 に追加
      - 提供済 + 取消 == 元数量 になったら、注文明細.status を「提供済」に自動確定
    """
    s = SessionLocal()
    try:
        j = request.get_json(force=True) or {}
        status = str(j.get("status") or "").strip()
        count  = int(j.get("count") or 1)
        if count <= 0:
            return jsonify({"ok": False, "error": "count must be >= 1"}), 400

        OrderItem, Menu = _models()
        it = s.get(OrderItem, item_id)
        if not it:
            return jsonify({"ok": False, "error": "item not found"}), 404

        qty_orig = int(_get_any(it, "qty", "数量", default=0))
        if qty_orig <= 0:
            return jsonify({"ok": False, "error": "quantity is zero"}), 400

        # 進捗エントリが無ければシード（qty_new = 元数量）
        progress_seed_if_needed(s, it)

        # ---- 取消は監査用マイナス行も作るので、税率等を先に準備
        will_cancel = status in ["取消", "ｷｬﾝｾﾙ", "キャンセル", "cancel", "void", "VOID", "Cancel"]
        tax_rate = None
        if will_cancel:
            menu_id = _get_any(it, "menu_id", "メニューid", "商品id")
            menu = s.get(Menu, menu_id) if menu_id is not None else None
            tax_rate = _guess_tax_rate(src_item=it, menu=menu)

        # ---- 進捗カウンタの移動（超過は ValueError）
        if status not in ["新規", "new", "調理中", "cooking", "提供済", "served"] + ["取消", "ｷｬﾝｾﾙ", "キャンセル", "cancel", "void", "VOID", "Cancel"]:
            return jsonify({"ok": False, "error": "invalid status"}), 400

        try:
            p_after = progress_move(s, it, status, count)  # qty_* を移動
        except ValueError as e:
            s.rollback()
            return jsonify({"ok": False, "error": str(e)}), 400

        neg_id = None
        # ---- 取消：監査用のマイナス行を追加
        if will_cancel:
            neg = OrderItem()
            _copy_if_exists(neg, it, [
                (["order_id","注文id","注文ID"], ["order_id","注文id","注文ID"]),
                (["menu_id","メニューid","商品id"], ["menu_id","メニューid","商品id"]),
                (["store_id","店舗ID"], ["store_id","店舗ID"]),
                (["tenant_id"], ["tenant_id"]),
                (["name","名称"], ["name","名称"]),
                (["unit_price","単価","税抜単価"], ["unit_price","単価","税抜単価"]),
                (["税込単価"], ["税込単価","price_incl"]),
            ])
            _set_first(neg, ["qty","数量"], -int(count))
            _set_first(neg, ["税率","tax_rate"], float(tax_rate if tax_rate is not None else 0.10))
            _set_first(neg, ["status","状態"], "取消")

            # 親リンク or メモに cancel_of を残す
            parent_set = False
            for name in ["parent_item_id","親明細ID","元明細ID"]:
                if hasattr(neg, name):
                    setattr(neg, name, item_id)
                    parent_set = True
                    break
            if not parent_set:
                memo_old = _get_any(neg, "memo","メモ","備考","備考欄", default="") or ""
                _set_first(neg, ["memo","メモ","備考","備考欄"], (memo_old + " ").strip() + f"cancel_of:{item_id}")

            now = datetime.utcnow()
            if hasattr(neg, "created_at"): neg.created_at = now
            if hasattr(neg, "updated_at"): neg.updated_at = now
            if hasattr(neg, "追加日時"):   setattr(neg, "追加日時", now)

            s.add(neg)
            s.flush()  # id 取得
            neg_id = getattr(neg, "id", None)

        # ---- 自動確定：提供済 + 取消 == 元数量 → 注文明細.status = 「提供済」
        finalized = progress_finalize_if_done(s, it)

        s.commit()
        mark_floor_changed()

        return jsonify({
            "ok": True,
            "progress": p_after,          # {"qty_new":..,"qty_cooking":..,"qty_served":..,"qty_canceled":..}
            "finalized": bool(finalized), # True のとき 注文明細.status は「提供済」に確定
            "negative_item_id": neg_id    # 取消のときだけ付与
        })

    except Exception as e:
        s.rollback()
        current_app.logger.exception("order_item status update failed")
        return jsonify({"ok": False, "error": "internal error"}), 500
    finally:
        s.close()






# ---------------------------------------------------------------------
# 画面ルート
# ---------------------------------------------------------------------
# --- テンプレ向けヘルパ注入（has_endpoint） ---------------------------------
@app.context_processor
def inject_helpers():
    # テンプレから has_endpoint('admin_console') のように呼べる
    def has_endpoint(name: str) -> bool:
        try:
            return name in current_app.view_functions
        except Exception:
            return False
    return dict(has_endpoint=has_endpoint)




# --- 店舗管理ポータル（タイル式ダッシュボード） ------------------------------
@app.route("/admin/console", endpoint="admin_console")
@require_admin  # あなたの既存デコレータ
def admin_console():
    sid = current_store_id()

    # ★ ここで現在店舗の「合流PIN必須」フラグを取得する
    s = SessionLocal()
    try:
        require_join_pin = 1  # デフォルトは「必須」
        if sid is not None:
            store = s.query(Store).get(sid)
            if store is not None and hasattr(store, "require_join_pin") and store.require_join_pin is not None:
                require_join_pin = store.require_join_pin
    finally:
        s.close()

    tiles = [
        {"title": "店舗情報",     "desc": "レシート・領収書用の店舗情報",     "endpoint": "admin_store_info",      "emoji": "🏪"},
        {"title": "メニュー管理", "desc": "新規作成と作成済み一覧", "endpoint": "admin_menu_home", "emoji": "🍽️"},
        {"title": "テーブル管理", "desc": "テーブル番号の登録や並び替え",     "endpoint": "admin_tables",          "emoji": "🪑"},
        {"title": "カテゴリ管理", "desc": "商品カテゴリの作成・並び替え",     "endpoint": "admin_categories",      "emoji": "🗂️"},
        {"title": "商品オプション", "desc": "割り方・トッピングなどの選択肢管理", "endpoint": "admin_product_options", "emoji": "🔧"},
        {"title": "プリンタ設定", "desc": "キッチン/ドリンカーなどの設定",    "endpoint": "admin_printers",        "emoji": "🖨️"},
        {"title": "支払方法",     "desc": "現金・クレカ・QRなどの管理",       "endpoint": "admin_payment_methods", "emoji": "💳"},
        {"title": "印刷ルール",   "desc": "注文伝票の振り分けルール",         "endpoint": "admin_rules",           "emoji": "🧾"},
        {"title": "メンバー追加", "desc": "管理者・従業員アカウントを追加",   "endpoint": "admin_member_new",      "emoji": "➕"},
    ]

    return render_template(
        "admin_console.html",
        title="店舗管理ページ",
        tiles=tiles,
        store_name=session.get("store_name"),
        sid=sid,
        # ★ テンプレートへ渡す
        require_join_pin=require_join_pin,
    )


# ---------------------------------------------------------------------
# 店舗情報編集ページ（レシート・領収書用）
# ---------------------------------------------------------------------
@app.route("/admin/store-info", methods=["GET", "POST"], endpoint="admin_store_info")
@require_admin
def admin_store_info():
    """店舗情報編集ページ（レシート・領収書用）"""
    sid = current_store_id()
    if sid is None:
        return redirect(url_for("admin_login"))
    
    s = SessionLocal()
    try:
        store = s.get(Store, sid)
        if not store:
            abort(404)
        
        if request.method == "POST":
            # フォームからデータを取得して保存
            store.address = request.form.get("住所", "").strip()
            store.phone = request.form.get("電話番号", "").strip()
            store.registration_number = request.form.get("登録番号", "").strip()
            store.business_hours = request.form.get("営業時間", "").strip()
            store.receipt_footer = request.form.get("レシートフッター", "").strip()
            
            s.commit()
            flash("店舗情報を保存しました。", "success")
            return redirect(url_for("admin_store_info"))
        
        # GETリクエスト：編集フォームを表示
        return render_template(
            "admin_store_info.html",
            store=store
        )
    finally:
        s.close()


# ★ 合流PINの ON/OFF を保存するルート ------------------------------
@app.route("/admin/console/join-pin", methods=["POST"], endpoint="admin_toggle_join_pin")
@require_admin
def admin_toggle_join_pin():
    """店舗ごとの『合流PIN必須』フラグを更新する"""
    sid = current_store_id()
    s = SessionLocal()
    try:
        store = s.query(Store).get(sid)
        if not store:
            abort(404)

        # チェックが付いていれば 1、外れていれば 0
        store.require_join_pin = 1 if request.form.get("require_join_pin") == "1" else 0
        s.commit()
        flash("合流PINの設定を保存しました。", "success")
    except Exception:
        s.rollback()
        current_app.logger.exception("failed to toggle join pin")
        flash("合流PINの設定保存に失敗しました。", "error")
    finally:
        s.close()

    return redirect(url_for("admin_console"))



# --- テーブル売上履歴 ------------------------------
@app.route("/admin/tables/sales")
@require_store_admin
def admin_table_sales():
    sid = current_store_id()
    if sid is None:
        return redirect(url_for("admin_login"))

    from datetime import datetime, timezone, timedelta
    from sqlalchemy import or_, not_
    from sqlalchemy.orm import sessionmaker, joinedload

    def _parse_date(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except Exception:
            return None

    # --- パラメータ ---
    table_id = request.args.get("table_id", type=int)
    from_s  = request.args.get("from")  # YYYY-MM-DD
    to_s    = request.args.get("to")    # YYYY-MM-DD
    dt_from = _parse_date(from_s) if from_s else None
    dt_to   = _parse_date(to_s) + timedelta(days=1) if to_s else None  # 終日含め +1日

    # 除外ステータス（統合済系）
    EXCLUDE_STATUSES = {
        "会計済(統合)", "統合済", "統合(会計済)",
        "closed(merged)", "merged", "merged_closed"
    }

    # _models() の戻り値差異を吸収
    try:
        TableSeat, OrderHeader, OrderItem, Menu = _models(
            "TableSeat", "OrderHeader", "OrderItem", "Menu"
        )
    except Exception:
        OrderItem, Menu = _models()
        TableSeat = globals().get("TableSeat")
        OrderHeader = globals().get("OrderHeader")
        if TableSeat is None or OrderHeader is None:
            raise RuntimeError(
                "_models() が4件返せず、TableSeat/OrderHeader も見つかりません。"
            )

    s = SessionLocal()
    try:
        # テーブル一覧
        qt = s.query(TableSeat)
        if hasattr(TableSeat, "store_id"):
            qt = qt.filter(TableSeat.store_id == sid)
        tables = qt.order_by(getattr(TableSeat, "table_no", TableSeat.id).asc()).all()

        # 売上ヘッダ
        qh = s.query(OrderHeader)
        if hasattr(OrderHeader, "store_id"):
            qh = qh.filter(OrderHeader.store_id == sid)
        if table_id:
            qh = qh.filter(OrderHeader.table_id == table_id)

        if hasattr(OrderHeader, "status"):
            qh = qh.filter(or_(OrderHeader.status.is_(None),
                               not_(OrderHeader.status.in_(list(EXCLUDE_STATUSES)))))

        # 期間
        if dt_from:
            conds = []
            for f in ("opened_at", "created_at", "開始日時", "作成日時"):
                if hasattr(OrderHeader, f):
                    conds.append(getattr(OrderHeader, f) >= dt_from)
            if conds:
                qh = qh.filter(or_(*conds))
        if dt_to:
            conds = []
            for f in ("opened_at", "created_at", "開始日時", "作成日時"):
                if hasattr(OrderHeader, f):
                    conds.append(getattr(OrderHeader, f) < dt_to)
            if conds:
                qh = qh.filter(or_(*conds))

        # 並び順
        order_key = (getattr(OrderHeader, "opened_at", None)
                     or getattr(OrderHeader, "created_at", None)
                     or getattr(OrderHeader, "id"))
        rows = qh.order_by(order_key.desc()).limit(1000).all()

        # ★★★ デバッグ：取得した伝票を出力 ★★★
        current_app.logger.info("[ADMIN SALES DEBUG] 取得した伝票数: %d", len(rows))
        for h in rows[:5]:  # 最初の5件だけ
            current_app.logger.info("[ADMIN SALES DEBUG] 伝票ID=%s status=%s", 
                                   getattr(h, "id", None), 
                                   getattr(h, "status", None))

        # 表示用 table_no を埋める
        table_map = {
            t.id: (getattr(t, "table_no", None)
                   or getattr(t, "テーブル番号", None)
                   or str(getattr(t, "id")))
            for t in tables
        }
        for h in rows:
            try:
                if not hasattr(h, "table_no"):
                    setattr(h, "table_no", table_map.get(getattr(h, "table_id", None)))
            except Exception:
                pass

        # 明細（items_map）
        order_ids = [getattr(h, "id") for h in rows] if rows else []
        items_map = {}
        
        # ★★★ デバッグ：order_ids を出力 ★★★
        current_app.logger.info("[ADMIN SALES DEBUG] order_ids: %s", order_ids[:10])
        
        if order_ids:
            # order_id カラム名を動的特定
            col_order = None
            for nm in ("order_id", "注文id", "注文ID"):
                if hasattr(OrderItem, nm):
                    col_order = getattr(OrderItem, nm)
                    break
            if col_order is None:
                current_app.logger.warning(
                    "[ADMIN SALES] OrderItem に order_id/注文id/注文ID が見つかりません"
                )
                col_order = getattr(OrderItem, "order_id")

            # ★★★ デバッグ：OrderItemモデルの情報を出力 ★★★
            current_app.logger.info("[ADMIN SALES DEBUG] OrderItem columns: %s", 
                                   [c.name for c in OrderItem.__table__.columns])

            # ★★★ 修正：グローバルフィルターを回避 ★★★
            # engine から直接 Session を作成してクエリを実行
            temp_session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)
            temp_s = temp_session_factory()
            try:
                # ★★★ デバッグ：生SQLで直接確認（カラム名を修正）★★★
                from sqlalchemy import text
                raw_count = temp_s.execute(
                    text(f'SELECT COUNT(*) FROM "T_注文明細" WHERE "注文ID" IN ({",".join(map(str, order_ids[:10]))})')
                ).scalar()
                current_app.logger.info("[ADMIN SALES DEBUG] 生SQL明細数: %d", raw_count)
                
                qi = temp_s.query(OrderItem).filter(col_order.in_(order_ids))

                # menu リレーションシップが存在する場合は Eager Loading
                if hasattr(OrderItem, "menu"):
                    qi = qi.options(joinedload(OrderItem.menu))

                if hasattr(OrderItem, "store_id"):
                    qi = qi.filter(OrderItem.store_id == sid)

                items = qi.all()
                
                # ★★★ デバッグ：取得した明細数を出力 ★★★
                current_app.logger.info("[ADMIN SALES DEBUG] 取得した明細数: %d", len(items))
                for it in items[:5]:  # 最初の5件だけ
                    current_app.logger.info("[ADMIN SALES DEBUG] 明細ID=%s order_id=%s qty=%s", 
                                           getattr(it, "id", None),
                                           getattr(it, "order_id", None),
                                           getattr(it, "qty", None))

                # ★★★ 元のセッションにマージして、リレーションシップアクセスを可能にする ★★★
                # まず、注文ごとにグループ化
                order_items_temp = {}
                for it in items:
                    try:
                        merged_item = s.merge(it)
                        oid = getattr(merged_item, "order_id", None)
                        if oid is None:
                            oid = getattr(merged_item, "注文id", None) or getattr(merged_item, "注文ID", None)
                        order_items_temp.setdefault(int(oid), []).append(merged_item)
                    except Exception as e:
                        current_app.logger.debug(
                            "[ADMIN SALES] item merge failed: %r (error: %s)", it, e
                        )
                
                # 各注文ごとに、メニューIDで集計してprogressを計算
                for oid, order_items in order_items_temp.items():
                    # メニューIDごとに集計
                    menu_stats = {}  # {menu_id: {qty_new, qty_cooking, qty_served, qty_canceled}}
                    
                    for it in order_items:
                        menu_id = getattr(it, "menu_id", None) or getattr(it, "メニューID", None)
                        if menu_id is None:
                            continue
                        
                        qty = int(getattr(it, "qty", 0) or 0)
                        status = getattr(it, "status", None) or getattr(it, "状態", None) or ""
                        status_lower = str(status).lower()
                        
                        if menu_id not in menu_stats:
                            menu_stats[menu_id] = {
                                "qty_new": 0,
                                "qty_cooking": 0,
                                "qty_served": 0,
                                "qty_canceled": 0
                            }
                        
                        # 数量がマイナスの場合は取消
                        if qty < 0:
                            # 取消商品自体はカウントしない（元の商品にカウントされる）
                            # 元の商品の取消数を増やす
                            menu_stats[menu_id]["qty_canceled"] += abs(qty)
                        # 取消判定（statusで判定）
                        elif ("取消" in status_lower) or ("ｷｬﾝｾﾙ" in status_lower) or ("cancel" in status_lower) or ("void" in status_lower):
                            menu_stats[menu_id]["qty_canceled"] += abs(qty)
                        # 提供済判定
                        elif ("提供済" in status_lower) or ("提供完了" in status_lower) or ("served" in status_lower) or ("done" in status_lower) or ("completed" in status_lower):
                            menu_stats[menu_id]["qty_served"] += abs(qty)
                        # 調理中判定
                        elif ("調理中" in status_lower) or ("cooking" in status_lower) or ("preparing" in status_lower):
                            menu_stats[menu_id]["qty_cooking"] += abs(qty)
                        # 新規判定
                        elif ("新規" in status_lower) or ("new" in status_lower) or ("pending" in status_lower):
                            menu_stats[menu_id]["qty_new"] += abs(qty)
                        else:
                            # デフォルトは新規
                            menu_stats[menu_id]["qty_new"] += abs(qty)
                    
                    # 各明細にprogressを設定
                    for it in order_items:
                        menu_id = getattr(it, "menu_id", None) or getattr(it, "メニューID", None)
                        if menu_id and menu_id in menu_stats:
                            it.progress = menu_stats[menu_id]
                        else:
                            it.progress = {
                                "qty_new": 0,
                                "qty_cooking": 0,
                                "qty_served": 0,
                                "qty_canceled": 0
                            }
                        
                        items_map.setdefault(oid, []).append(it)
            finally:
                temp_s.close()

        # ★★★ デバッグ：items_map の内容を出力 ★★★
        current_app.logger.info("[ADMIN SALES DEBUG] items_map keys: %s", list(items_map.keys())[:10])
        for oid, items in list(items_map.items())[:3]:
            current_app.logger.info("[ADMIN SALES DEBUG] order_id=%s 明細数=%d", oid, len(items))
        
        # ★★★ 各注文の合計金額を取消除外で再計算 ★★★
        for h in rows:
            order_id = getattr(h, "id", None)
            if order_id:
                try:
                    financials = _order_financials_excluding_cancels(s, order_id)
                    # 注文ヘッダーの金額を更新（表示用）
                    h.subtotal = financials["subtotal"]
                    h.tax = financials["tax"]
                    h.total = financials["total"]
                    # 小計と合計のカラム名のゆらぎに対応
                    if hasattr(h, "小計"):
                        setattr(h, "小計", financials["subtotal"])
                    if hasattr(h, "税額"):
                        setattr(h, "税額", financials["tax"])
                    if hasattr(h, "合計"):
                        setattr(h, "合計", financials["total"])
                except Exception as e:
                    current_app.logger.debug(
                        "[ADMIN SALES] Failed to recalculate financials for order %s: %s", order_id, e
                    )
        
        # 合計系
        total_orders = len(rows)
        total_amount = sum(int(getattr(h, "total", 0) or getattr(h, "合計", 0) or 0) for h in rows)
        total_subtotal = sum(int(getattr(h, "subtotal", 0) or getattr(h, "小計", 0) or 0) for h in rows)
        total_tax = sum(int(getattr(h, "tax", 0) or getattr(h, "税額", 0) or 0) for h in rows)

        return render_template(
            "admin_table_sales.html",
            title="テーブル売上履歴",
            tables=tables,
            rows=rows,
            items_map=items_map,
            current_table_id=table_id,
            from_date=from_s or "",
            to_date=to_s or "",
            total_orders=total_orders,
            total_amount=total_amount,
            total_subtotal=total_subtotal,
            total_tax=total_tax,
            store_name=session.get("store_name"),
            sid=sid,
            csrf_token=session.get("_csrf_token"),
        )
    finally:
        s.close()



# --- 返金（伝票単位） ------------------------------
@app.route("/admin/orders/<int:order_id>/refund", methods=["POST"])
@require_store_admin
def admin_order_refund(order_id):
    """
    返金を PaymentRecord に「マイナス金額」で記録する。
    - 金額: 正の数で受け取り、DBには負で保存
    - 支払方法: method_id が存在するモデルなら必ず method_id を決定して保存
    - デバッグ: app.config['DEBUG_REFUND']=True で詳細ログ
    """
    # ---- debug helpers (fallback; グローバル未定義でも動く) ----
    def __dbg_enabled(key: str) -> bool:
        try:
            from flask import current_app
            return bool(current_app.config.get(key, False))
        except Exception:
            return False

    def __dbg(ctx: str, **kv):
        if not __dbg_enabled('DEBUG_REFUND'):
            return
        try:
            import json
            from flask import current_app
            current_app.logger.debug(f"[{ctx}] " + json.dumps(kv, ensure_ascii=False, default=str))
        except Exception:
            pass

    data = request.get_json(silent=True) or request.form or {}
    if __dbg_enabled('DEBUG_REFUND'):
        __dbg("refund.input", order_id=order_id, data=dict(data))

    # 金額
    try:
        amount = float(data.get("amount") or 0)
    except Exception:
        return jsonify({"ok": False, "error": "invalid amount"}), 400
    if amount <= 0:
        return jsonify({"ok": False, "error": "amount must be > 0"}), 400

    # method（文字列コード: REFUND/CASH/CARD…）は“あれば優先”
    method_code = (data.get("method") or "").strip() or None
    reason = (data.get("reason") or "").strip()
    if not reason:
        return jsonify({"ok": False, "error": "reason required"}), 400

    s = SessionLocal()
    try:
        h = s.get(OrderHeader, order_id)
        if not h:
            return jsonify({"ok": False, "error": "order not found"}), 404

        sid = current_store_id()
        if hasattr(OrderHeader, "store_id") and sid is not None:
            if getattr(h, "store_id", None) != sid:
                return jsonify({"ok": False, "error": "forbidden"}), 403

        Pay = globals().get("PaymentRecord") or globals().get("T_支払")
        if Pay is None:
            return jsonify({"ok": False, "error": "Payment model not found"}), 500

        need_method_id = hasattr(Pay, "method_id")
        method_id = _call_pick_refund_method_id(s, sid, order_id, method_code) if need_method_id else None

        if __dbg_enabled('DEBUG_REFUND'):
            __dbg("refund.pick_method",
                  need_method_id=need_method_id, picked_method_id=method_id,
                  method_code=method_code, store_id=sid)

        if need_method_id and not method_id:
            return jsonify({
                "ok": False,
                "error": "no active payment method for refund",
                "hint": "支払方法マスタに REFUND か CASH を登録/有効化してください。"
            }), 422

        # --- レコード作成 ---
        from datetime import datetime, timezone
        pr = Pay()
        if hasattr(Pay, "store_id") and sid is not None:
            setattr(pr, "store_id", sid)
        setattr(pr, "order_id", order_id)

        # 金額（DBにはマイナスで保存）
        saved_amount = False
        for a in ("amount", "金額"):
            if hasattr(Pay, a):
                setattr(pr, a, -int(round(amount)))
                saved_amount = True
                break
        if not saved_amount:
            return jsonify({"ok": False, "error": "payment amount column not found"}), 500

        # method_id（FK）
        if need_method_id and method_id:
            setattr(pr, "method_id", method_id)

        # 文字列の method/type/支払方法 があれば補助的に入れる（任意）
        for a in ("method", "支払方法", "type", "種別"):
            if hasattr(Pay, a) and method_code:
                setattr(pr, a, method_code)
                break

        # メモ
        for a in ("memo", "メモ", "備考", "reason"):
            if hasattr(Pay, a):
                setattr(pr, a, f"refund: {reason}")
                break

        # 支払日時
        for a in ("created_at", "支払日時", "paid_at"):
            if hasattr(Pay, a):
                setattr(pr, a, datetime.now(timezone.utc))
                break

        # コミット前ダンプ
        if __dbg_enabled('DEBUG_REFUND'):
            dump = {}
            for f in ("id","order_id","store_id","tenant_id","method_id","amount","金額","method","支払方法","type","種別","memo","メモ","備考","reason","created_at","支払日時","paid_at"):
                if hasattr(pr, f):
                    dump[f] = getattr(pr, f)
            __dbg("refund.before_commit", record=dump)

        s.add(pr)
        s.commit()
        mark_floor_changed()
        if __dbg_enabled('DEBUG_REFUND'):
            __dbg("refund.ok", order_id=order_id)
        return jsonify({"ok": True})

    except Exception as e:
        s.rollback()
        current_app.logger.exception("[admin_order_refund] %s", e)
        if __dbg_enabled('DEBUG_REFUND'):
            return jsonify({"ok": False, "error": "internal error", "detail": str(e)}), 500
        return jsonify({"ok": False, "error": "internal error"}), 500
    finally:
        s.close()





# --- 明細の取消（部分数量対応）＋自動返金 ---------------------------------------------
@app.route("/admin/orders/<int:order_id>/items/<int:item_id>/cancel", methods=["POST"])
@require_store_admin
def admin_order_item_cancel(order_id, item_id):
    """
    指定明細の一部/全部を取消する（元明細は変更しない）。
    - 取消分は「負数量 & status=取消」の監査用明細を追加
    - ★ 会計済の場合のみ返金記録を作成（会計前は返金不要）
    - ヘッダ金額は『負数量も合算』でネット再計算（DBから読み直して堅牢）
    - 進捗カウンタ（T_明細進捗）を更新
    - デバッグ: app.config['DEBUG_CANCEL']=True / app.config['DEBUG_TOTALS']=True
    """
    # ---- debug helpers (fallback) ----
    def __dbg_enabled(key: str) -> bool:
        try:
            return bool(current_app.config.get(key, False))
        except Exception:
            return False

    def __dbg(ctx: str, **kv):
        if not __dbg_enabled('DEBUG_CANCEL'):
            return
        try:
            import json
            current_app.logger.debug(f"[{ctx}] " + json.dumps(kv, ensure_ascii=False, default=str))
        except Exception:
            try:
                current_app.logger.debug(f"[{ctx}] {kv!r}")
            except Exception:
                pass

    data = request.get_json(force=True) or request.form or {}
    qty_req = int((data.get("qty") or 0))
    reason  = (data.get("reason") or "").strip()
    if qty_req <= 0:
        return jsonify({"ok": False, "error": "qty must be > 0"}), 400
    if not reason:
        return jsonify({"ok": False, "error": "reason required"}), 400

    s = SessionLocal()
    try:
        it = s.get(OrderItem, item_id)
        if not it or getattr(it, "order_id", None) != order_id:
            return jsonify({"ok": False, "error": "item not found"}), 404

        sid = current_store_id()
        if hasattr(OrderItem, "store_id") and sid is not None:
            if getattr(it, "store_id", None) != sid:
                return jsonify({"ok": False, "error": "forbidden"}), 403

        # 現在数量（※ 元明細は変更しない方針）
        current_qty = int(getattr(it, "qty", None) or getattr(it, "数量", None) or 0)
        if qty_req > current_qty:
            return jsonify({"ok": False, "error": "qty exceeds current qty"}), 400

        # ★★★ 伝票の状態を確認（会計済かどうか）★★★
        order_header = it.order if hasattr(it, "order") else s.get(OrderHeader, order_id)
        order_status = getattr(order_header, "status", None) if order_header else None
        is_paid = (order_status in ("会計済", "closed", "paid", "settled"))
        
        __dbg("cancel.order_status", 
              order_id=order_id, 
              status=order_status, 
              is_paid=is_paid)

        # 取消金額（内税）を算出
        from math import floor
        unit_excl = int(getattr(it, "unit_price", None) or getattr(it, "税抜単価", None) or 0)
        rate = float(getattr(it, "tax_rate", None) or 0.10)
        unit_tax  = floor(unit_excl * rate)
        unit_incl = int(unit_excl + unit_tax)
        refund_amount = unit_incl * qty_req

        __dbg("cancel.input",
              order_id=order_id,
              item_id=getattr(it, "id", None),
              current_qty=current_qty,
              qty_req=qty_req,
              unit_excl=unit_excl,
              tax_rate=rate,
              unit_incl=unit_incl,
              refund_amount=refund_amount,
              store_id=sid)

        # ★ 元明細は触らず、取消分だけ負数量の監査明細を追加
        neg = OrderItem()
        if hasattr(OrderItem, "store_id") and sid is not None:
            setattr(neg, "store_id", sid)
        setattr(neg, "order_id", order_id)
        if hasattr(neg, "menu_id") and hasattr(it, "menu_id"):
            neg.menu_id = getattr(it, "menu_id")
        if hasattr(neg, "qty"):
            neg.qty = -qty_req
        elif hasattr(neg, "数量"):
            setattr(neg, "数量", -qty_req)
        if hasattr(neg, "unit_price"):
            neg.unit_price = unit_excl
        if hasattr(neg, "tax_rate"):
            neg.tax_rate = rate
        if hasattr(neg, "status"):
            neg.status = "取消"
        if hasattr(neg, "memo"):
            neg.memo = f"取消({qty_req}) reason={reason}"
        for a in ("added_at", "created_at", "作成日時"):
            if hasattr(neg, a):
                from datetime import datetime, timezone
                setattr(neg, a, datetime.now(timezone.utc))
                break
        s.add(neg)
        s.flush()  # neg の ID を採番

        # ★★★ 進捗カウンタを更新 ★★★
        try:
            # 元の明細の進捗を「取消」に移動
            progress_seed_if_needed(s, it)
            progress_move(s, it, "cancel", qty_req)
            
            # 取消行の進捗を初期化（qty_canceled に設定）
            progress_seed_if_needed(s, neg)
            
            __dbg("cancel.progress_updated", 
                  original_item_id=getattr(it, "id", None),
                  cancel_item_id=getattr(neg, "id", None),
                  qty_canceled=qty_req)
        except Exception as e:
            current_app.logger.warning("[cancel.progress] failed to update progress: %s", e)
            # 進捗更新失敗は致命的ではないので続行

        # ★★★ 返金記録は会計済の場合のみ作成 ★★★
        Pay = globals().get("PaymentRecord") or globals().get("T_支払")
        if Pay is not None and refund_amount > 0 and is_paid:
            __dbg("cancel.refund", reason="order is paid, creating refund record")
            
            pr = Pay()
            if hasattr(Pay, "store_id") and sid is not None:
                setattr(pr, "store_id", sid)
            setattr(pr, "order_id", order_id)

            need_method_id = hasattr(Pay, "method_id")
            pick = globals().get("_call_pick_refund_method_id")
            method_id = pick(s, sid, order_id, None) if (need_method_id and callable(pick)) else None
            __dbg("cancel.pick_method", need_method_id=need_method_id, picked_method_id=method_id)

            # 金額（マイナス）
            wrote_amount = False
            for a in ("amount", "金額"):
                if hasattr(Pay, a):
                    setattr(pr, a, -int(refund_amount))
                    wrote_amount = True
                    break
            if not wrote_amount:
                raise RuntimeError("payment amount column not found in PaymentRecord")

            # method_id 必須だが選べない場合は支払記録をスキップ（取消自体は成功）
            if need_method_id and not method_id:
                __dbg("cancel.skip_payment", reason="no payment method")
            else:
                if need_method_id and method_id:
                    setattr(pr, "method_id", method_id)
                for a in ("method", "支払方法", "type", "種別"):
                    if hasattr(Pay, a):
                        setattr(pr, a, "refund_item")
                        break
                for a in ("memo", "メモ", "備考", "reason"):
                    if hasattr(Pay, a):
                        setattr(pr, a, f"item_cancel: {reason}")
                        break
                from datetime import datetime, timezone
                for a in ("created_at", "支払日時", "paid_at"):
                    if hasattr(Pay, a):
                        setattr(pr, a, datetime.now(timezone.utc))
                        break

                dump = {}
                for f in ("id","order_id","store_id","tenant_id","method_id","amount","金額","method","支払方法","type","種別","memo","メモ","備考","reason","created_at","支払日時","paid_at"):
                    if hasattr(pr, f):
                        dump[f] = getattr(pr, f)
                __dbg("cancel.before_commit_payment", record=dump)

                s.add(pr)
        elif not is_paid:
            __dbg("cancel.no_refund", reason="order is not paid yet")

        # ▼▼ 再集計（コミット直前）＋デバッグ ▼▼
        try:
            s.flush()  # ID採番・関連の確定
            if bool(current_app.config.get("DEBUG_TOTALS", False)):
                hdr = it.order
                current_app.logger.debug(
                    "[cancel.recalc.before] order_id=%s subtotal=%s tax=%s total=%s",
                    order_id,
                    getattr(hdr, "subtotal", getattr(hdr, "小計", None)) if hdr else None,
                    getattr(hdr, "tax", getattr(hdr, "税額", None)) if hdr else None,
                    getattr(hdr, "total", getattr(hdr, "合計", None)) if hdr else None,
                )
            _recalc_order_totals_with_negatives_db(s, order_id, it.order)
            if bool(current_app.config.get("DEBUG_TOTALS", False)):
                hdr = it.order
                current_app.logger.debug(
                    "[cancel.recalc.after] order_id=%s subtotal=%s tax=%s total=%s",
                    order_id,
                    getattr(hdr, "subtotal", getattr(hdr, "小計", None)) if hdr else None,
                    getattr(hdr, "tax", getattr(hdr, "税額", None)) if hdr else None,
                    getattr(hdr, "total", getattr(hdr, "合計", None)) if hdr else None,
                )
        except Exception:
            current_app.logger.exception("[cancel.recalc] failed but continue")
        # ▲▲ ここまで ▲▲

        s.commit()
        mark_floor_changed()
        __dbg("cancel.ok", order_id=order_id, item_id=item_id)
        return jsonify({"ok": True})

    except Exception as e:
        s.rollback()
        app.logger.exception("[admin_order_item_cancel] %s", e)
        if current_app.config.get("DEBUG_CANCEL"):
            return jsonify({"ok": False, "error": "internal error", "detail": str(e)}), 500
        return jsonify({"ok": False, "error": "internal error"}), 500
    finally:
        s.close()



# --- ルート：ロール別トップへ誘導 ---------------------------------------------
@app.route("/")
def index():
    if not session.get("logged_in"):
        return redirect(url_for("login_choice"))
    r = session.get("role")
    if r == "sysadmin":
        # ▼ 修正: システム管理者も店舗管理ページへ誘導（必要なら dev_tools に戻してOK）
        return redirect(url_for("admin_console"))
    return redirect(url_for("floor") if is_store_admin_or_higher() else url_for("staff_floor"))


# --- フロア画面（テーブル状況＋オーダー要約） --------------------------------
@app.route("/floor")
@require_admin
def floor():
    """
    フロア画面：
      - 取消（負数量）を含め、毎回 注文明細 から小計/税/合計を再計算
      - 正数量で「状態＝取消/キャンセル」は除外（スタッフ画面と同一ロジック）
      - ヘッダの合計値は使わない
    """
    from sqlalchemy import func, and_
    from datetime import datetime
    import math

    debug_on = request.args.get("debug") in ("1", "true", "yes")

    sid = current_store_id()
    if sid is None:
        return redirect(url_for("admin_login"))

    def dbg(msg):
        try:
            app.logger.info(f"[FLOOR-DEBUG] {msg}")
        except Exception:
            pass

    dbg(f"sid={sid} tenant={session.get('tenant_slug')} session.store_id={session.get('store_id')}")

    ACTIVE_ORDER_STATUSES = {
        "open", "pending", "in_progress", "serving", "unpaid",
        "新規", "調理中", "提供済", "会計中"
    }

    s = SessionLocal()
    try:
        # ---- テーブル一覧（現店舗）----
        tables = (
            s.query(TableSeat)
             .filter(TableSeat.store_id == sid)
             .order_by(getattr(TableSeat, "table_no", TableSeat.id).asc())
             .all()
        )
        dbg(f"tables={len(tables)}")

        # ---- 各テーブルの最新オーダーヘッダ ----
        has_opened = hasattr(OrderHeader, "opened_at")
        if has_opened:
            sub = (
                s.query(OrderHeader.table_id, func.max(OrderHeader.opened_at).label("mx"))
                 .join(TableSeat, TableSeat.id == OrderHeader.table_id)
                 .filter(OrderHeader.store_id == sid, TableSeat.store_id == sid)
                 .group_by(OrderHeader.table_id)
            ).subquery()
            latest_headers = (
                s.query(OrderHeader)
                 .join(TableSeat, TableSeat.id == OrderHeader.table_id)
                 .join(sub, and_(OrderHeader.table_id == sub.c.table_id,
                                 OrderHeader.opened_at == sub.c.mx))
                 .filter(OrderHeader.store_id == sid, TableSeat.store_id == sid)
                 .all()
            )
        else:
            sub = (
                s.query(OrderHeader.table_id, func.max(OrderHeader.id).label("mx"))
                 .join(TableSeat, TableSeat.id == OrderHeader.table_id)
                 .filter(OrderHeader.store_id == sid, TableSeat.store_id == sid)
                 .group_by(OrderHeader.table_id)
            ).subquery()
            latest_headers = (
                s.query(OrderHeader)
                 .join(TableSeat, TableSeat.id == OrderHeader.table_id)
                 .join(sub, and_(OrderHeader.table_id == sub.c.table_id,
                                 OrderHeader.id == sub.c.mx))
                 .filter(OrderHeader.store_id == sid, TableSeat.store_id == sid)
                 .all()
            )
        dbg(f"latest_headers={len(latest_headers)}")

        header_by_table = {h.table_id: h for h in latest_headers}
        header_ids = [h.id for h in latest_headers]

        # ---- 明細から“毎回”再計算（store_id で絞らない）----
        recalc_totals = {}
        if header_ids:
            items = (
                s.query(OrderItem)
                 .filter(OrderItem.order_id.in_(header_ids))
                 .all()
            )
            buf = {}  # oid -> {"subtotal": int, "tax": int}
            for d in items or []:
                oid = int(getattr(d, "order_id"))
                q   = int(getattr(d, "qty", None) or getattr(d, "数量", None) or 0)
                if q == 0:
                    continue

                # スタッフ画面と同等の取消ラベル判定
                st_raw = (getattr(d, "status", None) or getattr(d, "状態", None) or "")
                st_low = str(st_raw).lower()
                is_cancel_label = (
                    ("取消" in st_low) or ("ｷｬﾝｾﾙ" in st_low) or ("キャンセル" in st_low)
                    or ("cancel" in st_low) or ("void" in st_low)
                )
                # 正数量かつ取消ラベルの行は除外（-1 の監査行はそのままネット減算）
                if q > 0 and is_cancel_label:
                    continue

                # 時価商品の場合、actual_price（実際価格）を優先する
                actual_price = getattr(d, "actual_price", None)
                if actual_price is not None:
                    unit_excl = int(actual_price)
                else:
                    unit_excl = int(getattr(d, "unit_price", None) or getattr(d, "税抜単価", None) or 0)
                rate      = float(getattr(d, "tax_rate", None) or 0.10)
                unit_tax  = int(math.floor(unit_excl * rate))

                if oid not in buf:
                    buf[oid] = {"subtotal": 0, "tax": 0}
                buf[oid]["subtotal"] += unit_excl * q
                buf[oid]["tax"]      += unit_tax  * q

            for oid, dct in buf.items():
                sub = int(dct["subtotal"])
                tax = int(dct["tax"])
                recalc_totals[oid] = {"subtotal": sub, "tax": tax, "total": sub + tax}

        dbg(f"recalc_totals(items)={len(recalc_totals)}")

        # ---- 支払合計 ----
        paid_map = {}
        if header_ids and "PaymentRecord" in globals():
            qpaid = (
                s.query(PaymentRecord.order_id, func.coalesce(func.sum(PaymentRecord.amount), 0))
                 .filter(PaymentRecord.order_id.in_(header_ids))
            )
            if hasattr(PaymentRecord, "store_id"):
                qpaid = qpaid.filter(PaymentRecord.store_id == sid)
            for oid, paid in qpaid.group_by(PaymentRecord.order_id).all():
                paid_map[int(oid)] = int(paid or 0)
        dbg(f"paid_map={len(paid_map)}")

        # ---- 最新QR ----
        qr_last_by_table = {}
        if "QrToken" in globals():
            base = s.query(QrToken)
            if hasattr(QrToken, "store_id"):
                base = base.filter(QrToken.store_id == sid)
            if hasattr(QrToken, "revoked"):
                base = base.filter(QrToken.revoked == 0)
            if hasattr(QrToken, "expires_at"):
                base = base.filter((QrToken.expires_at == None) | (QrToken.expires_at > datetime.utcnow()))

            if hasattr(QrToken, "created_at"):
                sub_qr = base.with_entities(QrToken.table_id, func.max(QrToken.created_at).label("mx")).group_by(QrToken.table_id).subquery()
                latest_qr = s.query(QrToken).join(sub_qr, and_(QrToken.table_id == sub_qr.c.table_id, QrToken.created_at == sub_qr.c.mx))
                if hasattr(QrToken, "store_id"):
                    latest_qr = latest_qr.filter(QrToken.store_id == sid)
                latest_qr = latest_qr.all()
            else:
                sub_qr = base.with_entities(QrToken.table_id, func.max(QrToken.id).label("mx")).group_by(QrToken.table_id).subquery()
                latest_qr = s.query(QrToken).join(sub_qr, and_(QrToken.table_id == sub_qr.c.table_id, QrToken.id == sub_qr.c.mx))
                if hasattr(QrToken, "store_id"):
                    latest_qr = latest_qr.filter(QrToken.store_id == sid)
                latest_qr = latest_qr.all()

            for q in latest_qr:
                qr_last_by_table[q.table_id] = getattr(q, "token", None) or getattr(q, "qr_token", None) or ""

        # ---- テンプレ用データ ----
        out = []
        for t in tables:
            tdict = {
                "id": t.id,
                "テーブル番号": getattr(t, "table_no", None) or t.id,
                "状態": getattr(t, "status", "") or "空席",
                "order": None,
                "last_qr": qr_last_by_table.get(t.id),
            }
            h = header_by_table.get(t.id)
            if h:
                tcalc = recalc_totals.get(h.id, {"subtotal": 0, "tax": 0, "total": 0})
                subtotal = int(tcalc["subtotal"])
                tax      = int(tcalc["tax"])
                total    = int(tcalc["total"])

                paid = int(paid_map.get(h.id, 0))
                remaining = max(0, total - paid)

                is_active = (not hasattr(OrderHeader, "status")) or (getattr(h, "status", None) in ACTIVE_ORDER_STATUSES)
                if is_active:
                    tdict["order"] = {
                        "id": h.id,
                        "状態": getattr(h, "status", "") or "",
                        "小計": subtotal,
                        "税額": tax,
                        "total": total,     # ← テンプレで total を優先させるため key も用意
                        "合計": total,      # ← 互換のため両方入れておく
                        "既払": paid,
                        "残額": remaining,
                    }
                else:
                    tdict["order"] = None
                    if not tdict["状態"]:
                        tdict["状態"] = "空席"

            out.append(tdict)

        current_tenant_slug = session.get("tenant_slug")
        debug_banner = f"sid={sid}" if debug_on else ""

        return render_template(
            "floor.html",
            tables=out,
            current_tenant_slug=current_tenant_slug,
            csrf_token=session.get("csrf_token"),
            debug_info=debug_banner,
            title="フロア",
        )
    finally:
        s.close()




# --- フロア変更検知（ポーリングAPI：バージョン比較） ---------------------------
@app.get("/admin/floor/changed")
def floor_changed():
    """
    クライアントから ?since=（前回受け取った version, int）を受け取り、
    サーバ側の _floor_version が新しければ changed=True を返す。
    """
    global _floor_version
    try:
        since = int(request.args.get("since", "0"))
    except Exception:
        since = 0
    changed = (_floor_version > since)
    return jsonify(changed=changed, version=_floor_version)


# --- フロアイベント（SSE: Server-Sent Events 配信） ---------------------------
@app.get("/events/floor")
def events_floor():
    q = queue.Queue(maxsize=8)
    with _floor_lock:
        _floor_waiters.append(q)
        ver = _floor_version  # 接続直後に一度流す

    def gen():
        try:
            # 接続直後に最新の版数 or "changed" を一度通知
            yield f"data: {ver}\n\n"
            # 以降、変更があれば "changed" を通知
            # Herokuのタイムアウト対策: 30秒ごとにハートビートを送信
            while True:
                try:
                    _ = q.get(timeout=30)  # 30秒でタイムアウト
                    yield "data: changed\n\n"
                except queue.Empty:
                    # タイムアウト時はハートビート（コメント）を送信
                    yield ": heartbeat\n\n"
        finally:
            with _floor_lock:
                if q in _floor_waiters:
                    _floor_waiters.remove(q)

    # 重要: text/event-stream を返す
    return Response(stream_with_context(gen()), mimetype="text/event-stream")



# --- QR印刷（サーバ側でPNG生成→data URL埋め込み） ----------------------------
@app.route("/qr/print/<int:table_id>")
@require_any
def qr_print(table_id: int):
    sid = current_store_id()
    if sid is None:
        return redirect(url_for("admin_login"))

    s = SessionLocal()
    try:
        # テーブルの存在チェック（店舗縛り）
        q_table = s.query(TableSeat).filter(TableSeat.id == table_id)
        if hasattr(TableSeat, "store_id"):
            q_table = q_table.filter(TableSeat.store_id == sid)
        table = q_table.first()
        if not table:
            abort(404)

        # 有効な最新トークン
        base = s.query(QrToken).filter(QrToken.table_id == table.id)
        if hasattr(QrToken, "store_id"):
            base = base.filter(QrToken.store_id == sid)
        if hasattr(QrToken, "revoked"):
            base = base.filter(QrToken.revoked == 0)
        if hasattr(QrToken, "expires_at"):
            base = base.filter((QrToken.expires_at == None) | (QrToken.expires_at > datetime.utcnow()))
        q = base.order_by(getattr(QrToken, "created_at", QrToken.id).desc()).first()

        if not q:
            return render_template(
                "qr_print.html",
                error="このテーブルの有効なQRがありません。先に「QR発行」を押してください。",
                table_no=getattr(table, "table_no", table.id),
                title="QR印刷"
            )

        token = getattr(q, "token", None) or getattr(q, "qr_token", None) or ""
        tenant_slug = session.get("tenant_slug")
        menu_url = url_for("menu_page", tenant_slug=tenant_slug, token=token, _external=True)

        # ★ サーバ側でQR(PNG)生成 → data URL でテンプレに渡す
        qr_data_url = None
        try:
            import qrcode
            from qrcode.constants import ERROR_CORRECT_M
            img = qrcode.make(menu_url, error_correction=ERROR_CORRECT_M, box_size=12, border=2)
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            qr_data_url = "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")
        except Exception as e:
            # ライブラリ未導入など。ログだけ残してテンプレ側でURLを表示
            app.logger.exception("[QR] build failed")

        return render_template(
            "qr_print.html",
            table_no=getattr(table, "table_no", table.id),
            menu_url=menu_url,
            qr_data_url=qr_data_url,  # ← これをテンプレで <img src=...> に使う
            title=f"QR印刷: テーブル {getattr(table, 'table_no', table.id)}"
        )
    finally:
        s.close()


# --- テーブル詳細（最新オーダー＋明細・合計の可視化） --------------------------
@app.route("/floor/table/<int:table_id>")
def table_detail(table_id):
    # QRトークンまたはセッションから店舗IDを取得
    token = request.args.get("token")
    is_qr_user = False
    sid = None
    
    # スタッフ権限をチェック
    is_staff = is_admin_or_staff()
    
    if token:
        # QRトークンから店舗IDを取得
        s_temp = SessionLocal()
        try:
            qt = s_temp.query(QrToken).filter_by(token=token).first()
            if qt and qt.store_id:
                sid = qt.store_id
                # スタッフとしてログインしている場合はQRユーザーではない
                is_qr_user = not is_staff
        finally:
            s_temp.close()
    
    if sid is None:
        # セッションから店舗IDを取得（従業員ログイン）
        sid = current_store_id()
        if sid is None:
            return redirect(url_for("admin_login"))

    s = SessionLocal()
    try:
        # --- テーブル本体（店舗スコープ） ---
        q_table = s.query(TableSeat).filter(TableSeat.id == table_id)
        if hasattr(TableSeat, "store_id"):
            q_table = q_table.filter(TableSeat.store_id == sid)
        table = q_table.first()
        if table is None:
            abort(404)

        # --- オーダーヘッダ（店舗スコープ + 状態フィルタ）---
        q_hdr = s.query(OrderHeader).filter(OrderHeader.table_id == table.id)
        if hasattr(OrderHeader, "store_id"):
            q_hdr = q_hdr.filter(OrderHeader.store_id == sid)
        if hasattr(OrderHeader, "status"):
            q_hdr = q_hdr.filter(OrderHeader.status.in_([
                "open", "pending", "in_progress", "serving", "unpaid",
                "新規", "調理中", "提供済", "会計中"
            ]))

        headers = (
            q_hdr.order_by(getattr(OrderHeader, "opened_at", OrderHeader.id).desc())
                 .limit(50)
                 .all()
        )

        # --- 明細取得（店舗スコープ）---
        items_map_raw = {}
        if headers:
            order_ids = [h.id for h in headers]
            qi = s.query(OrderItem).options(joinedload(OrderItem.menu)).filter(OrderItem.order_id.in_(order_ids))
            if hasattr(OrderItem, "store_id"):
                qi = qi.filter(OrderItem.store_id == sid)
            for it in qi.all():
                items_map_raw.setdefault(it.order_id, []).append(it)

        def _first(*vals):
            for v in vals:
                if v not in (None, ""):
                    return v

        def _to_int(x, default=0):
            try:
                return int(x)
            except Exception:
                return default

        def _to_rate(val, default=0.10):
            try:
                r = float(val)
                if r > 1:
                    r = r / 100.0
                return max(0.0, min(1.0, r))
            except Exception:
                return default

        def _price_incl(excl, rate):
            try:
                return display_price_incl_from_excl(excl, rate)
            except Exception:
                from math import floor
                return int(excl + floor(excl * rate))

        def _status_label(it) -> str:
            for nm in ("is_cancel", "is_cancelled", "cancelled"):
                if getattr(it, nm, None):
                    return "取消"
            if any([
                getattr(it, "served", None),
                getattr(it, "is_served", None),
                getattr(it, "served_at", None),
                getattr(it, "provided", None),
                getattr(it, "is_provided", None),
            ]):
                return "提供済"
            if any([getattr(it, "cooking", None), getattr(it, "is_cooking", None)]):
                return "調理中"
            cook_code = getattr(it, "cook_status", None)
            if isinstance(cook_code, int) and cook_code == 1:
                return "調理中"
            if isinstance(cook_code, str) and cook_code.lower() in {"1", "cooking", "調理中"}:
                return "調理中"

            raw = _first(
                getattr(it, "status", None),
                getattr(it, "item_status", None),
                getattr(it, "state", None),
                getattr(it, "serve_status", None),
                getattr(it, "progress", None),
            )
            if raw in (None, ""):
                return "新規"
            try:
                n = int(raw)
                return {0: "新規", 1: "調理中", 2: "提供済", 3: "取消"}.get(n, "新規")
            except Exception:
                pass
            sraw = str(raw).lower()
            if ("取消" in sraw) or ("ｷｬﾝｾﾙ" in sraw) or ("cancel" in sraw) or ("void" in sraw):
                return "取消"
            if ("提供済" in sraw) or ("提供完了" in sraw) or ("served" in sraw) or ("done" in sraw):
                return "提供済"
            if ("調理" in sraw) or ("cooking" in sraw) or ("in_progress" in sraw):
                return "調理中"
            if ("新規" in sraw) or ("open" in sraw) or ("pending" in sraw):
                return "新規"
            return "新規"

        def _item_to_dict(it):
            qty = _to_int(_first(getattr(it, "qty", None), getattr(it, "数量", None)), 1)
            # 時価商品の場合、actual_price（実際価格）を優先する
            actual_price = getattr(it, "actual_price", None)
            if actual_price is not None:
                unit_excl = _to_int(actual_price, 0)
            else:
                unit_excl = _to_int(_first(
                    getattr(it, "unit_price", None),
                    getattr(it, "税抜単価", None),
                    getattr(it, "price_excl", None),
                    getattr(it, "price", None),
                ), 0)
            
            # menuリレーションへのアクセスをtry-exceptで保護
            menu_tax_rate = None
            menu_name = None
            menu_photo_url = None
            try:
                menu_obj = getattr(it, "menu", None)
                if menu_obj:
                    menu_tax_rate = getattr(menu_obj, "tax_rate", None)
                    menu_name = getattr(menu_obj, "name", None)
                    menu_photo_url = getattr(menu_obj, "photo_url", None)
            except Exception as e:
                # トランザクションエラーの場合はロールバックして続行
                try:
                    s.rollback()
                except:
                    pass
                app.logger.warning(f"[_item_to_dict] menu access failed: {e}")
            
            rate = _to_rate(_first(
                getattr(it, "tax_rate", None),
                menu_tax_rate,
                0.10
            ), 0.10)
            name = _first(
                getattr(it, "name", None),
                getattr(it, "名称", None),
                menu_name,
                f"Item#{getattr(it, 'id', '')}"
            )
            photo_url = _first(
                getattr(it, "photo_url", None),
                menu_photo_url,
            )
            incl = _price_incl(unit_excl, rate)
            
            item_id = getattr(it, "id", None)
            progress = None
            if item_id and abs(qty) >= 1:
                try:
                    progress = progress_get(s, item_id)
                    app.logger.debug(f"[_item_to_dict] item_id={item_id} progress={progress}")
                except Exception as e:
                    app.logger.warning(f"[_item_to_dict] progress_get failed for item_id={item_id}: {e}")
                    try:
                        s.rollback()
                    except:
                        pass
                    progress = None
            
            # メモの取得
            memo = _first(
                getattr(it, "memo", None),
                getattr(it, "メモ", None),
                getattr(it, "note", None),
                ""
            )
            
            # 時価商品かどうかを判定：menu.is_market_priceを優先、なければunit_priceが0を基準にする
            original_unit_price = _to_int(_first(
                getattr(it, "unit_price", None),
                getattr(it, "税抜单価", None),
                getattr(it, "price_excl", None),
                getattr(it, "price", None),
            ), 0)
            # menuオブジェクトからis_market_priceを取得
            menu_is_market_price = False
            try:
                menu_obj = getattr(it, "menu", None)
                if menu_obj:
                    menu_is_market_price = bool(getattr(menu_obj, "is_market_price", 0))
            except Exception:
                pass
            # 時価商品の判定：menu.is_market_price=True または original_unit_price=0
            is_market_price = menu_is_market_price or (original_unit_price == 0)
            
            result = {
                "id": item_id,
                "種類": "item",
                "名称": name,
                "数量": qty,
                "税抜単価": unit_excl,
                "税込単価": incl,
                "税込小計": int(incl * qty),
                "税率": rate,
                "状態": _status_label(it),
                "メモ": memo,
                "写真URL": photo_url,
                "is_market_price": is_market_price,
                "_raw": it,
            }
            
            if progress:
                result["progress"] = progress
            
            return result

        items_map = {oid: [_item_to_dict(it) for it in lst]
                     for oid, lst in items_map_raw.items()}

        # 明細から合計金額を再計算（取消明細の負の数量を考慮）
        # _item_to_dict で "税込小計" = incl * qty が計算されているため、
        # qty が負の取消明細は "税込小計" が負になり、単純合計でネット金額になるはず。
        order_totals = {
            oid: sum(int(d.get("税込小計", 0)) for d in lst)
            for oid, lst in items_map.items()
        }

        if bool(current_app.config.get("DEBUG_TOTALS", False)) and headers:
            rid = headers[0].id
            app.logger.debug("[table_detail.totals] order_id=%s total=%s items=%s",
                             rid, order_totals.get(rid, 0),
                             [{"id": d["id"], "qty": d["数量"], "sub": d["税込小計"], "st": d["状態"]} for d in items_map.get(rid, [])][:6])

        context = dict(
            table=table,
            orders=headers,
            order_list=headers,
            items_map=items_map,
            order_totals=order_totals,
            csrf_token=session.get("csrf_token"),
            title=f"テーブル {getattr(table, 'table_no', table_id)}",
            is_qr_user=is_qr_user,  # QRユーザーフラグを追加
            qr_token=token if is_qr_user else None  # QRトークンを渡す
        )
        return render_template("table_detail.html", **context)
    finally:
        s.close()



# --- フロア用テーブル一覧API（管理者UI用） -----------------------------------
@app.route("/api/floor/tables")
@require_admin
def api_floor_tables():
    sid = current_store_id()
    if sid is None:
        return jsonify([])

    s = SessionLocal()
    try:
        q = s.query(TableSeat)
        if hasattr(TableSeat, "store_id"):
            q = q.filter(TableSeat.store_id == sid)
        rows = q.order_by(TableSeat.table_no.asc()).all()
        return jsonify([{"id": t.id, "no": t.table_no, "status": t.status} for t in rows])
    finally:
        s.close()


# --- KDS 画面（キッチン表示） -------------------------------------------------
@app.route("/kds")
@require_any
def kds():
    sid = current_store_id()
    if sid is None:
        return redirect(url_for("admin_login"))

    s = SessionLocal()
    try:
        rows = (
            s.query(
                OrderItem.id.label("id"),
                TableSeat.table_no.label("テーブル番号"),
                Menu.name.label("名称"),
                OrderItem.qty.label("数量"),
                OrderItem.memo.label("メモ"),
                OrderItem.status.label("状態")
            )
            .join(OrderHeader, OrderHeader.id == OrderItem.order_id)
            .join(TableSeat, TableSeat.id == OrderHeader.table_id)
            .join(Menu, Menu.id == OrderItem.menu_id)
            .filter(OrderItem.status.in_(["新規", "調理中"]))
            # ★ 店舗絞り込み（store_id があれば）
            .filter(OrderHeader.store_id == sid)
            .order_by(OrderItem.id.desc())
            .all()
        )
        rows_dict = [dict(r._mapping) for r in rows]
        return render_template("kds.html", title="KDS", rows=rows_dict)
    finally:
        s.close()


# --- KDS API：アイテム一覧（カテゴリ絞り込み対応） -----------------------------
@app.route("/api/kds/items")
@require_any
def kds_api_get_items():
    """
    KDS 表示用 API（進捗4カラム方式）
    - ?cat_ids=1,2,3 でカテゴリ絞り込み
    - ?debug=1        で詳細デバッグ情報を JSON + サーバーログに出力
    """
    s = SessionLocal()
    try:
        sid = current_store_id()
        if sid is None:
            return jsonify(ok=False, error="Store not found"), 403

        DEBUG = (request.args.get("debug") == "1")

        raw = (request.args.get("cat_ids") or "").strip()
        cat_ids = [int(x) for x in raw.split(",") if x.strip().isdigit()] if raw else []
        if DEBUG:
            current_app.logger.debug("[KDS] sid=%s raw_cat_ids=%r -> cat_ids=%r", sid, raw, cat_ids)

        # ベース抽出（元行のみ：qty>0。明細.statusは見ない）
        q = (
            s.query(
                OrderItem.id.label("id"),
                OrderItem.order_id.label("order_id"),
                TableSeat.table_no.label("table_no"),
                Menu.name.label("name"),
                OrderItem.qty.label("qty_orig"),
                OrderItem.memo.label("memo"),
                OrderItem.status.label("detail_status"),
                OrderItem.added_at.label("ordered_at"),  # ★ 注文時刻
            )
            .join(OrderHeader, OrderHeader.id == OrderItem.order_id)
            .join(TableSeat, TableSeat.id == OrderHeader.table_id)
            .join(Menu, Menu.id == OrderItem.menu_id)
            .filter(OrderHeader.store_id == sid)
            .filter(OrderHeader.status != "会計済")
            .filter(OrderItem.qty > 0)
        )

        # カテゴリ絞り込み
        menu_ids = None
        if cat_ids:
            in_list = ",".join(str(v) for v in cat_ids)
            menu_ids = s.execute(text(f"""
                SELECT DISTINCT menu_id
                FROM "R_KDSカテゴリ_メニュー"
                WHERE "店舗ID" = :sid
                  AND kds_category_id IN ({in_list})
            """), {"sid": sid}).scalars().all()
            if DEBUG:
                current_app.logger.debug("[KDS] fetched menu_ids len=%d sample=%r",
                                         len(menu_ids or []), (menu_ids[:10] if menu_ids else []))
            if not menu_ids:
                if DEBUG:
                    return jsonify(ok=True, items=[], debug={
                        "sid": sid, "cat_ids": cat_ids, "menu_ids": [], "reason": "no menus for selected categories"
                    })
                return jsonify(ok=True, items=[])

            q = q.filter(OrderItem.menu_id.in_(menu_ids))

        rows = q.order_by(OrderItem.id.desc()).all()
        if DEBUG:
            current_app.logger.debug("[KDS] base rows=%d ids(sample)=%r",
                                     len(rows), [r.id for r in rows[:10]])

        if not rows:
            if DEBUG:
                return jsonify(ok=True, items=[], debug={
                    "sid": sid, "cat_ids": cat_ids, "menu_ids": menu_ids,
                    "rows": 0, "reason": "no base rows"
                })
            return jsonify(ok=True, items=[])

        # 進捗取得（分割しつつ）
        def fetch_progress_for_ids(id_list):
            if not id_list:
                return {}
            CHUNK = 900
            out = {}
            try:
                for i in range(0, len(id_list), CHUNK):
                    chunk = id_list[i:i + CHUNK]
                    in_ids = ",".join(str(i) for i in chunk)
                    rs = s.execute(text(f"""
                        SELECT item_id, qty_new, qty_cooking, qty_served, qty_canceled
                        FROM "T_明細進捗" WHERE item_id IN ({in_ids})
                    """)).mappings().all()
                    for r in rs:
                        out[r["item_id"]] = {
                            "qty_new": int(r["qty_new"]),
                            "qty_cooking": int(r["qty_cooking"]),
                            "qty_served": int(r["qty_served"]),
                            "qty_canceled": int(r["qty_canceled"]),
                        }
            except Exception as e:
                # T_明細進捗テーブルが存在しない場合はスキップ
                app.logger.warning(f"[fetch_progress_for_ids] T_明細進捗テーブルへのアクセスに失敗: {e}")
                s.rollback()
            return out

        ids = [r.id for r in rows]
        prog_map = fetch_progress_for_ids(ids)

        # 進捗テーブルに status 列が無い旧DBへの保険（その場で追加）
        def ensure_progress_has_status():
            try:
                dialect = s.bind.dialect.name
                if dialect == 'sqlite':
                    cols = [c[1] for c in s.execute(text("PRAGMA table_info('T_明細進捗')")).fetchall()]
                else:
                    cols_result = s.execute(text("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'T_明細進捗'
                    """)).fetchall()
                    cols = [c[0] for c in cols_result]

                if "status" not in cols:
                    s.execute(text("""ALTER TABLE "T_明細進捗" ADD COLUMN status TEXT NOT NULL DEFAULT '新規'"""))
                    s.commit()
                    if DEBUG:
                        current_app.logger.debug("[KDS] progress table: added status column with DEFAULT '新規'")
            except Exception as e:
                try:
                    s.rollback()
                except Exception:
                    pass
                current_app.logger.warning("[KDS] ensure_progress_has_status failed: %s", e)

        ensure_progress_has_status()

        # 無いものは「qty_orig でシード」してから使う（★ status='新規' を必ず入れる）
        def _now_iso():
            from datetime import datetime, timezone
            return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        seeded = []
        for r in rows:
            if r.id not in prog_map:
                s.execute(text("""
                    INSERT INTO "T_明細進捗"
                      (item_id, qty_new, qty_cooking, qty_served, qty_canceled, status, updated_at)
                    VALUES (:id, :n, 0, 0, 0, '新規', :ts)
                    ON CONFLICT(item_id) DO UPDATE SET updated_at=:ts
                """), {"id": r.id, "n": int(r.qty_orig or 0), "ts": _now_iso()})
                prog_map[r.id] = {
                    "qty_new": int(r.qty_orig or 0),
                    "qty_cooking": 0,
                    "qty_served": 0,
                    "qty_canceled": 0,
                }
                seeded.append(r.id)

        if seeded and DEBUG:
            current_app.logger.debug("[KDS] progress seeded item_ids=%r", seeded)

        # アイテム構築
        from datetime import datetime, timedelta, timezone  # ★ JST 変換用

        items = []
        filtered_zero = []
        for r in rows:
            p = prog_map.get(r.id, {"qty_new": 0, "qty_cooking": 0, "qty_served": 0, "qty_canceled": 0})
            n, c, sv, cx = p["qty_new"], p["qty_cooking"], p["qty_served"], p["qty_canceled"]
            qty_remain = n + c
            qty_orig = int(r.qty_orig)

            # すべて消化（提供済+取消 == 元数量）→ KDS非表示
            if qty_remain <= 0 and (sv + cx) >= qty_orig:
                filtered_zero.append({"id": r.id, "n": n, "c": c, "sv": sv, "cx": cx, "orig": qty_orig})
                continue

            # ★ 注文時刻（UTC → JST）の整形
            ordered_time = ""
            v = getattr(r, "ordered_at", None)
            if v:
                try:
                    if hasattr(v, "strftime"):
                        # datetime 型
                        dt = v
                        if dt.tzinfo is None:
                            # タイムゾーン無し → UTC とみなして +9時間
                            dt = dt + timedelta(hours=9)
                        else:
                            # 何かしら tzinfo 付き → JST に変換
                            dt = dt.astimezone(timezone(timedelta(hours=9)))
                        ordered_time = dt.strftime("%H:%M")
                    else:
                        # 文字列の場合（例: "YYYY-MM-DD HH:MM:SS" を想定）
                        t = str(v)
                        try:
                            dt = datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
                            dt = dt + timedelta(hours=9)
                            ordered_time = dt.strftime("%H:%M")
                        except ValueError:
                            # パースできなければとりあえず後ろの HH:MM だけ抜く
                            if " " in t:
                                ordered_time = t.split(" ")[1][:5]
                            else:
                                ordered_time = t[:5]
                except Exception:
                    ordered_time = ""

            disp = "調理中" if c > 0 else "新規"
            items.append({
                "id": r.id,
                "table_no": r.table_no,
                "name": r.name,
                "qty": qty_remain,  # 未消化分（新規+調理中）
                "memo": r.memo,
                "status": disp,
                "ordered_time": ordered_time,  # ★ JST に補正済み
                "counts": {
                    "new": n,
                    "cooking": c,
                    "served": sv,
                    "canceled": cx,
                    "original": qty_orig,
                },
            })

        if seeded:
            s.commit()

        if DEBUG:
            dbg = {
                "sid": sid,
                "cat_ids": cat_ids,
                "menu_ids_len": (len(menu_ids) if menu_ids is not None else None),
                "rows": len(rows),
                "progress_found": len(prog_map),
                "seeded_count": len(seeded),
                "filtered_zero": filtered_zero,
                "returned": len(items),
            }
            current_app.logger.debug("[KDS] debug=%r", dbg)
            return jsonify(ok=True, items=items, debug=dbg)

        return jsonify(ok=True, items=items)

    except Exception as e:
        current_app.logger.exception("KDS API error: %s", e)
        return jsonify(ok=False, error="internal error"), 500
    finally:
        s.close()





# --- KDS 管理トップ -----------------------------------------------------------
@app.route("/admin/kds")
@require_admin
def admin_kds_home():
    return render_template("admin_kds_home.html", title="KDS管理")


# --- KDS カテゴリ管理（作成/更新/削除） ---------------------------------------
@app.route("/admin/kds/categories", methods=["GET", "POST"])
@require_admin
def admin_kds_categories():
    ensure_kds_category_tables()  # 保険（IF NOT EXISTSで軽い）
    s = SessionLocal()
    sid = current_store_id()
    try:
        if request.method == "POST":
            act = (request.form.get("act") or "").strip()

            if act == "create":
                name  = (request.form.get("name") or "").strip()
                order = int(request.form.get("order") or 0)
                if name:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    s.execute(text(
                        'INSERT INTO "M_KDSカテゴリ"(名称, 表示順, 有効, "店舗ID", 登録日時, 更新日時) '
                        'VALUES (:n,:o,1,:sid,:t,:t)'
                    ), {"n": name, "o": order, "sid": sid, "t": now})
                    s.commit()
                return redirect(url_for('admin_kds_categories'))

            elif act == "update":
                cid     = int(request.form.get("id") or 0)
                name    = (request.form.get("name") or "").strip()
                order   = int(request.form.get("order") or 0)
                enabled = 1 if (request.form.get("enabled") == "1") else 0
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                s.execute(text(
                    'UPDATE "M_KDSカテゴリ" SET 名称=:n, 表示順=:o, 有効=:e, 更新日時=:t '
                    'WHERE id=:cid AND "店舗ID"=:sid'
                ), {"n": name, "o": order, "e": enabled, "t": now, "cid": cid, "sid": sid})
                s.commit()
                return redirect(url_for('admin_kds_categories'))

            elif act == "delete":
                cid = int(request.form.get("id") or 0)
                # 子→親の順で削除
                s.execute(text(
                    'DELETE FROM "R_KDSカテゴリ_メニュー" WHERE "店舗ID"=:sid AND kds_category_id=:cid'
                ), {"sid": sid, "cid": cid})
                s.execute(text(
                    'DELETE FROM "M_KDSカテゴリ" WHERE "店舗ID"=:sid AND id=:cid'
                ), {"sid": sid, "cid": cid})
                s.commit()
                return redirect(url_for('admin_kds_categories'))

        # GET: 一覧
        cats = s.execute(text(
            'SELECT id, 名称, 表示順, 有効 FROM "M_KDSカテゴリ" '
            'WHERE "店舗ID"=:sid ORDER BY 有効 DESC, 表示順, id'
        ), {"sid": sid}).mappings().all()

        return render_template("admin_kds_categories.html", cats=cats, title="KDSカテゴリ")

    finally:
        s.close()


# --- KDS カテゴリ割当（メニュー⇄KDSカテゴリのマッピング） -----------------------
@app.route("/admin/kds/mapping", methods=["GET", "POST"])
@require_admin
def admin_kds_mapping():
    """KDSカテゴリ ↔ メニュー の割当画面"""
    ensure_kds_category_tables()

    s = SessionLocal()
    sid = current_store_id()
    try:
        # ---------- POST: 割当追加 / 割当削除 ----------
        if request.method == "POST":
            act = request.form.get("act")

            # 新規割当の追加
            if act == "create":
                menu_id = int(request.form.get("menu_id") or 0)
                cat_id  = int(request.form.get("kds_category_id") or 0)
                if menu_id and cat_id:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    # 同じ組み合わせがあれば一旦削除してから INSERT（重複防止）
                    s.execute(text(
                        'DELETE FROM "R_KDSカテゴリ_メニュー" '
                        'WHERE "店舗ID"=:sid AND menu_id=:mid AND kds_category_id=:cid'
                    ), {"sid": sid, "mid": menu_id, "cid": cat_id})

                    s.execute(text(
                        'INSERT INTO "R_KDSカテゴリ_メニュー" '
                        '(kds_category_id, menu_id, "店舗ID", 登録日時) '
                        'VALUES (:cid,:mid,:sid,:t)'
                    ), {"cid": cat_id, "mid": menu_id, "sid": sid, "t": now})
                    s.commit()

            # 割当の削除
            elif act == "delete":
                mapping_id = int(request.form.get("mapping_id") or 0)
                if mapping_id:
                    s.execute(text(
                        'DELETE FROM "R_KDSカテゴリ_メニュー" '
                        'WHERE id=:id AND "店舗ID"=:sid'
                    ), {"id": mapping_id, "sid": sid})
                    s.commit()

            return redirect(url_for("admin_kds_mapping"))

        # ---------- GET: 画面表示用データ取得 ----------

        # KDSカテゴリ一覧（有効なもの）
        kds_categories = s.execute(text(
            'SELECT id, 名称 FROM "M_KDSカテゴリ" '
            'WHERE "店舗ID"=:sid AND 有効=1 '
            'ORDER BY 表示順, id'
        ), {"sid": sid}).mappings().all()

        # ==== メニュー一覧（論理削除 is_deleted = 1 を除外） ====
        def table_exists(name: str) -> bool:
            dialect = s.bind.dialect.name
            if dialect == "sqlite":
                return bool(s.execute(text(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name=:n"
                ), {"n": name}).scalar())
            else:
                return bool(s.execute(text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_name=:n"
                ), {"n": name}).scalar())

        menu_sql = None
        params = {"sid": sid}

        # 英語スキーマ
        if table_exists("Menu"):
            menu_sql = text(
                'SELECT id, name FROM "Menu" '
                'WHERE store_id=:sid AND COALESCE(is_deleted, 0) = 0 '
                'ORDER BY display_order, name'
            )

        # 日本語スキーマ（商品）
        elif table_exists("M_商品"):
            menu_sql = text(
                'SELECT id, 名称 AS name FROM "M_商品" '
                'WHERE "店舗ID"=:sid AND COALESCE(is_deleted, 0) = 0 '
                'ORDER BY 表示順, 名称'
            )

        # 日本語スキーマ（メニュー）
        elif table_exists("M_メニュー"):
            menu_sql = text(
                'SELECT id, 名称 AS name FROM "M_メニュー" '
                'WHERE "店舗ID"=:sid AND COALESCE(is_deleted, 0) = 0 '
                'ORDER BY 表示順, 名称'
            )

        menus = []
        if menu_sql is not None:
            menus = s.execute(menu_sql, params).mappings().all()
        else:
            try:
                flash("メニューのテーブルが見つかりません。先にメニューの初期セットアップを行ってください。", "warning")
            except Exception:
                pass

        # menu_id → menu_name の辞書
        menu_name_map = {m["id"]: m["name"] for m in menus}

        # 割当済み一覧
        rows = s.execute(text("""
            SELECT r.id,
                   r.menu_id,
                   kc.名称 AS kds_category_name
            FROM "R_KDSカテゴリ_メニュー" r
            JOIN "M_KDSカテゴリ" kc
              ON kc.id = r.kds_category_id
             AND kc."店舗ID" = r."店舗ID"
            WHERE r."店舗ID" = :sid
            ORDER BY r.menu_id, kc.名称, r.id
        """), {"sid": sid}).mappings().all()

        mappings = []
        assigned_menu_ids = set()
        for r in rows:
            mid = r["menu_id"]
            assigned_menu_ids.add(mid)
            mappings.append({
                "id": r["id"],
                "menu_name": menu_name_map.get(mid, f"ID {mid}"),
                "kds_category_name": r["kds_category_name"],
            })

        # ★ 未割当メニュー一覧（削除されていない & 割当の無いもの）
        unassigned_menus = [
            m for m in menus if m["id"] not in assigned_menu_ids
        ]

        return render_template(
            "admin_kds_mapping.html",
            menus=menus,
            kds_categories=kds_categories,
            mappings=mappings,
            unassigned_menus=unassigned_menus,
            title="KDSカテゴリ割当",
        )

    finally:
        s.close()




# --- KDS API：カテゴリ一覧（スタッフ権限） -----------------------------------
@app.route("/api/kds/categories")
@require_staff  # 権限は運用に合わせて
def api_kds_categories():
    s = SessionLocal()
    sid = current_store_id()
    try:
        cats = s.execute(text('SELECT id, 名称 FROM "M_KDSカテゴリ" WHERE "店舗ID"=:sid AND 有効=1 ORDER BY 表示順, id'),
                         {"sid": sid}).mappings().all()
        return jsonify(ok=True, categories=[{"id": c["id"], "name": c["名称"]} for c in cats])
    finally:
        s.close()



# =============================================================================
# KDS・注文表示／会計 API セクション
# =============================================================================

# --- [Asset] KDS通知サウンド（新規注文アラート WAV 配信） -----------------------
@app.route("/assets/order_notify.wav")
def kds_order_notify_sound():
    """
    KDS の新規注文アラート音（order_notify.wav）を配信するエンドポイント。
    /mnt/data に配置されたファイルを audio/wav で返す。
    """
    return send_from_directory(
        "/mnt/data",
        "order_notify.wav",
        mimetype="audio/wav",
        max_age=3600
    )


# --- [KDS Helper] 注文明細ステータス再計算（ヘッダ） ---------------------------
def _recalc_order_status(session_db, header: OrderHeader) -> None:
    items = (session_db.query(OrderItem.status)
             .filter(OrderItem.order_id == header.id,
                     OrderItem.status != "取消").all())
    statuses = [row[0] for row in items]
    if not statuses:
        header.status = "新規"; return
    if all(st == "提供済" for st in statuses):
        header.status = "提供済"; return
    if any(st == "調理中" for st in statuses):
        header.status = "調理中"; return
    header.status = "新規"


# --- [KDS Helper] 注文金額再計算（小計・税額・合計） ---------------------------
def _recalc_order_amounts(session_db, header: OrderHeader) -> None:
    rows = (session_db.query(OrderItem.unit_price, OrderItem.tax_rate, OrderItem.qty)
            .filter(OrderItem.order_id == header.id, OrderItem.status != "取消").all())
    subtotal, taxsum = 0, 0
    for unit_price, tax_rate, qty in rows:
        unit, rate, q = int(unit_price or 0), float(tax_rate or 0.0), int(qty or 0)
        subtotal += unit * q
        per_unit_tax = int(math.floor(unit * rate))
        taxsum += per_unit_tax * q
    header.subtotal, header.tax, header.total = int(subtotal), int(taxsum), int(subtotal + taxsum)


# --- [Menu API] カテゴリ別メニュー一覧 -----------------------------------------
@app.get("/api/menus/by_category/<int:category_id>", endpoint="api_menus_by_category")
def api_menus_by_category(category_id: int):
    s = SessionLocal()
    try:
        # QRトークンから店舗IDを取得（QRセルフオーダー用）
        token = request.args.get("token")
        if token:
            qt = s.query(QrToken).filter_by(token=token).first()
            if not qt:
                return jsonify(ok=False, error="無効なトークン"), 400
            sid = qt.store_id
        else:
            # セッションから店舗IDを取得（従業員用）
            sid = current_store_id()
        
        if sid is None:
            return jsonify(ok=False, error="店舗スコープ不明"), 400

        from sqlalchemy.orm import aliased
        L = aliased(ProductCategoryLink)

        q = s.query(Menu)
        if hasattr(Menu, "store_id"):
            q = q.filter(Menu.store_id == sid)

        # 削除済みメニューは注文画面に表示しない
        if hasattr(Menu, "is_deleted"):
            q = q.filter(Menu.is_deleted == 0)

        if category_id != 0:  # 0=すべて
            q = (
                q.join(L, L.product_id == Menu.id)
                 .filter(L.category_id == category_id)
                 # ★ カテゴリ内の表示順を最優先
                 .order_by(
                     L.display_order.asc(),
                     Menu.display_order.asc(),
                     Menu.id.asc()
                 )
            )
        else:
            # すべて表示の既存ルール（必要ならカテゴリ順も含めて拡張可）
            q = q.order_by(Menu.display_order.asc(), Menu.id.asc())

        rows = q.all()

        out = []
        for m in rows:
            eff_rate  = resolve_effective_tax_rate_for_menu(s, m.id, m.tax_rate)
            price_excl = int(m.price)
            price_incl = display_price_incl_from_excl(price_excl, eff_rate)
            out.append({
                "id": m.id,
                "name": m.name,
                "description": m.description or "",
                "photo_url": m.photo_url,
                "price_excl": price_excl,
                "price_incl": price_incl,
                "available": int(m.available or 0),
                "is_market_price": bool(getattr(m, "is_market_price", 0)),
            })
        return jsonify(ok=True, menus=out)
    except Exception as e:
        app.logger.error("[api_menus_by_category] %s", e, exc_info=True)
        return jsonify(ok=False, error=str(e)), 500
    finally:
        s.close()


# --- [Order API] 現在の明細 JSON（管理画面） -----------------------------------
@app.route("/admin/order/<int:order_id>/detail/json")
@require_any  # ← staff/admin どちらでも可。必要に応じて @require_staff 等に変更
def order_detail_json(order_id: int):
    sid = current_store_id()
    app.logger.info(f"[DETAIL-JSON] in sid={sid} order_id={order_id}")

    if sid is None:
        app.logger.warning("[DETAIL-JSON] no store in session")
        return jsonify(ok=False, error="no store"), 403

    s = SessionLocal()
    try:
        h = s.get(OrderHeader, order_id)
        if not h:
            app.logger.warning(f"[DETAIL-JSON] order not found: {order_id}")
            return jsonify(ok=False, error="order not found"), 404
        if hasattr(h, "store_id") and h.store_id != sid:
            app.logger.warning(f"[DETAIL-JSON] store mismatch: sid={sid} header.sid={getattr(h,'store_id',None)}")
            return jsonify(ok=False, error="order not for this store"), 404

        # --------- ヘルパ ---------
        def _is_cancel_item(it) -> bool:
            for nm in ("is_cancel", "is_cancelled", "cancelled"):
                if getattr(it, nm, None):
                    return True
            raw = getattr(it, "status", None) or getattr(it, "item_status", None) or getattr(it, "state", None)
            if raw is None:
                return False
            sraw = str(raw).lower()
            return ("取消" in sraw) or ("ｷｬﾝｾﾙ" in sraw) or ("cancel" in sraw) or ("void" in sraw)

        def _is_served_item(it) -> bool:
            if any([getattr(it,"served",None), getattr(it,"is_served",None),
                    getattr(it,"served_at",None), getattr(it,"provided",None),
                    getattr(it,"is_provided",None), getattr(it,"provided_at",None)]):
                return True
            try:
                qty = int(getattr(it, "qty", getattr(it, "数量", 0)) or 0)
                provided_qty = int(getattr(it, "served_qty", None)
                                   or getattr(it, "provided_qty", None)
                                   or getattr(it, "提供数量", None) or 0)
                if qty > 0 and provided_qty >= qty:
                    return True
            except Exception:
                pass
            raw = getattr(it, "status", None) or getattr(it, "item_status", None) or getattr(it, "state", None) or ""
            sraw = str(raw).lower()
            return ("提供済" in sraw) or ("提供完了" in sraw) or ("served" in sraw) or ("done" in sraw) or ("completed" in sraw)

        def _status_label(it) -> str:
            if _is_cancel_item(it): return "取消"
            if _is_served_item(it): return "提供済"
            raw = getattr(it, "status", None) or getattr(it, "item_status", None) or getattr(it, "state", None) or ""
            sraw = str(raw).lower()
            if ("調理中" in sraw) or ("cooking" in sraw) or ("in_progress" in sraw):
                return "調理中"
            return "新規"

        def _name_of(it) -> str:
            m = getattr(it, "menu", None)
            if m and getattr(m, "name", None):
                return str(m.name)
            if getattr(it, "name", None):
                return str(it.name)
            if getattr(it, "名称", None):
                return str(getattr(it, "名称"))
            mid = getattr(it, "menu_id", None) or getattr(it, "menuId", None)
            return str(mid) if mid is not None else f"item#{getattr(it,'id','')}"

        # --------- 明細取得 ---------
        q = s.query(OrderItem).filter(OrderItem.order_id == order_id)
        if hasattr(OrderItem, "store_id"):
            q = q.filter(OrderItem.store_id == sid)
        items = q.order_by(OrderItem.id.asc()).all()
        app.logger.info(f"[DETAIL-JSON] items={len(items)}")

        # --------- 整形／集計 ---------
        out_items = []
        total_excl, total_incl = 0, 0
        for it in items:
            qty  = int(getattr(it, "qty", 0) or 0)
            unit = int(getattr(it, "unit_price", 0) or 0)
            rate = float(getattr(it, "tax_rate", 0.0) or 0.0)
            unit_incl = unit + int(math.floor(unit * rate))
            sub_excl  = unit * qty
            sub_incl  = unit_incl * qty
            lbl = _status_label(it)

            if not _is_cancel_item(it):
                total_excl += sub_excl
                total_incl += sub_incl

            # progress情報を計算（提供済・取消の数量）
            qty_served = 0
            qty_canceled = 0
            
            if _is_cancel_item(it):
                # 取消の場合は全数量を取消としてカウント
                qty_canceled = abs(qty)
            elif _is_served_item(it):
                # 提供済の場合は全数量を提供済としてカウント
                qty_served = abs(qty)
            
            # served_qtyやprovided_qtyが明示的に設定されている場合はそれを使用
            if hasattr(it, 'served_qty') and it.served_qty is not None:
                qty_served = int(it.served_qty or 0)
            elif hasattr(it, 'provided_qty') and it.provided_qty is not None:
                qty_served = int(it.provided_qty or 0)

            # メモの取得
            memo = getattr(it, "memo", None) or getattr(it, "メモ", None) or getattr(it, "note", None) or ""
            
            out_items.append({
                "name": _name_of(it),
                "qty": qty,
                "unit_excl": unit,
                "unit_incl": unit_incl,
                "sub_excl": sub_excl,
                "sub_incl": sub_incl,
                "status": lbl,
                "memo": memo,
                "メモ": memo,
                "progress": {
                    "qty_served": qty_served,
                    "qty_canceled": qty_canceled
                }
            })

        # ヘッダ保存済み金額があれば優先
        h_sub = getattr(h, "subtotal", None)
        h_tax = getattr(h, "tax", None)
        h_tot = getattr(h, "total", None)
        if h_sub is not None and h_tax is not None and h_tot is not None:
            try:
                total_excl = int(h_sub or 0)
                total_incl = int(h_tot or (total_excl + int(h_tax or 0)))
            except Exception:
                pass

        opened_at = None
        if hasattr(h, "opened_at") and h.opened_at:
            try: opened_at = h.opened_at.isoformat()
            except Exception: opened_at = str(h.opened_at)

        payload = {
            "ok": True,
            "order": {
                "id": h.id,
                "status": getattr(h, "status", "") or "",
                "opened_at": opened_at,
                "total_excl": int(total_excl),
                "total_incl": int(total_incl),
                "join_pin": getattr(h, "join_pin", None),
                "join_pin_expires_at": getattr(h, "join_pin_expires_at", None),
            },
            "items": out_items
        }
        app.logger.info(f"[DETAIL-JSON] send totals excl={payload['order']['total_excl']} incl={payload['order']['total_incl']}")
        return jsonify(payload)

    except Exception as e:
        app.logger.exception("[DETAIL-JSON] unexpected error")
        return jsonify(ok=False, error=str(e)), 500
    finally:
        s.close()




# --- [Order API] 会計完了（残額=0 確認→クローズ） ------------------------------
# --- dev guard: avoid duplicate endpoint registration (開発中のみ) ------------
if "order_complete" in app.view_functions:
    app.logger.warning("replacing endpoint: order_complete")
    del app.view_functions["order_complete"]


# --- [Order API] 会計完了（残額=0 確認→クローズ） -----------------------------------------------
@app.route("/admin/order/<int:order_id>/complete", methods=["POST"])
@require_staff
def order_complete(order_id: int):
    """
    残額=0 なら会計完了にする。
    判定・合計更新は /admin/order/<id>/summary と同じルール:
      - 正数量かつ状態が 取消/キャンセル/void の行は合計から除外
      - 負数量(監査行)はネットに反映
      - 既払は PaymentRecord 合算（返金はマイナス）
    """
    from sqlalchemy import func
    import math
    from datetime import datetime, timezone

    s = SessionLocal()
    try:
        Header = globals().get("OrderHeader")
        Item   = globals().get("OrderItem")
        Pay    = globals().get("PaymentRecord") or globals().get("T_支払")
        Table  = globals().get("TableSeat")

        sid = current_store_id()
        h = s.get(Header, order_id)
        if not h or (hasattr(h, "store_id") and sid is not None and getattr(h, "store_id") != sid):
            return jsonify(ok=False, error="order not found"), 404

        # 実効 store_id（ヘッダ優先）
        sid_eff = getattr(h, "store_id", None) if getattr(h, "store_id", None) is not None else sid

        # --- 明細ネット合計（内税・取消ラベル除外・負数量反映） ---
        subtotal_excl = 0
        tax_total     = 0
        total_incl    = 0

        CANCEL_WORDS = ("取消", "ｷｬﾝｾﾙ", "キャンセル", "cancel", "void")

        from sqlalchemy.orm import joinedload
        qi = s.query(Item).options(joinedload(Item.menu)).filter(getattr(Item, "order_id") == order_id)
        if hasattr(Item, "store_id") and sid_eff is not None:
            qi = qi.filter(getattr(Item, "store_id") == sid_eff)
        items = qi.all()

        for d in items:
            qty = int(getattr(d, "qty", None) or getattr(d, "数量", None) or 0)
            if qty == 0:
                continue
            st = str(getattr(d, "status", None) or getattr(d, "状態", None) or "").lower()
            # 正数量の取消ラベルは除外
            if qty > 0 and any(w in st for w in CANCEL_WORDS):
                continue

            unit_excl = int(
                getattr(d, "unit_price", None)
                or getattr(d, "税抜単価", None)
                or getattr(d, "price_excl", None)
                or getattr(d, "price", None)
                or 0
            )
            rate = float(
                getattr(d, "tax_rate", None)
                or getattr(getattr(d, "menu", None), "tax_rate", None)
                or 0.10
            )
            unit_tax  = math.floor(unit_excl * rate)
            unit_incl = unit_excl + unit_tax

            subtotal_excl += unit_excl * qty
            tax_total     += unit_tax  * qty
            total_incl    += unit_incl * qty

        # --- 既払（返金はマイナス） ---
        paid = 0
        if Pay is not None:
            col_amount = getattr(Pay, "amount", None) or getattr(Pay, "金額", None)
            if col_amount is not None:
                qp = s.query(func.coalesce(func.sum(col_amount), 0)).filter(
                    getattr(Pay, "order_id") == order_id
                )
                if hasattr(Pay, "store_id") and sid_eff is not None:
                    qp = qp.filter(getattr(Pay, "store_id") == sid_eff)
                paid = int(qp.scalar() or 0)

        remaining = int(total_incl) - int(paid)
        if remaining != 0:
            return jsonify(ok=False, error="残額があるため完了できません", summary={
                "subtotal": int(subtotal_excl), "tax": int(tax_total),
                "total": int(total_incl), "paid": int(paid), "remaining": int(remaining)
            }), 400

        # ヘッダの金額を最新で反映してからステータス更新
        if hasattr(h, "subtotal"): h.subtotal = int(subtotal_excl)
        if hasattr(h, "tax"):      h.tax      = int(tax_total)
        if hasattr(h, "total"):    h.total    = int(total_incl)
        if hasattr(h, "合計"):      setattr(h, "合計", int(total_incl))

        if hasattr(Header, "status"):   h.status   = "会計済"
        if hasattr(Header, "closed_at"): h.closed_at = datetime.now(timezone.utc)

        # テーブルを空席へ
        if getattr(h, "table_id", None):
            t = s.get(Table, getattr(h, "table_id"))
            if t and hasattr(Table, "status"):
                t.status = "空席"

        # ===== ここから、あなたの既存のバックフィル/履歴/リセット処理 =====
        if getattr(h, "table_id", None):
            row = (
                s.query(T_お客様詳細)
                .filter(T_お客様詳細.table_id == h.table_id, T_お客様詳細.order_id == None)  # noqa: E711
                .order_by(T_お客様詳細.id.desc())
                .first()
            )
            if row:
                row.order_id = h.id
                if getattr(row, "store_id", None) in (None, 0) and getattr(h, "store_id", None):
                    row.store_id = h.store_id
                current_app.logger.info(
                    "[backfill] T_お客様詳細 id=%s table_id=%s -> order_id=%s",
                    getattr(row, "id", None), h.table_id, h.id
                )
                s.flush()
            else:
                peek = (
                    s.query(T_お客様詳細)
                    .filter(T_お客様詳細.table_id == h.table_id)
                    .order_by(T_お客様詳細.id.desc())
                    .limit(5)
                    .all()
                )
                current_app.logger.info(
                    "[backfill][peek] table_id=%s (no orphan found). recent=%s",
                    h.table_id,
                    [{"id": getattr(r, "id", None), "order_id": getattr(r, "order_id", None),
                      "store_id": getattr(r, "store_id", None)} for r in peek],
                )

        rec = append_checkout_customer_detail_history(
            s,
            order_id=order_id,
            store_id=getattr(h, "store_id", None),
            table_id=getattr(h, "table_id", None),
            reason="会計完了",
            author=(session.get("staff_name") or session.get("admin_name") or None),
        )

        try:
            reset_info = _reset_customer_detail_after_checkout(
                s, order_id=order_id, table_id=getattr(h, "table_id", None)
            )
            s.flush()
        except Exception:
            current_app.logger.exception("reset customer detail after checkout failed")
            reset_info = {"error": "exception in reset"}

        try:
            need_fallback = False
            if isinstance(reset_info, dict) and "error" not in reset_info:
                by_order = reset_info.get("by_order") or reset_info.get("deleted_by_order") or 0
                orphans  = reset_info.get("orphans")  or reset_info.get("deleted_orphan_by_table") or 0
                fallback = reset_info.get("fallback_by_table") or 0
                if (by_order + orphans + fallback) == 0 and getattr(h, "table_id", None):
                    need_fallback = True
            if need_fallback:
                del_cnt = (
                    s.query(T_お客様詳細)
                     .filter(T_お客様詳細.table_id == h.table_id)
                     .delete(synchronize_session=False)
                )
                s.flush()
                current_app.logger.warning(
                    "[reset_customer_detail][route-fallback] force-deleted table_id=%s -> %s rows",
                    h.table_id, del_cnt
                )
                if isinstance(reset_info, dict):
                    reset_info["route_fallback_deleted"] = del_cnt
        except Exception:
            current_app.logger.exception("route-level fallback delete failed")

        s.flush()
        new_id = getattr(rec, "id", None)
        total  = getattr(rec, "合計人数", None)
        current_app.logger.info(
            "[order_complete] history_id=%s total=%s order=%s reset=%s",
            new_id, total, order_id, reset_info
        )

        s.commit()
        try:
            mark_floor_changed()
        except Exception:
            pass
        return jsonify(ok=True, history_id=new_id, history_total=total, reset=reset_info)

    except Exception as e:
        s.rollback()
        current_app.logger.exception("order_complete failed")
        return jsonify(ok=False, error=str(e)), 500
    finally:
        s.close()






# --- debug: お客様詳細（T_お客様詳細）覗き見 -----------------------------------------------
@app.get("/__probe/customer_detail")
def __probe_customer_detail():
    s = SessionLocal()
    try:
        order_id = request.args.get("order_id", type=int)
        table_id = request.args.get("table_id", type=int)
        Model = globals().get("T_お客様詳細")
        if Model is None:
            return jsonify(ok=False, error="T_お客様詳細 not found"), 404
        q = s.query(Model)
        if order_id is not None:
            q = q.filter(Model.order_id == order_id)
        if table_id is not None:
            q = q.filter(Model.table_id == table_id)
        rows = q.order_by(Model.id.desc()).limit(50).all()
        data = [{
            "id": getattr(r, "id", None),
            "order_id": getattr(r, "order_id", None),
            "table_id": getattr(r, "table_id", None),
            "store_id": getattr(r, "store_id", None),
        } for r in rows]
        return jsonify(ok=True, count=len(data), rows=data)
    except Exception as e:
        current_app.logger.exception("__probe_customer_detail failed")
        return jsonify(ok=False, error=str(e)), 500
    finally:
        s.close()



# --- debug: お客様詳細履歴を強制追加（/__debug/append_history/<order_id>） -----------------------
@app.post("/__debug/append_history/<int:order_id>")
def __debug_append_history(order_id):
    s = SessionLocal()
    try:
        # 任意: store_id / table_id は注文ヘッダから取得
        h = s.get(OrderHeader, order_id)
        store_id = getattr(h, "store_id", None) if h else None
        table_id = getattr(h, "table_id", None) if h else None

        rec = append_checkout_customer_detail_history(
            s,
            order_id=order_id,
            store_id=store_id,
            table_id=table_id,
            reason="DEBUG append",
            author="debug",
        )
        s.commit()
        return jsonify(
            ok=True,
            id=getattr(rec, "id", None),
            total=getattr(rec, "合計人数", None)
        )
    except Exception as e:
        s.rollback()
        return jsonify(ok=False, error=str(e)), 500
    finally:
        s.close()




# --- [Order API] 支払取消（全 Payment void → 状態調整） -------------------------
@app.route("/admin/order/<int:order_id>/void-payments", methods=["POST"])
@require_staff
def order_void_payments(order_id: int):
    s = SessionLocal()
    try:
        sid = current_store_id()
        h = s.get(OrderHeader, order_id)
        if not h or (hasattr(h, "store_id") and sid is not None and getattr(h, "store_id") != sid):
            return jsonify(ok=False, error="order not found"), 404

        # 会計済は取り消し不可（※要件に合わせてここを緩める場合は席占有チェックを追加）
        if getattr(h, "status", None) == "会計済":
            return jsonify(ok=False, error="会計済のため取り消せません"), 400

        # ------- 支払記録の削除 -------
        PayModel = globals().get("PaymentRecord")
        if PayModel is None:
            return jsonify(ok=False, error="PaymentRecord model not found"), 500

        q = s.query(PayModel).filter(PayModel.order_id == order_id)
        if hasattr(PayModel, "store_id") and sid is not None:
            q = q.filter(PayModel.store_id == sid)
        deleted = q.delete(synchronize_session=False)
        s.flush()

        # ------- サマリ再取得 -------
        sm = _get_payment_summary(s, order_id)
        if not sm.get("exists"):
            return jsonify(ok=False, error="order not found after void"), 404

        # ------- ステータス更新 -------
        if hasattr(h, "status"):
            h.status = "新規" if sm.get("paid", 0) == 0 else "会計中"

        # テーブル状態：空席なら着席へ戻す（再オープンの意図）
        try:
            if hasattr(h, "table_id") and h.table_id:
                t = s.get(TableSeat, h.table_id)
                if t and hasattr(t, "status") and (t.status in (None, "", "空席")):
                    t.status = "着席"
        except Exception:
            pass

        # ------- 来客情報の復元（履歴 → 現在） -------
        restored = False
        try:
            if "restore_customer_detail_from_history" in globals():
                restored = bool(restore_customer_detail_from_history(s, order_id))
        except Exception:
            restored = False

        # ------- 取消明細を除外した金額の再計算（ヘルパがあれば） -------
        try:
            if "recalc_order_totals_excluding_cancel" in globals():
                recalc_order_totals_excluding_cancel(s, order_id)
                # サマリも更新し直す
                sm = _get_payment_summary(s, order_id)
        except Exception:
            pass

        s.commit()
        mark_floor_changed()
        return jsonify(ok=True, summary=sm, restored_guests=restored, deleted_payments=deleted)
    except Exception as e:
        s.rollback()
        return jsonify(ok=False, error=str(e)), 500
    finally:
        s.close()



# --- [Public API] 現在の明細 JSON（トークン検証） -------------------------------
@app.get("/public/order/<int:order_id>/detail/json")
def public_order_detail_json(order_id: int):
    token = (request.args.get("token") or "").strip()
    if not token:
        return jsonify(ok=False, error="token required"), 400

    table_id = verify_token(token)
    if not table_id:
        return jsonify(ok=False, error="invalid token"), 403

    s = SessionLocal()
    try:
        h = s.get(OrderHeader, order_id)
        if not h or int(getattr(h, "table_id", 0) or 0) != int(table_id):
            # トークンのテーブルに紐付いていない order_id は見せない
            return jsonify(ok=False, error="order not found"), 404

        # 明細を取得
        items = (s.query(OrderItem)
                   .filter(OrderItem.order_id == order_id)
                   .order_by(OrderItem.id.asc())
                   .all())

        out_items = []
        subtotal = 0
        taxsum   = 0
        for it in items:
            unit = int(getattr(it, "unit_price", 0) or 0)        # 税抜
            rate = float(getattr(it, "tax_rate", 0.10) or 0.10)  # 税率
            qty  = int(getattr(it, "qty", 0) or 0)

            unit_incl = unit + int(math.floor(unit * rate))
            sub_excl  = unit * qty
            sub_incl  = unit_incl * qty

            subtotal += sub_excl
            taxsum   += int(math.floor(unit * rate)) * qty

            # 名前の取得（menu リレーションがあれば優先）
            name = None
            try:
                if getattr(it, "menu", None) and getattr(it.menu, "name", None):
                    name = it.menu.name
            except Exception:
                pass
            if not name:
                name = getattr(it, "name", None) or f"item#{getattr(it, 'id', '')}"

            status = getattr(it, "status", "") or ""
            
            # メモの取得
            memo = getattr(it, "memo", None) or getattr(it, "メモ", None) or getattr(it, "note", None) or ""
            
            out_items.append({
                "name": str(name),
                "qty": qty,
                "unit_excl": unit,
                "unit_incl": unit_incl,
                "sub_excl": sub_excl,
                "sub_incl": sub_incl,
                "status": str(status),
                "memo": memo,
                "メモ": memo,
            })

        total_excl = int(getattr(h, "subtotal", None) or subtotal)
        total_incl = int(getattr(h, "total",    None) or (subtotal + taxsum))
        try:
            opened_at = h.opened_at.isoformat()
        except Exception:
            opened_at = str(getattr(h, "opened_at", "") or "")

        # Include join_pin and expiration fields so that the frontend can display the PIN.
        return jsonify(ok=True, order={
            "id": h.id,
            "status": getattr(h, "status", "") or "",
            "opened_at": opened_at,
            "total_excl": total_excl,
            "total_incl": total_incl,
            "join_pin": getattr(h, "join_pin", None),
            "join_pin_expires_at": getattr(h, "join_pin_expires_at", None),
        }, items=out_items)
    except Exception as e:
        app.logger.exception("[PUBLIC-DETAIL-JSON] unexpected error")
        return jsonify(ok=False, error=str(e)), 500
    finally:
        s.close()


# --- [Table API] テーブル番号ラベル取得 ----------------------------------------
@app.get("/api/tables/<int:table_id>/label")
def api_table_label(table_id: int):
    s = SessionLocal()
    try:
        t = s.get(TableSeat, table_id)
        v = None
        if t:
            v = getattr(t, "table_no", None) or getattr(t, "テーブル番号", None)
        table_no = str(v if (v is not None and str(v).strip()) else table_id)
        return jsonify({"ok": True, "table_id": table_id, "table_no": table_no})
    finally:
        s.close()



# =============================================================================
# 分割会計：サマリー／決済登録 API
# =============================================================================

# --- [Settle Helper] 支払サマリー取得 ------------------------------------------
def _get_payment_summary(session_db, order_id: int) -> dict:
    order = session_db.get(OrderHeader, order_id)
    if not order:
        return {"exists": False}
    paid = session_db.query(func.coalesce(func.sum(PaymentRecord.amount), 0)) \
        .filter(PaymentRecord.order_id == order_id).scalar() or 0
    # total が未計算なら subtotal+tax でフォールバック
    total = int( (getattr(order, "total", None)
                  if getattr(order, "total", None) is not None
                  else (int(getattr(order, "subtotal", 0) or 0) + int(getattr(order, "tax", 0) or 0))) )
    remaining = max(0, total - int(paid))
    return {"exists": True, "order_id": order_id, "total": total,
            "paid": int(paid), "remaining": int(remaining),
            "status": getattr(order, "status", None), "table_id": int(order.table_id)}



# --- ヘルパ：T_注文 / T_注文明細 / T_支払 を解決 ------------------------------------
def _get_models_for_orders():
    TOrder = globals().get("T_注文") or globals().get("OrderHeader")
    TItem  = globals().get("T_注文明細") or globals().get("T_注文詳細") or globals().get("OrderItem")
    PayRec = (globals().get("T_支払") or globals().get("PaymentRecord"))
    return TOrder, TItem, PayRec



# --- ヘルパ：T_注文 / T_注文明細 を使って合計・既払・残額を計算 ----------------------------------
def _get_models_for_orders():
    """環境にあるモデルを柔軟に解決"""
    TOrder = globals().get("T_注文") or globals().get("OrderHeader")
    TItem  = globals().get("T_注文明細") or globals().get("T_注文詳細") or globals().get("OrderItem")
    PayRec = globals().get("PaymentRecord")  # 既存の支払テーブル（変更しない）
    return TOrder, TItem, PayRec

def _calc_order_summary_from_T(s, *, store_id: int, table_id: int):
    """
    テーブル上の『最新のアクティブ注文』を T_注文/T_注文明細 から集計し、
    {"id", "合計", "既払", "残額", "状態"} を返す。無ければ None。
    """
    TOrder, TItem, PayRec = _get_models_for_orders()
    if not TOrder:
        return None

    # 進行中ステータスは既存定義に合わせる
    active_status = ["新規", "調理中", "提供済", "会計中"]

    # 1) 最新アクティブ注文を取得
    q = s.query(TOrder).filter(getattr(TOrder, "table_id") == table_id)
    if hasattr(TOrder, "store_id"):
        q = q.filter(getattr(TOrder, "store_id") == store_id)
    if hasattr(TOrder, "status"):
        q = q.filter(getattr(TOrder, "status").in_(active_status))
    hdr = q.order_by(getattr(TOrder, "id").desc()).first()
    if not hdr:
        return None

    # 2) 合計金額（ヘッダに total/subtotal/tax があれば優先）
    total = getattr(hdr, "total", None)
    subtotal = getattr(hdr, "subtotal", None)
    tax = getattr(hdr, "tax", None)

    def _num(x, default=0):
        try:
            return int(x)
        except Exception:
            try:
                return float(x or 0)
            except Exception:
                return default

    if total is None:
        # 明細から再計算（カラム名が英/和混在しても動くように吸収）
        items = []
        if TItem:
            items = s.query(TItem).filter(getattr(TItem, "order_id") == getattr(hdr, "id")).all()

        sub = 0
        tx  = 0
        for it in items:
            unit = (_num(getattr(it, "unit_price", None))
                    or _num(getattr(it, "単価", None)))
            qty  = (_num(getattr(it, "qty", None))
                    or _num(getattr(it, "数量", None), 1))
            # 税率が無ければ 0 とみなす（税抜で運用している場合）
            rate = (getattr(it, "tax_rate", None) if hasattr(it, "tax_rate") else
                    getattr(it, "税率", None))
            rate = float(rate or 0)
            sub += unit * qty
            # 既存互換：1行の税 = floor(unit*rate) * qty
            tx  += int(unit * rate) * qty
        subtotal = sub
        tax = tx
        total = sub + tx

    # 3) 既払（既存の PaymentRecord を利用。T_支払があるなら置換可）
    paid = 0
    if PayRec is not None:
        for p in s.query(PayRec).filter(getattr(PayRec, "order_id") == getattr(hdr, "id")).all():
            paid += _num(getattr(p, "amount", None))

    remaining = max(0, _num(total) - _num(paid))

    return {
        "id": getattr(hdr, "id"),
        "合計": _num(total),
        "既払": _num(paid),
        "残額": _num(remaining),
        "状態": getattr(hdr, "status", None),
    }




# --- 会計サマリ（取消除外版） ------------------------------------------
@app.route("/admin/order/<int:order_id>/summary")
@require_store_admin
def admin_order_summary(order_id: int):
    """
    会計モーダル用サマリ
      合計: 明細ネット合計（数量の負は反映、ただし“正数量かつ状態=取消(キャンセル)”は除外、内税）
      既払: 支払合計（返金はマイナス）
      残額: 合計 - 既払
    店舗フィルタはヘッダの store_id を優先
    """
    from sqlalchemy import func
    import math

    s = SessionLocal()
    try:
        # キャッシュをクリアして最新のデータを取得
        s.expire_all()
        
        Header = globals().get("OrderHeader")
        Item   = globals().get("OrderItem")
        Pay    = globals().get("PaymentRecord") or globals().get("T_支払")

        if Header is None or Item is None:
            return jsonify({"ok": False, "error": "models not found"}), 500

        h = s.get(Header, order_id)
        if not h:
            return jsonify({"ok": False, "error": "order not found"}), 404

        # 店舗スコープ検証（ヘッダ優先）
        sid_req = current_store_id()
        sid_hdr = getattr(h, "store_id", None)
        if hasattr(Header, "store_id") and sid_req is not None:
            if sid_hdr is not None and sid_hdr != sid_req:
                return jsonify({"ok": False, "error": "forbidden"}), 403
        sid_eff = sid_hdr if sid_hdr is not None else sid_req

        # --- 明細ネット合計（内税） ---
        subtotal_excl = 0
        tax_total     = 0
        total_incl    = 0

        from sqlalchemy.orm import joinedload
        qi = s.query(Item).options(joinedload(Item.menu)).filter(getattr(Item, "order_id") == order_id)
        if hasattr(Item, "store_id") and sid_eff is not None:
            qi = qi.filter(getattr(Item, "store_id") == sid_eff)
        items = qi.all()
        CANCEL_WORDS = ("取消", "ｷﾔﾞﾝｾﾙ", "キャンセル", "cancel", "void")
        
        # 時価商品のリスト（actual_priceが未設定のもの）
        market_price_items = []

        for d in items:
            qty = int(getattr(d, "qty", None) or getattr(d, "数量", None) or 0)
            if qty == 0:
                continue

            # 「正数量かつ取消ラベル」は合計から除外する
            st = str(getattr(d, "status", None) or getattr(d, "状態", None) or "").lower()
            if qty > 0 and any(w in st for w in CANCEL_WORDS):
                continue  # ←ここがポイント

            unit_excl = int(
                getattr(d, "unit_price", None)
                or getattr(d, "税抜単価", None)
                or getattr(d, "price_excl", None)
                or getattr(d, "price", None)
                or 0
            )
            rate = float(
                getattr(d, "tax_rate", None)
                or getattr(getattr(d, "menu", None), "tax_rate", None)
                or 0.10
            )
            
            # 時価商品の場合、actual_priceを使用（Python属性名を使用）
            menu = getattr(d, "menu", None)
            is_market_price = getattr(menu, "is_market_price", 0) if menu else 0
            actual_price = getattr(d, "actual_price", None)
            
            if menu:
                app.logger.debug(f"[summary] item_id={getattr(d, 'id', None)} menu.id={menu.id} menu.name={getattr(menu, '名称', None)} menu.is_market_price={getattr(menu, 'is_market_price', None)} menu.時価={getattr(menu, '時価', None)} is_market_price={is_market_price} actual_price={actual_price}")
            else:
                app.logger.debug(f"[summary] item_id={getattr(d, 'id', None)} menu=None")
            
            if is_market_price and actual_price is None:
                # 時価商品で価格が未設定
                market_price_items.append({
                    "id": getattr(d, "id", None),
                    "name": menu.name if menu else "",
                    "qty": qty,
                })
                # 合計には含めない
                continue
            
            if is_market_price and actual_price is not None:
                # 時価商品で価格が設定済み
                unit_excl = int(actual_price)
            
            unit_tax  = math.floor(unit_excl * rate)
            unit_incl = unit_excl + unit_tax

            subtotal_excl += unit_excl * qty
            tax_total     += unit_tax  * qty
            total_incl    += unit_incl * qty

        # --- 既払（返金はマイナス） ---
        paid = 0
        if Pay is not None:
            col_amount = getattr(Pay, "amount", None) or getattr(Pay, "金額", None)
            if col_amount is not None:
                qp = s.query(func.coalesce(func.sum(col_amount), 0))\
                      .filter(getattr(Pay, "order_id") == order_id)
                if hasattr(Pay, "store_id") and sid_eff is not None:
                    qp = qp.filter(getattr(Pay, "store_id") == sid_eff)
                paid = int(qp.scalar() or 0)

        remaining = int(total_incl) - int(paid)

        # デバッグ（任意）
        if bool(current_app.config.get("DEBUG_TOTALS", False)):
            app.logger.debug(
                "[summary] order_id=%s sid_req=%s sid_hdr=%s sid_eff=%s items=%s subtotal=%s tax=%s total=%s paid=%s remaining=%s",
                order_id, sid_req, sid_hdr, sid_eff, len(items),
                subtotal_excl, tax_total, total_incl, paid, remaining
            )

        return jsonify({
            "ok": True,
            # 正規キー
            "subtotal": int(subtotal_excl),
            "tax": int(tax_total),
            "total": int(total_incl),
            "paid": int(paid),
            "remaining": int(remaining),
            "status": getattr(h, "status", ""),
            "market_price_items": market_price_items,  # 時価商品リスト
            # 日本語キー（互換）
            "小計": int(subtotal_excl),
            "税額": int(tax_total),
            "合計": int(total_incl),
            "既払": int(paid),
            "残額": int(remaining),
            # 旧フロント互換エイリアス
            "total_amount": int(total_incl),
            "grand_total": int(total_incl),
            "totalIncl": int(total_incl),
            "sum_total": int(total_incl),
            "paid_amount": int(paid),
            "paid_total": int(paid),
            "amount_paid": int(paid),
            "amount_due": int(remaining),
            "due": int(remaining),
            "balance": int(remaining),
            "remaining_amount": int(remaining),
        })
    except Exception as e:
        s.rollback()
        app.logger.exception("[admin_order_summary] %s", e)
        return jsonify({"ok": False, "error": "internal error"}), 500
    finally:
        s.close()






# --- 時価商品の価格更新 -----------------------------
@app.route("/admin/order_item/<int:item_id>/set_price", methods=["POST"])
@require_store_admin
def set_market_price(item_id):
    """
    時価商品の実際の価格を設定する
    """
    app.logger.info("[set_market_price] START item_id=%s", item_id)
    data = request.get_json(force=True) or {}
    app.logger.info("[set_market_price] data=%s", data)
    price = data.get("price")
    price_mode = data.get("mode", data.get("price_mode", "excl"))  # "excl" or "incl"
    app.logger.info("[set_market_price] price=%s, price_mode=%s", price, price_mode)
    
    if price is None or not isinstance(price, (int, float)) or price < 0:
        return jsonify({"ok": False, "error": "invalid price"}), 400
    
    if price_mode not in ("excl", "incl"):
        return jsonify({"ok": False, "error": "invalid price_mode"}), 400
    
    s = SessionLocal()
    try:
        item = s.get(OrderItem, item_id)
        if not item:
            return jsonify({"ok": False, "error": "item not found"}), 404
        
        # 店舗スコープ検証
        sid = current_store_id()
        if hasattr(OrderItem, "store_id") and sid is not None:
            if getattr(item, "store_id", None) != sid:
                return jsonify({"ok": False, "error": "forbidden"}), 403
        
        # 時価商品かどうか確認
        menu = getattr(item, "menu", None)
        is_market_price = getattr(menu, "is_market_price", 0) if menu else 0
        
        # 価格が0円の商品も時価商品として扱う
        current_price = getattr(item, "unit_price", 0) or 0
        if not is_market_price and current_price != 0:
            return jsonify({"ok": False, "error": "not a market price item"}), 400
        
        # 税率を取得
        tax_rate = getattr(menu, "tax_rate", None) or getattr(menu, "税率", None) or 0.10
        app.logger.info("[set_market_price] tax_rate=%s", tax_rate)
        
        # 税込/税抜モードに応じて税抜価格を計算
        if price_mode == "incl":
            # 税込価格が入力された場合、税抜価格を逆算
            # 税込合計が入力した金額と一致するように、税抜価格を切り上げ
            import math
            actual_price_excl = math.ceil(price / (1 + tax_rate))
            app.logger.info("[set_market_price] price_mode=incl: input_price=%s -> actual_price_excl=%s (rounded up)", price, actual_price_excl)
        else:
            # 税抜価格が入力された場合、そのまま使用
            actual_price_excl = int(price)
            app.logger.info("[set_market_price] price_mode=excl: actual_price_excl=%s", actual_price_excl)
        
        # 実際価格を設定（Pythonの属性名を使用）
        app.logger.info("[set_market_price] Setting actual_price=%s for item_id=%s", actual_price_excl, item_id)
        item.actual_price = actual_price_excl
        s.commit()
        app.logger.info("[set_market_price] Successfully committed price for item_id=%s, actual_price=%s", item_id, item.actual_price)
        
        return jsonify({"ok": True})
    except Exception as e:
        s.rollback()
        app.logger.exception("[set_market_price] %s", e)
        return jsonify({"ok": False, "error": "internal error"}), 500
    finally:
        s.close()


# --- 分割会計（取消除外の残額を基準に検証） -----------------------------
@app.route("/admin/settle/<int:table_id>/pay", methods=["POST"])
@require_staff
def admin_settle_pay(table_id):
    data = request.get_json(force=True) or {}
    order_id = int(data.get("order_id") or 0)
    rows = data.get("payments") or []
    force = bool(data.get("force"))

    if not order_id or not isinstance(rows, list) or not rows:
        return jsonify({"ok": False, "error": "invalid payload"}), 400

    s = SessionLocal()
    try:
        # 伝票存在＆店舗チェック
        h = s.get(OrderHeader, order_id)
        if not h:
            return jsonify({"ok": False, "error": "order not found"}), 404
        sid = current_store_id()
        if hasattr(OrderHeader, "store_id") and sid is not None:
            if getattr(h, "store_id", None) != sid:
                return jsonify({"ok": False, "error": "forbidden"}), 403

        # 取消行を除外した金額で基準を作る
        fin = _order_financials_excluding_cancels(s, order_id)
        remaining_before = fin["remaining"]

        # 未提供がある場合のブロック（フロントから force で上書き可能）
        # 最新の pending を再カウント
        pending = 0
        try:
            items = s.query(OrderItem).filter(OrderItem.order_id == order_id).all()
            def _served_flag(it):
                if getattr(it, "served", None): return True
                if getattr(it, "is_served", None): return True
                if getattr(it, "served_at", None): return True
                raw = str(getattr(it, "status", "")).lower()
                return ("提供済" in raw) or ("served" in raw) or ("done" in raw)

            for it in items:
                if _is_item_cancelled(it):
                    continue
                if not _served_flag(it):
                    pending += 1
        except Exception:
            pending = 0

        if pending > 0 and not force:
            # UI 側で確認ダイアログ→force で再POST のフロー
            return jsonify({"ok": False, "need_force": True, "pending": pending})

        # 入力検証
        total_input = 0
        for r in rows:
            mid = int(r.get("method_id") or 0)
            amt = int(r.get("amount") or 0)
            if not mid or amt <= 0:
                return jsonify({"ok": False, "error": "invalid payment row"}), 400
            total_input += amt

        if total_input > remaining_before:
            return jsonify({"ok": False, "error": "amount exceeds remaining"}), 400

        # 支払方法が実在するか簡易チェック（存在すればOK）
        methods = {}
        try:
            mids = {int(r.get("method_id")) for r in rows if r.get("method_id")}
            for m in s.query(PaymentMethod).filter(PaymentMethod.id.in_(mids)).all():
                methods[m.id] = m
        except Exception:
            pass  # PaymentMethod テーブルが無い環境でも動くように

        # 支払レコードを作成
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        for r in rows:
            mid = int(r.get("method_id") or 0)
            amt = int(r.get("amount") or 0)
            pay = PaymentRecord(
                order_id=order_id,
                amount=amt
            )
            # 任意項目（存在すれば設定）
            if hasattr(PaymentRecord, "method_id"):
                setattr(pay, "method_id", mid)
            if hasattr(PaymentRecord, "paid_at"):
                setattr(pay, "paid_at", now.strftime("%Y-%m-%d %H:%M:%S"))
            if hasattr(PaymentRecord, "store_id") and sid is not None:
                setattr(pay, "store_id", sid)
            s.add(pay)

        s.flush()

        # 登録後の残額を再計算（取消除外版）
        fin_after = _order_financials_excluding_cancels(s, order_id)
        s.commit()

        return jsonify({
            "ok": True,
            "summary": {
                "total": fin_after["total"],
                "paid": fin_after["paid"],
                "remaining": fin_after["remaining"],
            }
        })
    except Exception as e:
        s.rollback()
        app.logger.exception("[admin_settle_pay] %s", e)
        return jsonify({"ok": False, "error": "internal error"}), 500
    finally:
        s.close()



# --- [KDS API] 注文明細ステータス更新（count/部分取消し対応） ---
@app.route("/api/order_item/<int:item_id>/status", methods=["POST"])
@require_any
def api_order_item_status(item_id: int):
    """
    JSON: { "status": "調理中|提供済|取消", "count": <int>=1 }
      - 進捗は T_明細進捗 の qty_* を「個数移動」で表現
      - 取消は進捗移動に加えて、監査用のマイナス行も T_注文明細 に追加
      - 提供済 + 取消 == 元数量 になったら、注文明細.status を「提供済」に自動確定
    """
    s = SessionLocal()
    try:
        j = request.get_json(force=True) or {}
        status = str(j.get("status") or "").strip()
        count  = int(j.get("count") or 1)
        if count <= 0:
            return jsonify(ok=False, error="count must be >= 1"), 400

        OrderItem, Menu = _models()
        it = s.get(OrderItem, item_id)
        if not it:
            return jsonify(ok=False, error="item not found"), 404

        qty_orig = int(_get_any(it, "qty", "数量", default=0))
        if qty_orig <= 0:
            return jsonify(ok=False, error="quantity is zero"), 400

        # 進捗エントリが無ければシード（qty_new = 元数量）
        progress_seed_if_needed(s, it)

        # 取消の場合は監査用負行を作るので税率の準備
        will_cancel = status in ["取消","ｷｬﾝｾﾙ","キャンセル","cancel","void","VOID","Cancel"]
        tax_rate = None
        if will_cancel:
            menu_id = _get_any(it, "menu_id", "メニューid", "商品id")
            menu = s.get(Menu, menu_id) if menu_id is not None else None
            tax_rate = _guess_tax_rate(src_item=it, menu=menu)

        # 進捗カウンタの移動（超過は ValueError で 400）
        if status not in ["調理中","cooking","提供済","served","取消","ｷｬﾝｾﾙ","キャンセル","cancel","void","VOID","Cancel"]:
            return jsonify(ok=False, error="invalid status"), 400

        try:
            p_after = progress_move(s, it, status, count)
        except ValueError as e:
            s.rollback()
            return jsonify(ok=False, error=str(e)), 400

        neg_id = None
        # 取消は監査用マイナス行を追加（会計整合のため）
        if will_cancel:
            neg = OrderItem()
            _copy_if_exists(neg, it, [
                (["order_id","注文id","注文ID"], ["order_id","注文id","注文ID"]),
                (["menu_id","メニューid","商品id"], ["menu_id","メニューid","商品id"]),
                (["store_id","店舗ID"], ["store_id","店舗ID"]),
                (["tenant_id"], ["tenant_id"]),
                (["name","名称"], ["name","名称"]),
                (["unit_price","単価","税抜単価"], ["unit_price","単価","税抜単価"]),
                (["税込単価"], ["税込単価","price_incl"]),
            ])
            _set_first(neg, ["qty","数量"], -int(count))
            _set_first(neg, ["税率","tax_rate"], float(tax_rate if tax_rate is not None else 0.10))
            _set_first(neg, ["status","状態"], "取消")

            # 親リンク or メモに cancel_of を残す
            parent_set = False
            for name in ["parent_item_id","親明細ID","元明細ID"]:
                if hasattr(neg, name):
                    setattr(neg, name, item_id)
                    parent_set = True
                    break
            if not parent_set:
                memo_old = _get_any(neg, "memo","メモ","備考","備考欄", default="") or ""
                _set_first(neg, ["memo","メモ","備考","備考欄"], (memo_old + " ").strip() + f"cancel_of:{item_id}")

            now = datetime.utcnow()
            if hasattr(neg, "created_at"): neg.created_at = now
            if hasattr(neg, "updated_at"): neg.updated_at = now
            if hasattr(neg, "追加日時"):   setattr(neg, "追加日時", now)

            s.add(neg)
            s.flush()
            neg_id = getattr(neg, "id", None)

        # 自動確定：提供済 + 取消 == 元数量 → 注文明細.status を「提供済」
        finalized = progress_finalize_if_done(s, it)

        s.commit()
        mark_floor_changed()

        return jsonify({
            "ok": True,
            "progress": p_after,          # {"qty_new":..,"qty_cooking":..,"qty_served":..,"qty_canceled":..}
            "finalized": bool(finalized), # True なら注文明細.status は「提供済」に確定
            "negative_item_id": neg_id    # 取消時のみ
        })

    except Exception as e:
        s.rollback()
        current_app.logger.exception("api_order_item_status error")
        return jsonify(ok=False, error="internal error"), 500
    finally:
        s.close()




# --- [KDS API] 注文明細ステータス更新（互換エイリアス） ------------------------
@app.route("/kds/api/item/<int:item_id>/status", methods=["POST"])
@require_any
def api_order_item_status_alias(item_id: int):
    return api_order_item_status(item_id)



# =============================================================================
# メニュー管理（管理者）
# =============================================================================
# 既存で使っている前提のヘルパ/モデル：
#   SessionLocal, require_admin, current_store_id, validate_store_id, ensure_store_id_in_master
#   allowed_image, UPLOAD_DIR, now_str
#   fetch_categories_with_depth, ProductCategoryLink
#   Menu（ORM。M_メニューにマップ済み想定）
#   effective_tax_rate_from_form, get_price_input_mode,
#   normalize_price_for_storage, display_price_incl_from_excl
# =============================================================================


# --- 共通ヘルパ：カテゴリツリーを構築（管理画面用） -----------------------------
def build_category_tree_for_admin(s, sid=None):
    """
    {"root":[{id,name},...], "<cat_id>":[{id,name},...], ...} を返す。
    Category/parent_id が無い環境でもフォールバックで動作。
    """
    try:
        cats_q = s.query(Category)
        if sid is not None and hasattr(Category, "store_id"):
            cats_q = cats_q.filter(Category.store_id == sid)

        order1 = getattr(Category, "display_order", getattr(Category, "id"))
        cats = cats_q.order_by(order1, Category.name).all()

        def node(c):
            return {"id": int(c.id), "name": getattr(c, "name", getattr(c, "名称", str(c.id)))}

        bucket = {}
        for c in cats:
            pid = getattr(c, "parent_id", None)
            key = str(pid) if (pid not in (None, 0)) else "root"
            bucket.setdefault(key, []).append(node(c))

        tree = {"root": bucket.get("root", [])}
        for c in cats:
            tree[str(c.id)] = bucket.get(str(c.id), [])
        return tree

    except Exception:
        flat = fetch_categories_with_depth(s)
        tree = {
            "root": [
                {"id": int(c.id), "name": getattr(c, "name", getattr(c, "名称", str(c.id)))}
                for c in flat if int(getattr(c, "depth", 1)) == 1
            ]
        }
        for c in flat:
            tree[str(c.id)] = []
        return tree


# --- 画面：メニュー管理トップ ---------------------------------------------------
@app.route("/admin/menu/home", endpoint="admin_menu_home")
@require_admin
def admin_menu_home():
    return render_template("menu_home.html", title="メニュー管理")


# --- 画面：メニュー新規作成フォーム --------------------------------------------
@app.route("/admin/menu/new_form", endpoint="admin_menu_new_form")
@require_admin
def admin_menu_new_form():
    s = SessionLocal()
    try:
        cats = fetch_categories_with_depth(s)
        mode = get_price_input_mode()  # "incl" or "excl"
        return render_template(
            "menu_new.html",
            title="メニュー新規作成",
            cats=cats,
            price_input_mode=mode
        )
    finally:
        s.close()


# --- 共通ヘルパ：数値変換（int/float の安全化） ---------------------------------
def _to_int(v, default=0):
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default

def _to_float(v, default=None):
    try:
        if v is None:
            return default
        s = str(v).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


# --- 登録API：メニュー新規作成（M_メニューへの INSERT） --------------------------
@app.route("/admin/menu/new", methods=["POST"], endpoint="admin_menu_new")
@require_admin
def admin_menu_new():
    f = request.form
    s = SessionLocal()
    try:
        sid = current_store_id()
        if sid is None:
            return "店舗が選択されていません。ログインし直してください。", 400

        # 店舗ID マスター整合
        try:
            ok = validate_store_id(sid)
        except Exception:
            ok = False
        if not ok:
            try:
                ensure_store_id_in_master(f"store_{sid}", f"店舗{sid}")
                if not validate_store_id(sid):
                    return f"店舗ID {sid} の登録に失敗しました。", 500
            except Exception as e:
                app.logger.error("[menu_new] ensure master error: %s", e, exc_info=True)
                return f"店舗ID {sid} の登録に失敗しました。", 500

        # 画像アップロード or URL
        photo_url = (f.get("写真URL") or "").strip()
        file = request.files.get("写真ファイル")
        if file and file.filename:
            if not allowed_image(file.filename):
                return "対応していない画像形式です（jpg/jpeg/png/gif/webp）", 400
            filename_org = secure_filename(file.filename)
            ext = filename_org.rsplit(".", 1)[1].lower() if "." in filename_org else "png"
            ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"{ts}_{os.urandom(4).hex()}.{ext}"
            save_path = os.path.join(UPLOAD_DIR, filename)
            file.save(save_path)
            photo_url = url_for("uploaded_file", filename=filename)

        # カテゴリ必須
        cat_ids = [cid for cid in f.getlist("cat_id[]") if cid]
        if not cat_ids:
            return "カテゴリは必ず1つ以上選択してください。", 400

        # 価格・税率の正規化
        eff_rate = effective_tax_rate_from_form(f)     # 例: 0.10
        mode = get_price_input_mode()                  # "incl" / "excl"
        raw_price = _to_int(f.get("価格"), 0)

        # 0円はOK／負数はエラー
        if raw_price < 0:
            return "価格は0以上で入力してください。", 400

        result = normalize_price_for_storage(raw_price, mode, eff_rate)
        if isinstance(result, tuple):
            price_excl, price_incl = result
        else:
            price_excl = int(result)
            price_incl = display_price_incl_from_excl(price_excl, eff_rate)

        price_excl = _to_int(price_excl, 0)
        price_incl = _to_int(price_incl, price_excl)
        now = now_str()

        # 時価フラグ
        is_market_price = 1 if f.get("時価") == "1" else 0

        # INSERT
        insert_sql = text("""
            INSERT INTO "M_メニュー"
                ("名称","価格","写真URL","説明","提供可否","税率","時価","表示順",
                 "作成日時","更新日時","tenant_id","店舗ID","税込価格")
            VALUES
                (:name,:price:photo,:desc,:avail,:rate,:is_market_price,:disp,:created,:updated,:tenant_id,:store_id,:price_incl)
        """.replace(":price:photo", ':price,:photo'))  # ← 文字列整形の安全策
        s.execute(insert_sql, {
            "name": (f.get("名称") or "").strip(),
            "price": price_excl,
            "photo": (photo_url or None),
            "desc": (f.get("説明") or None),
            "avail": 1,
            "rate": float(eff_rate),
            "is_market_price": is_market_price,
            "disp": _to_int(f.get("表示順"), 0),
            "created": now,
            "updated": now,
            "tenant_id": session.get("tenant_id"),
            "store_id": sid,
            "price_incl": price_incl,
        })
        s.flush()

        # 新規ID（SQLite）
        new_id = s.execute(text("SELECT last_insert_rowid()")).scalar()

        # カテゴリリンク作成
        cat_orders = f.getlist("cat_order[]")
        cat_taxes  = f.getlist("cat_tax[]") if "cat_tax[]" in f else []

        for idx, cid in enumerate(f.getlist("cat_id[]")):
            cid_str = (cid or "").strip()
            if not cid_str:
                continue

            disp = _to_int(cat_orders[idx] if idx < len(cat_orders) else None, 0)
            tax_rate = None
            if idx < len(cat_taxes):
                tax_rate = _to_float(cat_taxes[idx], default=None)

            try:
                link = ProductCategoryLink(
                    product_id=new_id,
                    category_id=_to_int(cid_str, 0),
                    display_order=disp,
                    tax_rate=tax_rate,
                    assigned_at=now,
                    store_id=sid if hasattr(ProductCategoryLink, "store_id") else None
                )
                s.add(link)
            except Exception:
                pass

        s.commit()
        flash("メニューを登録しました。")
        return redirect(url_for("admin_menu_list"))

    except Exception as e:
        s.rollback()
        app.logger.error("[admin_menu_new] insert failed: %s", e, exc_info=True)
        return f"登録に失敗しました: {e}", 500
    finally:
        s.close()


# --- 画面：作成済みメニュー一覧 -------------------------------------------------
@app.route("/admin/menu/list", endpoint="admin_menu_list")
@require_admin
def admin_menu_list():
    s = SessionLocal()
    try:
        sid = current_store_id()
        category_tree = build_category_tree_for_admin(s, sid)
        return render_template(
            "menu_list.html",
            title="作成済みメニュー",
            category_tree=category_tree
        )
    finally:
        s.close()


# --- API：カテゴリ別メニュー一覧（管理画面・Ajax） ------------------------------
@app.route("/api/admin/menus/by_category/<int:category_id>")
@require_admin
def api_admin_menus_by_category(category_id: int):
    s = SessionLocal()
    try:
        sid = current_store_id()
        from sqlalchemy.orm import aliased
        L = aliased(ProductCategoryLink)

        q = (
            s.query(Menu)
             .join(L, L.product_id == Menu.id)
             .filter(L.category_id == category_id)
        )
        if sid is not None and hasattr(Menu, "store_id"):
            q = q.filter(Menu.store_id == sid)

        # 削除済みは表示対象外
        if hasattr(Menu, "is_deleted"):
            q = q.filter(Menu.is_deleted == 0)

        rows = q.order_by(
            L.display_order.asc(),
            Menu.display_order.asc(),
            Menu.name.asc()
        ).all()

        out = []
        for m in rows:
            eff_rate  = resolve_effective_tax_rate_for_menu(s, m.id, m.tax_rate)
            price_excl = int(m.price)
            price_incl = display_price_incl_from_excl(price_excl, eff_rate)
            out.append({
                "id": m.id,
                "name": m.name,
                "description": m.description or "",
                "photo_url": m.photo_url,
                "price_excl": price_excl,
                "price_incl": price_incl,
                "available": int(m.available or 0),
                "is_market_price": bool(getattr(m, "is_market_price", 0)),
            })
        return jsonify(ok=True, menus=out)
    except Exception as e:
        app.logger.error("[api_admin_menus_by_category] %s", e, exc_info=True)
        return jsonify(ok=False, error=str(e)), 500
    finally:
        s.close()


# --- ユーティリティ：カテゴリの子孫ID収集（含む/含まないは呼び出し側で指定） ---
def _collect_descendant_category_ids(s, root_cid: int, include_root: bool = True) -> list[int]:
    from sqlalchemy import or_
    # まず全カテゴリを取得（店舗スコープ考慮）
    sid = current_store_id()
    q = s.query(Category)
    if hasattr(Category, "store_id") and sid is not None:
        q = q.filter(Category.store_id == sid)
    cats = q.all()

    children_map = {}
    for c in cats:
        children_map.setdefault(c.parent_id, []).append(c.id)

    result = []
    queue = [root_cid]
    if include_root:
        result.append(root_cid)
    seen = set(result)

    while queue:
        pid = queue.pop(0)
        for child_id in children_map.get(pid, []):
            if child_id in seen:
                continue
            seen.add(child_id)
            result.append(child_id)
            queue.append(child_id)

    return result

# --- API：カテゴリ一括 提供開始/停止 ---
@app.post("/api/admin/category/<int:cid>/bulk_available")
@require_admin
def api_admin_category_bulk_available(cid: int):
    """
    JSON Body:
      { "available": 0|1, "recursive": 0|1 }
    指定（＋必要なら子孫）カテゴリに属する '削除されていない' メニューの available を一括更新。
    """
    from sqlalchemy.orm import aliased
    L = aliased(ProductCategoryLink)
    s = SessionLocal()
    try:
        data = request.get_json(silent=True) or {}
        to_available = 1 if str(data.get("available", 1)) in ("1", "true", "True") else 0
        recursive = str(data.get("recursive", 1)) in ("1", "true", "True")

        # 対象カテゴリID集合
        cat_ids = _collect_descendant_category_ids(s, cid, include_root=True) if recursive else [cid]

        sid = current_store_id()

        # 対象メニュー抽出（カテゴリリンク経由）
        q = (
            s.query(Menu)
             .join(L, L.product_id == Menu.id)
             .filter(L.category_id.in_(cat_ids))
        )
        if sid is not None and hasattr(Menu, "store_id"):
            q = q.filter(Menu.store_id == sid)
        if hasattr(Menu, "is_deleted"):
            q = q.filter(Menu.is_deleted == 0)

        targets = q.all()

        # 一括更新
        now = now_str()
        for m in targets:
            m.available = to_available
            if hasattr(m, "updated_at"):
                m.updated_at = now

        s.commit()
        return jsonify(ok=True, count=len(targets), available=to_available, category_ids=cat_ids)
    except Exception as e:
        s.rollback()
        app.logger.error("[bulk_available] %s", e, exc_info=True)
        return jsonify(ok=False, error=str(e)), 500
    finally:
        s.close()


# --- API：カテゴリ一括 論理削除 ---
@app.post("/api/admin/category/<int:cid>/bulk_delete")
@require_admin
def api_admin_category_bulk_delete(cid: int):
    """
    JSON Body:
      { "recursive": 0|1 }
    指定（＋必要なら子孫）カテゴリに属するメニューを一括 '論理削除'。
    既存の単体削除のポリシー（is_deleted=1, available=0, deleted_at, updated_at）に合わせる。
    """
    from sqlalchemy.orm import aliased
    L = aliased(ProductCategoryLink)
    s = SessionLocal()
    try:
        data = request.get_json(silent=True) or {}
        recursive = str(data.get("recursive", 1)) in ("1", "true", "True")

        cat_ids = _collect_descendant_category_ids(s, cid, include_root=True) if recursive else [cid]
        sid = current_store_id()

        q = (
            s.query(Menu)
             .join(L, L.product_id == Menu.id)
             .filter(L.category_id.in_(cat_ids))
        )
        if sid is not None and hasattr(Menu, "store_id"):
            q = q.filter(Menu.store_id == sid)

        # 既に削除済みはスキップ
        if hasattr(Menu, "is_deleted"):
            q = q.filter(Menu.is_deleted == 0)

        menus = q.all()

        # 単体削除の仕様に合わせてフラグ更新（既存の単体削除の設計はこちら）:
        # is_deleted=1, available=0, deleted_at=UTC, updated_at=now_str()
        # filecite: 単体削除の仕様
        from datetime import datetime as _dt
        now_s = now_str()
        for m in menus:
            m.is_deleted = 1
            m.available = 0
            if hasattr(m, "deleted_at"):
                m.deleted_at = _dt.utcnow()
            if hasattr(m, "updated_at"):
                m.updated_at = now_s

        s.commit()
        return jsonify(ok=True, count=len(menus), category_ids=cat_ids)
    except Exception as e:
        s.rollback()
        app.logger.error("[bulk_delete] %s", e, exc_info=True)
        return jsonify(ok=False, error=str(e)), 500
    finally:
        s.close()



# --- API：メニュー提供可否トグル（即時反映） -----------------------------------
@app.route("/api/admin/menu/<int:mid>/toggle", methods=["POST"])
@require_admin
def api_admin_menu_toggle(mid: int):
    s = SessionLocal()
    try:
        m = s.get(Menu, mid)
        if not m:
            return jsonify(ok=False, error="not found"), 404

        sid = current_store_id()
        if sid is not None and hasattr(m, "store_id") and m.store_id != sid:
            return jsonify(ok=False, error="forbidden"), 403

        m.available = 0 if (m.available == 1 or m.available is True) else 1
        m.updated_at = now_str()
        s.commit()

        return jsonify(ok=True, available=int(m.available))
    except Exception as e:
        s.rollback()
        app.logger.error("[api_admin_menu_toggle] %s", e, exc_info=True)
        return jsonify(ok=False, error=str(e)), 500
    finally:
        s.close()


# --- デバッグ：ログレベル設定（任意） -------------------------------------------
try:
    app.logger.setLevel(logging.DEBUG)
except Exception:
    pass


# --- デバッグヘルパ：メニュー編集デバッグ ON/OFF 判定 --------------------------
def _is_debug_mode():
    """URLに ?debug=1 か、POSTに __debug=1 があるとデバッグON。session固定も可。"""
    if request.args.get("debug") in ("1", "true", "on"):
        session["debug_menu_edit"] = True
    if request.form.get("__debug") in ("1", "true", "on"):
        session["debug_menu_edit"] = True
    return bool(session.get("debug_menu_edit"))


# --- デバッグヘルパ：ログ/画面用の情報蓄積 -------------------------------------
def _dbg_push(debug_list, label, **data):
    """デバッグ情報を配列に蓄積＆ログにも出す"""
    row = {"label": label, "data": data}
    debug_list.append(row)
    try:
        app.logger.debug("[menu_edit][%s] %s", label, json.dumps(data, ensure_ascii=False, default=str))
    except Exception:
        app.logger.debug("[menu_edit][%s] %r", label, data)


# --- デバッグヘルパ：デバッグパネル付きでHTMLを返す ----------------------------
def _render_debug_page(title, body_html, debug_info, status=200):
    """本来のHTMLにデバッグパネルを合成して返す（テンプレを触らずに可視化）"""
    return render_template_string("""
{{ body|safe }}
<hr>
<div style="font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, 'Liberation Mono', monospace; background:#0b1220; color:#e5e7eb; padding:16px; border-radius:8px;">
  <h3 style="margin-top:0;">🛠 Debug Panel: {{ title }}</h3>
  {% for row in debug_info %}
    <div style="margin:10px 0; padding:10px; background:#111827; border:1px solid #374151; border-radius:6px;">
      <div style="color:#93c5fd; font-weight:bold;">{{ row.label }}</div>
      <pre style="margin:6px 0 0; white-space:pre-wrap;">{{ row.data | tojson(indent=2, ensure_ascii=False) }}</pre>
    </div>
  {% endfor %}
</div>
""", title=title, body=body_html, debug_info=debug_info), status


# --- 画面/API：メニュー編集（GET表示／POST更新） -------------------------------
@app.route("/admin/menu/<int:mid>/edit", methods=["GET", "POST"], endpoint="admin_menu_edit")
@require_admin
def admin_menu_edit(mid):
    import os, traceback
    from datetime import datetime, timezone
    from sqlalchemy import text, inspect

    s = SessionLocal()
    debug_info = []
    try:
        m = s.get(Menu, mid)
        if not m:
            abort(404)

        # 店舗スコープ確認
        sid = current_store_id()
        if sid is not None and hasattr(m, "store_id") and m.store_id != sid:
            abort(403, "他の店舗のメニューは編集できません")

        _dbg_push(debug_info, "init", mid=mid, sid=sid, has_store_id=hasattr(m, "store_id"))

        # カテゴリ候補
        cats_flat = fetch_categories_with_depth(s)
        _dbg_push(debug_info, "cats_loaded", count=len(cats_flat))

        # ---------- POST: 更新 ----------
        if request.method == "POST":
            f = request.form
            _dbg_push(debug_info, "post_form_keys", keys=list(f.keys()))
            _dbg_push(debug_info, "post_files", files=list(request.files.keys()))

            # (A) 店舗IDマスター整合
            ok = True
            try:
                ok = validate_store_id(sid) if sid is not None else False
            except Exception as e:
                ok = False
                _dbg_push(debug_info, "validate_store_id_error", error=str(e))
            if not ok:
                try:
                    ensure_store_id_in_master(f"store_{sid}", f"店舗{sid}")
                    if not validate_store_id(sid):
                        _dbg_push(debug_info, "ensure_store_id_failed", sid=sid)
                        return ("店舗ID {sid} の登録に失敗しました。", 500)
                except Exception as e:
                    app.logger.error(f"[menu_edit] ensure master error: {e}", exc_info=True)
                    _dbg_push(debug_info, "ensure_store_id_exception", error=str(e))
                    return (f"店舗ID {sid} の登録に失敗しました。", 500)

            # (B) カテゴリ必須
            cat_ids = [cid for cid in f.getlist("cat_id[]") if cid]
            _dbg_push(debug_info, "cat_ids_parsed", cat_ids=cat_ids)
            if not cat_ids:
                msg = "カテゴリは必ず1つ以上選択してください。"
                if _is_debug_mode():
                    return _render_debug_page("Validation Error", msg, debug_info, status=400)
                return (msg, 400)

            # (C) 画像アップロード（任意）／URL
            photo_url = (f.get("写真URL") or "").strip()
            file = request.files.get("写真ファイル")
            if file and file.filename:
                if not allowed_image(file.filename):
                    if _is_debug_mode():
                        _dbg_push(debug_info, "image_reject", filename=file.filename)
                        return _render_debug_page("Image Error", "対応していない画像形式です（jpg/jpeg/png/gif/webp）", debug_info, status=400)
                    return ("対応していない画像形式です（jpg/jpeg/png/gif/webp）", 400)
                filename_org = secure_filename(file.filename)
                ext = filename_org.rsplit(".", 1)[1].lower() if "." in filename_org else "jpg"
                tsf = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                filename = f"{tsf}_{os.urandom(4).hex()}.{ext}"
                save_path = os.path.join(UPLOAD_DIR, filename)
                file.save(save_path)
                photo_url = url_for("uploaded_file", filename=filename)
                _dbg_push(debug_info, "image_saved", filename=filename, path=save_path, photo_url=photo_url)
            else:
                if photo_url == "":
                    photo_url = m.photo_url

            # (D) 価格・税率の正規化
            try:
                eff_rate = effective_tax_rate_from_form(f)   # 0.10 など
                mode = get_price_input_mode()                # "incl" or "excl"
                raw_price = _to_int(f.get("価格"), 0)
                if raw_price < 0:
                    msg = "価格は0以上で入力してください。"
                    if _is_debug_mode():
                        _dbg_push(debug_info, "price_negative", raw_price=raw_price)
                        return _render_debug_page("Price Validation", msg, debug_info, status=400)
                    return (msg, 400)

                _dbg_push(debug_info, "price_raw", mode=mode, eff_rate=eff_rate, raw_price=raw_price)

                result = normalize_price_for_storage(raw_price, mode, eff_rate)
                if isinstance(result, tuple):
                    price_excl, price_incl = result
                else:
                    price_excl = _to_int(result, 0)
                    price_incl = _to_int(display_price_incl_from_excl(price_excl, eff_rate), price_excl)

                price_excl = _to_int(price_excl, 0)
                price_incl = _to_int(price_incl, price_excl)
                _dbg_push(debug_info, "price_normalized", price_excl=price_excl, price_incl=price_incl)
            except Exception as e:
                app.logger.error("[menu_edit] price/tax normalize failed: %s", e, exc_info=True)
                _dbg_push(debug_info, "price_normalize_exception", error=str(e), traceback=traceback.format_exc())
                if _is_debug_mode():
                    return _render_debug_page("Price/Tax Normalize Error", "価格または税率の入力が不正です。", debug_info, status=400)
                return ("価格または税率の入力が不正です。", 400)

            # (E) 本体更新＋日本語カラム強制UPDATE
            m.name = (f.get("名称") or "").strip()
            m.price = price_excl
            if hasattr(m, "price_incl"):
                m.price_incl = price_incl
            if photo_url:
                m.photo_url = photo_url
            m.description   = f.get("説明")
            m.tax_rate      = float(eff_rate)
            m.display_order = _to_int(f.get("表示順"), 0)
            # 時価商品フラグ
            if hasattr(m, "is_market_price"):
                m.is_market_price = 1 if f.get("時価") else 0
            if hasattr(m, "store_id") and sid is not None:
                m.store_id = sid

            ts = datetime.now(timezone.utc).isoformat(timespec="microseconds")
            m.updated_at = ts

            try:
                from sqlalchemy import inspect
                h = inspect(m).attrs
                def _hist_dict(attr_name: str):
                    try:
                        hist = getattr(h, attr_name).history
                        return {
                            "added":     list(hist.added or []),
                            "deleted":   list(hist.deleted or []),
                            "unchanged": list(hist.unchanged or []),
                        }
                    except Exception as e:
                        return {"error": str(e)}
                _dbg_push(debug_info, "dirty_hist",
                          price=_hist_dict("price"),
                          price_incl=_hist_dict("price_incl") if hasattr(m, "price_incl") else None)

                s.flush()
                _dbg_push(debug_info, "flush_ok",
                          menu_id=mid,
                          assigned_price_excl=int(m.price),
                          assigned_price_incl=int(getattr(m, "price_incl", price_incl)))

                res = s.execute(
                    text('UPDATE "M_メニュー" '
                         'SET "名称"=:nm, '
                         '    "価格"=:pe, '
                         '    "税込価格"=:pi, '
                         '    "説明"=:desc, '
                         '    "税率"=:tax, '
                         '    "表示順"=:disp, '
                         '    "写真URL"=:photo, '
                         '    "時価"=:is_mp, '
                         '    "更新日時"=:ts '
                         'WHERE "M_メニュー".id = :id'),
                    {
                        "nm": (f.get("名称") or "").strip(),
                        "pe": int(price_excl),
                        "pi": int(price_incl),
                        "desc": f.get("説明"),
                        "tax": float(eff_rate),
                        "disp": _to_int(f.get("表示順"), 0),
                        "photo": (photo_url or m.photo_url),
                        "is_mp": 1 if f.get("時価") else 0,
                        "ts": ts,
                        "id": mid,
                    }
                )
                _dbg_push(debug_info, "force_update_rowcount",
                          rowcount=res.rowcount, pe=price_excl, pi=price_incl, ts=ts)

                m = s.get(Menu, mid, populate_existing=True)
                _dbg_push(debug_info, "after_force_reload",
                          name=str(m.name),
                          pe=int(m.price),
                          pi=int(getattr(m, "price_incl", 0)),
                          updated_at=str(m.updated_at))

            except Exception as e:
                app.logger.error("[menu_edit] flush/force update failed: %s", e, exc_info=True)
                s.rollback()
                _dbg_push(debug_info, "flush_or_force_exception", error=str(e), traceback=traceback.format_exc())
                if _is_debug_mode():
                    return _render_debug_page("Flush/Force Error", f"更新失敗（保存時）: {e}", debug_info, status=500)
                return (f"更新失敗（保存時）: {e}", 500)

            # (F) カテゴリ付け替え
            q = s.query(ProductCategoryLink).filter(ProductCategoryLink.product_id == mid)
            if hasattr(ProductCategoryLink, "store_id") and sid is not None:
                q = q.filter(ProductCategoryLink.store_id == sid)
            q.delete()

            cat_orders = f.getlist("cat_order[]")
            cat_taxes  = f.getlist("cat_tax[]")
            _dbg_push(debug_info, "cat_lists_raw",
                      cat_id=f.getlist("cat_id[]"), cat_order=cat_orders, cat_tax=cat_taxes)

            for idx, cid in enumerate(f.getlist("cat_id[]")):
                cid_str = (cid or "").strip()
                if not cid_str:
                    continue

                disp = _to_int(cat_orders[idx] if idx < len(cat_orders) else None, 0)
                tax_rate = None
                if idx < len(cat_taxes):
                    tax_rate = _to_float(cat_taxes[idx], default=None)

                try:
                    link = ProductCategoryLink(
                        product_id=mid,
                        category_id=_to_int(cid_str, 0),
                        display_order=disp,
                        tax_rate=tax_rate,
                        assigned_at=now_str(),
                        store_id=sid if hasattr(ProductCategoryLink, "store_id") else None
                    )
                    s.add(link)
                except Exception as e:
                    _dbg_push(debug_info, "link_add_exception", idx=idx, error=str(e))

                _dbg_push(debug_info, "link_added",
                          idx=idx, category_id=_to_int(cid_str, 0), display_order=disp, tax_rate=tax_rate)

            # (G) コミット
            try:
                s.commit()
                _dbg_push(debug_info, "commit_ok", menu_id=mid)
            except Exception as e:
                app.logger.error("[menu_edit] commit failed: %s", e, exc_info=True)
                s.rollback()
                _dbg_push(debug_info, "commit_exception", error=str(e), traceback=traceback.format_exc())
                if _is_debug_mode():
                    return _render_debug_page("Commit Error", f"更新失敗（コミット時）: {e}", debug_info, status=500)
                return (f"更新失敗（コミット時）: {e}", 500)

            if _is_debug_mode():
                body = "<p>更新が完了しました（デバッグONのためリダイレクトを停止）。</p>"
                body += f'<p><a href="{url_for("admin_menu_list")}">メニュー一覧へ戻る</a></p>'
                return _render_debug_page("POST Success", body, debug_info, status=200)

            return redirect(url_for("admin_menu_list"))

        # ---------- GET: 初期表示 ----------
        links = (s.query(ProductCategoryLink)
                 .filter(ProductCategoryLink.product_id == mid)
                 .order_by(ProductCategoryLink.display_order.asc(),
                           ProductCategoryLink.category_id.asc())
                 .all())
        _dbg_push(debug_info, "links_loaded", count=len(links))

        eff_rate_for_view = resolve_effective_tax_rate_for_menu(s, mid, m.tax_rate)
        mode = get_price_input_mode()

        def ensure_price_incl_for(menu_obj, rate):
            try:
                v = getattr(menu_obj, "price_incl")
                if v is not None:
                    return int(v)
            except Exception:
                pass
            return display_price_incl_from_excl(int(menu_obj.price), rate)

        initial_price = ensure_price_incl_for(m, eff_rate_for_view) if mode == "incl" else int(m.price)
        _dbg_push(debug_info, "get_ready", eff_rate_for_view=eff_rate_for_view, mode=mode, initial_price=initial_price)

        body = render_template(
            "menu_edit.html",
            title="メニュー編集",
            m={
                "名称": m.name,
                "価格": int(m.price),
                "税込価格": ensure_price_incl_for(m, eff_rate_for_view),
                "写真URL": m.photo_url,
                "説明": m.description,
                "税率": m.tax_rate,
                "表示順": m.display_order,
                "時価": getattr(m, "is_market_price", 0),
            },
            cats=cats_flat,
            selected_links=[{
                "category_id": ln.category_id,
                "display_order": ln.display_order,
                "tax_rate": ln.tax_rate
            } for ln in links],
            price_input_mode=mode,
            initial_price=initial_price
        )

        if _is_debug_mode():
            return _render_debug_page("GET", body, debug_info, status=200)
        return body

    except Exception as e:
        app.logger.error("[menu_edit] unexpected error: %s", e, exc_info=True)
        _dbg_push(debug_info, "unexpected_exception", error=str(e), traceback=traceback.format_exc())
        if _is_debug_mode():
            return _render_debug_page("Unexpected Error", "処理中に例外が発生しました。", debug_info, status=500)
        raise
    finally:
        s.close()


# --- デバッグ：DB接続情報ダンプ -------------------------------------------------
@app.route("/__debug/dbinfo")
@require_admin
def __debug_dbinfo():
    import os
    from flask import abort
    if os.getenv("ENABLE_DEV_TOOLS") != "1":
        abort(404)
    u = engine.url
    sqlite_abspath = None
    if u.drivername.startswith("sqlite"):
        sqlite_abspath = os.path.abspath(u.database) if u.database else None
    return jsonify({
        "driver": u.drivername,
        "database": u.database,
        "sqlite_abspath": sqlite_abspath,
        "cwd": os.getcwd(),
        "DATABASE_URL": str(u),
    })


# --- デバッグ：メニュー個別ダンプ ----------------------------------------------
@app.route("/__debug/menu/<int:mid>")
@require_admin
def __debug_menu(mid):
    import os
    from flask import abort
    if os.getenv("ENABLE_DEV_TOOLS") != "1":
        abort(404)
    s = SessionLocal()
    try:
        m = s.get(Menu, mid)
        if not m:
            abort(404)
        links = (s.query(ProductCategoryLink)
                   .filter_by(product_id=mid)
                   .order_by(ProductCategoryLink.display_order.asc())
                   .all())
        try:
            tr = float(getattr(m, "tax_rate", 0) or 0)
        except Exception:
            tr = 0.0
        disp_incl = getattr(m, "price_incl", None)
        if disp_incl is None:
            disp_incl = display_price_incl_from_excl(int(m.price), tr)

        return jsonify({
            "menu": {
                "id": m.id,
                "name": m.name,
                "price_excl": int(m.price),
                "price_incl_col": getattr(m, "price_incl", None),
                "price_incl_display": int(disp_incl),
                "tax_rate": tr,
                "display_order": getattr(m, "display_order", None),
                "store_id": getattr(m, "store_id", None),
                "updated_at": getattr(m, "updated_at", None),
            },
            "links": [{
                "category_id": l.category_id,
                "display_order": l.display_order,
                "tax_rate": l.tax_rate,
                "store_id": getattr(l, "store_id", None),
            } for l in links]
        })
    finally:
        s.close()



# --- デバッグ：メニュー一覧ダンプ ----------------------------------------------
@app.route("/__debug/menu_list")
@require_admin
def __debug_menu_list():
    import os
    from flask import abort
    if os.getenv("ENABLE_DEV_TOOLS") != "1":
        abort(404)
    s = SessionLocal()
    try:
        sid = current_store_id()
        q = s.query(Menu)
        if hasattr(Menu, "store_id") and sid is not None:
            q = q.filter(Menu.store_id == sid)
        items = (q.order_by(Menu.display_order.asc(), Menu.id.desc()).all())

        out = []
        for m in items:
            try:
                tr = float(getattr(m, "tax_rate", 0) or 0)
            except Exception:
                tr = 0.0
            disp_incl = getattr(m, "price_incl", None)
            if disp_incl is None:
                disp_incl = display_price_incl_from_excl(int(m.price), tr)
            out.append({
                "id": m.id,
                "name": m.name,
                "price_excl": int(m.price),
                "price_incl_col": getattr(m, "price_incl", None),
                "price_incl_display": int(disp_incl),
                "tax_rate": tr,
                "display_order": getattr(m, "display_order", None),
                "store_id": getattr(m, "store_id", None),
                "updated_at": getattr(m, "updated_at", None),
            })
        return jsonify({"sid": sid, "count": len(out), "items": out})
    finally:
        s.close()


# --- 削除API：メニュー削除（論理削除へ変更） ------------------------
@app.route("/admin/menu/<int:mid>/delete", methods=["POST"], endpoint="admin_menu_delete")
@require_admin
def admin_menu_delete(mid: int):
    """
    メニュー削除（論理削除）:
      - 店舗スコープ（current_store_id）を満たすレコードだけ対象
      - 物理削除は行わず、is_deleted=1, available=0, deleted_at=現在日時 を設定
      - 例外時はロールバックし、エラーメッセージを返す
    """
    s = SessionLocal()
    debug_info = []
    try:
        m = s.get(Menu, mid)
        if not m:
            abort(404)

        # 店舗スコープ確認
        sid = current_store_id()
        if sid is not None and hasattr(m, "store_id") and m.store_id != sid:
            abort(403, "他の店舗のメニューは削除できません")

        _dbg_push(debug_info, "init", mid=mid, sid=sid, has_store_id=hasattr(m, "store_id"))

        # 既に削除済みなら何もしない
        if getattr(m, "is_deleted", 0):
            if _is_debug_mode():
                body = "<p>既に削除済みです。</p>"
                body += f'<p><a href="{url_for("admin_menu_list")}">メニュー一覧へ戻る</a></p>'
                return _render_debug_page("Already Deleted", body, debug_info, status=200)
            return redirect(url_for("admin_menu_list"))

        try:
            # 論理削除フラグを立て、提供停止と削除日時を更新
            m.is_deleted = 1
            m.available = 0
            m.deleted_at = datetime.utcnow()
            m.updated_at = now_str()
            s.commit()
            _dbg_push(debug_info, "delete_logical_ok", menu_id=mid)
        except Exception as e:
            s.rollback()
            app.logger.error("[menu_delete] logical delete failed: %s", e, exc_info=True)
            _dbg_push(debug_info, "logical_delete_exception", error=str(e), traceback=traceback.format_exc())
            if _is_debug_mode():
                return _render_debug_page("Delete Error", f"メニューの削除に失敗: {e}", debug_info, status=500)
            return (f"メニューの削除に失敗しました。", 500)

        if _is_debug_mode():
            body = "<p>削除が完了しました（デバッグONのためリダイレクトを停止）。</p>"
            body += f'<p><a href="{url_for("admin_menu_list")}">メニュー一覧へ戻る</a></p>'
            return _render_debug_page("DELETE Success", body, debug_info, status=200)

        return redirect(url_for("admin_menu_list"))

    except Exception as e:
        app.logger.error("[menu_delete] unexpected error: %s", e, exc_info=True)
        _dbg_push(debug_info, "unexpected_exception", error=str(e), traceback=traceback.format_exc())
        if _is_debug_mode():
            return _render_debug_page("Unexpected Error", "削除処理中に例外が発生しました。", debug_info, status=500)
        raise
    finally:
        s.close()

# --- 復活API：削除済みメニューの復元（提供状態は復元しない） ----------------------
@app.post("/admin/menu/<int:mid>/restore", endpoint="admin_menu_restore")
@require_admin
def admin_menu_restore(mid: int):
    """
    削除済みメニューの復元。
    is_deleted フラグと deleted_at をクリアし、available は変更しません（0 のまま）。
    提供開始にしたい場合は /admin/menu/<id>/restore_and_enable を使用します。
    """
    s = SessionLocal()
    try:
        m = s.get(Menu, mid)
        if not m:
            flash("メニューが見つかりません。", "error")
            return redirect(url_for("admin_menu_list_deleted"))

        # 店舗スコープ確認
        sid = current_store_id()
        if sid is not None and hasattr(m, "store_id") and m.store_id != sid:
            flash("他の店舗のメニューは操作できません。", "error")
            return redirect(url_for("admin_menu_list_deleted"))

        if not getattr(m, "is_deleted", 0):
            # 既に削除フラグが立っていない場合は一覧に戻る
            flash("このメニューは削除されていません。", "info")
            return redirect(url_for("admin_menu_list_deleted"))

        m.is_deleted = 0
        m.deleted_at = None
        # available は変更しない（復活後も停止中）。必要に応じて手動で提供開始してください。
        m.updated_at = now_str()
        s.commit()
        flash("メニューを復元しました。", "success")
        return redirect(url_for("admin_menu_list_deleted"))
    except Exception as e:
        s.rollback()
        current_app.logger.error("[menu_restore] failed: %s", e, exc_info=True)
        flash("復元に失敗しました。", "error")
        return redirect(url_for("admin_menu_list_deleted"))
    finally:
        s.close()

# --- 復活API：削除済みメニューの復元＋提供開始 ---------------------------
@app.post("/admin/menu/<int:mid>/restore_and_enable", endpoint="admin_menu_restore_and_enable")
@require_admin
def admin_menu_restore_and_enable(mid: int):
    """
    削除済みメニューの復元と同時に提供開始にする。
    is_deleted を 0 に戻し、deleted_at をクリアし、available=1 に設定します。
    """
    s = SessionLocal()
    try:
        m = s.get(Menu, mid)
        if not m:
            flash("メニューが見つかりません。", "error")
            return redirect(url_for("admin_menu_list_deleted"))

        # 店舗スコープ確認
        sid = current_store_id()
        if sid is not None and hasattr(m, "store_id") and m.store_id != sid:
            flash("他の店舗のメニューは操作できません。", "error")
            return redirect(url_for("admin_menu_list_deleted"))

        if not getattr(m, "is_deleted", 0):
            flash("このメニューは削除されていません。", "info")
            return redirect(url_for("admin_menu_list_deleted"))

        m.is_deleted = 0
        m.deleted_at = None
        m.available = 1
        m.updated_at = now_str()
        s.commit()
        flash("メニューを復元して提供開始しました。", "success")
        return redirect(url_for("admin_menu_list_deleted"))
    except Exception as e:
        s.rollback()
        current_app.logger.error("[menu_restore_and_enable] failed: %s", e, exc_info=True)
        flash("復元に失敗しました。", "error")
        return redirect(url_for("admin_menu_list_deleted"))
    finally:
        s.close()

# --- 画面：削除済みメニュー一覧 ---------------------------------------------
@app.get("/admin/menus/deleted", endpoint="admin_menu_list_deleted")
@require_admin
def admin_menu_list_deleted():
    """
    削除済みメニュー一覧を表示する。
    通常のメニュー一覧には表示されない削除済みメニューを確認・復元するためのページです。
    """
    s = SessionLocal()
    try:
        sid = current_store_id()
        # 削除済みメニューを取得（店舗スコープがあれば適用）
        q = s.query(Menu).filter(Menu.is_deleted == 1)
        if sid is not None and hasattr(Menu, "store_id"):
            q = q.filter(Menu.store_id == sid)
        menus = q.order_by(Menu.deleted_at.desc().nullslast()).all()
        return render_template("menu_list_deleted.html", title="削除済みメニュー一覧", menus=menus)
    finally:
        s.close()




# ---------------------------------------------------------------------
# 共通ユーティリティ（支払方法管理で利用）
# ---------------------------------------------------------------------

# --- 関数：指定テーブルに列が存在するか（SQLite PRAGMA） ------------------------
def has_db_column(table_name: str, column_name: str) -> bool:
    """テーブルに指定列が存在するか確認（DB方言対応）"""
    with engine.connect() as conn:
        dialect = conn.dialect.name if hasattr(conn, 'dialect') else 'sqlite'
        
        if dialect == 'sqlite':
            # SQLite: PRAGMA table_info を使用
            cols = [row[1] for row in conn.execute(
                text(f"PRAGMA table_info('{table_name}')")
            ).fetchall()]
        else:
            # PostgreSQL: information_schema.columns を使用
            cols = [
                r["column_name"] for r in conn.execute(
                    text("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = :table_name
                    """),
                    {"table_name": table_name}
                ).mappings().all()
            ]
        return column_name in cols


# ---------------------------------------------------------------------
# 支払方法管理（店舗スコープ対応・列未追加でも落ちない安全版＋ユニーク移行）
# ---------------------------------------------------------------------

# --- 関数：支払方法コードのユニーク制約を複合キーへ移行 -------------------------
def ensure_payment_method_unique_scope():
    """コード単体ユニークを"可能なら"複合ユニークへ。sqlite_autoindex_* はDROPせずスキップ。"""
    with engine.begin() as conn:
        # データベース方言を判定
        dialect = conn.dialect.name if hasattr(conn, 'dialect') else 'sqlite'
        
        if dialect == 'sqlite':
            # SQLite: PRAGMA table_info を使用
            cols = [row[1] for row in conn.execute(text("PRAGMA table_info('M_支払方法')")).fetchall()]
        else:
            # PostgreSQL: information_schema.columns を使用
            cols = [
                r["column_name"] for r in conn.execute(
                    text("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = :table_name
                    """),
                    {"table_name": "M_支払方法"}
                ).mappings().all()
            ]
        
        has_store  = "店舗ID"    in cols
        has_tenant = "tenant_id" in cols

        # SQLiteのみでインデックスの処理を実行
        if dialect == 'sqlite':
            # 既存ユニークIndexを列挙。「コード」単体ユニークで、かつ自動ではないものだけDROP
            idx_rows = conn.execute(text("PRAGMA index_list('M_支払方法')")).fetchall()
            for r in idx_rows:
                idx_name = r[1]   # name
                unique   = r[2]   # 1=unique
                if unique != 1:
                    continue
                # 自動インデックスはDROP不可なのでスキップ
                if idx_name.startswith("sqlite_autoindex_"):
                    continue
                cols_of_idx = [ir[2] for ir in conn.execute(text(f"PRAGMA index_info('{idx_name}')")).fetchall()]
                if cols_of_idx == ["コード"]:
                    try:
                        conn.execute(text(f'DROP INDEX IF EXISTS "{idx_name}"'))
                    except OperationalError:
                        # DROPできなければ無視（必要ならリビルド案内）
                        pass

        # 目的の複合ユニークIndexを作成（ある場合は何もしない）
        if has_tenant and has_store:
            if dialect == 'sqlite':
                conn.execute(text(
                    'CREATE UNIQUE INDEX IF NOT EXISTS uq_支払方法_tenant_store_code '
                    'ON "M_支払方法"(tenant_id, "店舗ID", "コード")'
                ))
            else:
                # PostgreSQLでは IF NOT EXISTS をサポートしないため、エラーを無視
                try:
                    conn.execute(text(
                        'CREATE UNIQUE INDEX uq_支払方法_tenant_store_code '
                        'ON "M_支払方法"(tenant_id, "店舗ID", "コード")'
                    ))
                except Exception:
                    pass
        elif has_tenant:
            if dialect == 'sqlite':
                conn.execute(text(
                    'CREATE UNIQUE INDEX IF NOT EXISTS uq_支払方法_tenant_code '
                    'ON "M_支払方法"(tenant_id, "コード")'
                ))
            else:
                # PostgreSQLでは IF NOT EXISTS をサポートしないため、エラーを無視
                try:
                    conn.execute(text(
                        'CREATE UNIQUE INDEX uq_支払方法_tenant_code '
                        'ON "M_支払方法"(tenant_id, "コード")'
                    ))
                except Exception:
                    pass

        return has_store, has_tenant


# --- 画面：支払方法一覧（管理者） ------------------------------------------------
@app.route("/admin/payment_methods")
@require_admin
def admin_payment_methods():
    sid = current_store_id()
    if sid is None:
        flash("店舗が選択されていません。いったんログインし直してください。")
        return redirect(url_for("admin_login"))

    s = SessionLocal()
    try:
        has_store_col = has_db_column("M_支払方法", "店舗ID")
        has_tenant_col = has_db_column("M_支払方法", "tenant_id")

        # 必要な列だけ明示SELECT（存在しない列は選ばない）
        q = s.query(
            PaymentMethod.id,
            PaymentMethod.code,
            PaymentMethod.name,
            PaymentMethod.active,
            PaymentMethod.display_order,
        )

        if has_tenant_col and session.get("tenant_id") is not None and hasattr(PaymentMethod, "tenant_id"):
            q = q.filter(PaymentMethod.tenant_id == session.get("tenant_id"))

        if has_store_col and hasattr(PaymentMethod, "store_id"):
            q = q.filter(PaymentMethod.store_id == sid)

        rows = q.order_by(PaymentMethod.display_order, PaymentMethod.name).all()

        lst = [{
            "id": r.id, "コード": r.code, "名称": r.name,
            "有効": r.active, "表示順": r.display_order
        } for r in rows]

        return render_template("payment_methods.html", title="支払方法マスタ", rows=lst)
    except Exception as e:
        return f"支払方法の取得中にエラーが発生しました: {e}", 500
    finally:
        s.close()


# --- 登録：支払方法の新規追加 ----------------------------------------------------
@app.route("/admin/payment_methods/new", methods=["POST"])
@require_admin
def admin_payment_methods_new():
    sid = current_store_id()
    if sid is None:
        flash("店舗が選択されていません。")
        return redirect(url_for("admin_payment_methods"))

    # ★ ここでユニーク制約を複合に移行
    has_store_col, has_tenant_col = ensure_payment_method_unique_scope()

    f = request.form
    s = SessionLocal()
    try:
        code = (f.get("コード") or "").strip()
        name = (f.get("名称") or "").strip()
        disp = int(f.get("表示順", 0))
        if not code or not name:
            flash("コード・名称は必須です。")
            return redirect(url_for("admin_payment_methods"))

        # 重複コードチェック（列の有無を考慮）
        q = s.query(PaymentMethod.id).filter(PaymentMethod.code == code)
        if has_tenant_col and hasattr(PaymentMethod, "tenant_id") and session.get("tenant_id") is not None:
            q = q.filter(PaymentMethod.tenant_id == session.get("tenant_id"))
        if has_store_col and hasattr(PaymentMethod, "store_id"):
            q = q.filter(PaymentMethod.store_id == sid)
        if q.first():
            flash("同じコードが既に存在します。")
            return redirect(url_for("admin_payment_methods"))

        now = now_str()

        if has_store_col:
            pm = PaymentMethod(
                code=code, name=name, active=1,
                display_order=disp, created_at=now, updated_at=now
            )
            if hasattr(PaymentMethod, "store_id"):
                pm.store_id = sid
            if has_tenant_col and hasattr(PaymentMethod, "tenant_id") and session.get("tenant_id") is not None:
                pm.tenant_id = session.get("tenant_id")
            s.add(pm)
        else:
            cols = ['"コード"', '"名称"', '"有効"', '"表示順"', '"作成日時"', '"更新日時"']
            vals = [':code', ':name', '1', ':disp', ':created', ':updated']
            params = {"code": code, "name": name, "disp": disp, "created": now, "updated": now}
            if has_tenant_col and session.get("tenant_id") is not None:
                cols.append('tenant_id')
                vals.append(':tenant_id')
                params["tenant_id"] = session.get("tenant_id")
            sql = f'INSERT INTO "M_支払方法" ({", ".join(cols)}) VALUES ({", ".join(vals)})'
            s.execute(text(sql), params)

        s.commit()
        return redirect(url_for("admin_payment_methods"))
    except IntegrityError as ie:
        s.rollback()
        flash("ユニーク制約により登録できませんでした。別店舗でも同じコードを使う場合は、\n"
              "① /admin/dev/migrate_payment_method_store_id を実行\n"
              "② /admin/dev/rebuild_payment_method_unique を実行\n"
              f"詳細: {ie}")
        return redirect(url_for("admin_payment_methods"))
    except Exception as e:
        s.rollback()
        flash(f"登録に失敗しました: {e}")
        return redirect(url_for("admin_payment_methods"))
    finally:
        s.close()


# --- 切替：支払方法の有効/無効トグル --------------------------------------------
@app.route("/admin/payment_methods/<int:pid>/toggle", methods=["POST"])
@require_admin
def admin_payment_methods_toggle(pid):
    sid = current_store_id()
    if sid is None:
        flash("店舗が選択されていません。")
        return redirect(url_for("admin_payment_methods"))

    s = SessionLocal()
    try:
        has_store_col  = has_db_column("M_支払方法", "店舗ID")
        has_tenant_col = has_db_column("M_支払方法", "tenant_id")
        now = now_str()

        if has_store_col:
            q = s.query(PaymentMethod).filter(PaymentMethod.id == pid)
            if hasattr(PaymentMethod, "store_id"):
                q = q.filter(PaymentMethod.store_id == sid)
            if has_tenant_col and hasattr(PaymentMethod, "tenant_id") and session.get("tenant_id") is not None:
                q = q.filter(PaymentMethod.tenant_id == session.get("tenant_id"))
            pm = q.first()
            if pm:
                pm.active = 0 if pm.active == 1 else 1
                pm.updated_at = now
                s.commit()
            else:
                flash("対象の支払方法が見つかりません。")
        else:
            where = ['id = :pid']
            params = {"pid": pid}
            if has_tenant_col and session.get("tenant_id") is not None:
                where.append('tenant_id = :tenant_id')
                params["tenant_id"] = session.get("tenant_id")
            sql = f'''
                UPDATE "M_支払方法"
                SET "有効" = CASE WHEN "有効" = 1 THEN 0 ELSE 1 END,
                    "更新日時" = :now
                WHERE {' AND '.join(where)}
            '''
            params["now"] = now
            s.execute(text(sql), params)
            s.commit()

        return redirect(url_for("admin_payment_methods"))
    except Exception as e:
        s.rollback()
        flash(f"更新に失敗しました: {e}")
        return redirect(url_for("admin_payment_methods"))
    finally:
        s.close()


# --- 公開API：支払方法のJSON（会計UI用） ---------------------------------------
@app.route("/admin/payment_methods/json")
@require_any
def payment_methods_json():
    sid = current_store_id()
    if sid is None:
        return jsonify({"ok": True, "methods": []})

    s = SessionLocal()
    try:
        has_store_col  = has_db_column("M_支払方法", "店舗ID")
        has_tenant_col = has_db_column("M_支払方法", "tenant_id")

        q = s.query(
            PaymentMethod.id,
            PaymentMethod.name,
            PaymentMethod.code,
        ).filter(PaymentMethod.active == 1)

        if has_tenant_col and hasattr(PaymentMethod, "tenant_id") and session.get("tenant_id") is not None:
            q = q.filter(PaymentMethod.tenant_id == session.get("tenant_id"))

        if has_store_col and hasattr(PaymentMethod, "store_id"):
            q = q.filter(PaymentMethod.store_id == sid)

        rows = q.order_by(PaymentMethod.display_order, PaymentMethod.name).all()
        return jsonify({"ok": True, "methods": [
            {"id": r.id, "name": r.name, "code": r.code} for r in rows
        ]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        s.close()


# --- 開発：支払方法ユニークインデックスの再構築 --------------------------------
@app.route("/admin/dev/rebuild_payment_method_unique")
@require_admin
def rebuild_payment_method_unique():
    from sqlalchemy import text
    with engine.begin() as conn:
        # データベース方言を判定
        dialect = conn.dialect.name if hasattr(conn, 'dialect') else 'sqlite'
        
        if dialect == 'sqlite':
            # SQLite: PRAGMA table_info を使用
            col_rows = conn.execute(text("PRAGMA table_info('M_支払方法')")).fetchall()
            colnames = [r[1] for r in col_rows]
        else:
            # PostgreSQL: information_schema.columns を使用
            colnames = [
                r["column_name"] for r in conn.execute(
                    text("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = :table_name
                    """),
                    {"table_name": "M_支払方法"}
                ).mappings().all()
            ]
        
        has_store  = "店舗ID" in colnames
        has_tenant = "tenant_id" in colnames

        if not has_store:
            conn.execute(text('ALTER TABLE "M_支払方法" ADD COLUMN "店舗ID" INTEGER'))
            has_store = True

        # SQLiteのみでテーブル再作成を実行（PostgreSQLでは不要）
        if dialect == 'sqlite':
            conn.execute(text("""
                CREATE TABLE "M_支払方法__new" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    "店舗ID" INTEGER,
                    "コード" TEXT NOT NULL,
                    "名称"  TEXT NOT NULL,
                    "有効"  INTEGER DEFAULT 1,
                    "表示順" INTEGER DEFAULT 0,
                    "作成日時" TEXT,
                    "更新日時" TEXT,
                    tenant_id INTEGER
                )
            """))

            conn.execute(text("""
                INSERT INTO "M_支払方法__new"
                (id, "店舗ID", "コード", "名称", "有効", "表示順", "作成日時", "更新日時", tenant_id)
                SELECT id, "店舗ID", "コード", "名称", "有効", "表示順", "作成日時", "更新日時", tenant_id
                FROM "M_支払方法"
            """))

            conn.execute(text('DROP TABLE "M_支払方法"'))
            conn.execute(text('ALTER TABLE "M_支払方法__new" RENAME TO "M_支払方法"'))

            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_支払方法_order_name ON "M_支払方法"("表示順","名称")'))
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_支払方法_store ON "M_支払方法"("店舗ID")'))

            if has_tenant and has_store:
                conn.execute(text(
                    'CREATE UNIQUE INDEX IF NOT EXISTS uq_支払方法_tenant_store_code '
                    'ON "M_支払方法"(tenant_id, "店舗ID", "コード")'
                ))
            elif has_tenant:
                conn.execute(text(
                    'CREATE UNIQUE INDEX IF NOT EXISTS uq_支払方法_tenant_code '
                    'ON "M_支払方法"(tenant_id, "コード")'
                ))
    return "ok"


# ---------------------------------------------------------------------
# ★ 開発用：M_支払方法 に 店舗ID 列を追加するマイグレーション
# ---------------------------------------------------------------------

# --- 開発：支払方法テーブルへ店舗IDを追加＆既存行を埋める ----------------------
@app.route("/admin/dev/migrate_payment_method_store_id")
@require_admin
def migrate_payment_method_store_id():
    from sqlalchemy import text
    sid = current_store_id()
    if sid is None:
        return "store_id not in session", 400
    with engine.begin() as conn:
        # データベース方言を判定
        dialect = conn.dialect.name if hasattr(conn, 'dialect') else 'sqlite'
        
        if dialect == 'sqlite':
            # SQLite: PRAGMA table_info を使用
            cols = [r[1] for r in conn.execute(text("PRAGMA table_info('M_支払方法')")).fetchall()]
        else:
            # PostgreSQL: information_schema.columns を使用
            cols = [
                r["column_name"] for r in conn.execute(
                    text("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = :table_name
                    """),
                    {"table_name": "M_支払方法"}
                ).mappings().all()
            ]
        
        if "店舗ID" not in cols:
            conn.execute(text('ALTER TABLE "M_支払方法" ADD COLUMN "店舗ID" INTEGER'))
        conn.execute(text('UPDATE "M_支払方法" SET "店舗ID" = :sid WHERE "店舗ID" IS NULL'), {"sid": sid})
    return "ok"


# ---------------------------------------------------------------------
# テーブル/カテゴリ/プリンタ/印刷ルール（管理者）
# ---------------------------------------------------------------------

# --- 画面：テーブル一覧 ---------------------------------------------------------
@app.route("/admin/tables")
@require_admin
def admin_tables():
    s = SessionLocal()
    try:
        sid = current_store_id()
        q = s.query(TableSeat)
        if sid is not None and hasattr(TableSeat, "store_id"):
            q = q.filter(TableSeat.store_id == sid)   # ★ 店舗で絞る
        rows = q.order_by(TableSeat.table_no).all()
        tables = [{"id": r.id, "テーブル番号": r.table_no, "状態": r.status} for r in rows]
        return render_template("tables.html", title="テーブル管理", tables=tables)
    finally:
        s.close()


# --- 登録：テーブル新規作成 -----------------------------------------------------
@app.route("/admin/tables/new", methods=["POST"])
@require_admin
def admin_tables_new():
    s = SessionLocal()
    try:
        n = (request.form.get("テーブル番号") or "").strip()
        if not n:
            return "テーブル番号は必須です", 400
        sid = current_store_id()
        rec = TableSeat(table_no=n, status="空席")
        if sid is not None and hasattr(rec, "store_id"):
            rec.store_id = sid                     # ★ 店舗IDを保存
        s.add(rec); s.commit()
        return redirect(url_for("admin_tables"))
    finally:
        s.close()


# --- 削除：テーブル削除 ---------------------------------------------------------
@app.route("/admin/tables/<int:table_id>/delete", methods=["POST"])
@require_admin
def admin_tables_delete(table_id):
    s = SessionLocal()
    try:
        t = s.get(TableSeat, table_id)
        if t:
            s.delete(t)
            s.commit()
        return redirect(url_for("admin_tables"))
    finally:
        s.close()


# --- ヘルパ：テーブル用の恒久QRトークンを確保 -----------------------------------
# 依存: pip install qrcode[pil]
def _ensure_permanent_qr_token(s, table_id: int) -> str:
    """そのテーブル用の『恒久トークン』を1つ確保。既存があればそれを返す。"""
    # 例: QrToken に unique( table_id, permanent ) or token を持っている前提
    t = (s.query(QrToken)
          .filter(QrToken.table_id == table_id,
                  QrToken.permanent == 1)  # なければ is_active=1 等でもOK
          .order_by(QrToken.id.desc())
          .first())
    if t:
        return t.token

    # 新規発行（有効期限なし）
    token = base64.urlsafe_b64encode(os.urandom(24)).decode().rstrip("=")
    q = QrToken(
        table_id=table_id,
        token=token,
        permanent=1,
        created_at=now_str(),
        store_id=current_store_id()  # before_flushでも入るが明示
    )
    s.add(q); s.flush()
    return token


# --- 画面：テーブルQR印刷ページ -------------------------------------------------
@app.route("/admin/tables/<int:table_id>/qr")
@require_admin
def admin_table_qr(table_id):
    sid = current_store_id()
    if sid is None:
        return redirect(url_for("admin_login"))

    s = SessionLocal()
    try:
        # テーブルの存在 & 店舗一致
        t = (s.query(TableSeat)
               .filter(TableSeat.id == table_id,
                       TableSeat.store_id == sid)
               .first())
        if not t:
            abort(404)

        # 恒久トークンを確保
        token = _ensure_permanent_qr_token(s, table_id)
        slug  = session.get("tenant_slug")

        # お客さま用URL（menu_page は /t/<tenant_slug>/m/<token> で想定）
        access_url = url_for("menu_page", tenant_slug=slug, token=token, _external=True)

        # QR画像URL（下の png ルート）
        png_url    = url_for("qr_png", tenant_slug=slug, token=token)

        return render_template(
            "admin_table_qr.html",
            table_no=getattr(t, "table_no", table_id),
            access_url=access_url,
            png_url=png_url,
        )
    finally:
        s.close()


# --- 画像：テーブルQRのPNGを返す ------------------------------------------------
@app.route("/t/<tenant_slug>/qr/<token>.png")
def qr_png(tenant_slug, token):
    url = url_for("menu_page", tenant_slug=tenant_slug, token=token, _external=True)
    img = qrcode.make(url)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return send_file(buf, mimetype="image/png", as_attachment=False)


# ---------------------------------------------------------------------
# カテゴリ管理（管理者）
# ---------------------------------------------------------------------

# --- 画面：カテゴリ一覧 ---------------------------------------------------------
@app.route("/admin/categories")
@require_admin
def admin_categories():
    s = SessionLocal()
    try:
        sid = current_store_id()
        q = s.query(Category)
        if sid is not None and hasattr(Category, "store_id"):
            q = q.filter(Category.store_id == sid)     # ★ 店舗で絞る
        cats = q.order_by(Category.parent_id, Category.display_order, Category.name).all()

        # ツリー整形（親→子の順に並べる）
        children_map, roots = {}, []
        for c in cats:
            children_map.setdefault(c.parent_id, []).append(c)
            if not c.parent_id:
                roots.append(c)

        out = []
        def dfs(node, depth):
            parent = s.get(Category, node.parent_id) if node.parent_id else None
            out.append({
                "id": node.id, "名称": node.name,
                "親": parent.name if parent else None,
                "表示順": node.display_order, "有効": node.active, "depth": depth,
            })
            for ch in sorted(children_map.get(node.id, []), key=lambda x:(x.display_order, x.name)):
                dfs(ch, depth+1)

        for r in sorted(roots, key=lambda x:(x.display_order, x.name)):
            dfs(r, 1)

        return render_template("categories.html", title="カテゴリ管理", cats=out)
    finally:
        s.close()


# --- 登録：カテゴリ新規作成 -----------------------------------------------------
@app.route("/admin/categories/new", methods=["POST"])
@require_admin
def admin_categories_new():
    s = SessionLocal()
    try:
        f = request.form
        name = (f.get("名称") or "").strip()
        if not name:
            return "名称は必須です", 400

        sid = current_store_id()
        parent_id = int(f["親カテゴリID"]) if f.get("親カテゴリID") else None

        # 階層深さチェック（最大5）
        if parent_id:
            def depth(sess, pid):
                d, cur = 1, sess.get(Category, pid)
                while cur and cur.parent_id:
                    d += 1
                    cur = sess.get(Category, cur.parent_id)
                return d
            if depth(s, parent_id) >= 5:
                return "これ以上子カテゴリを作れません（最大5階層）", 400

        rec = Category(
            parent_id=parent_id,
            name=name,
            display_order=int(f.get("表示順", 0)),
            active=1, created_at=now_str(), updated_at=now_str(),
        )
        if sid is not None and hasattr(Category, "store_id"):
            rec.store_id = sid                         # ★ 店舗IDを保存
        s.add(rec); s.commit()
        return redirect(url_for("admin_categories"))
    finally:
        s.close()


# --- 画面/更新：カテゴリ編集 ----------------------------------------------------
@app.route("/admin/categories/<int:cid>/edit", methods=["GET", "POST"])
@require_admin
def admin_categories_edit(cid):
    s = SessionLocal()
    try:
        c = s.get(Category, cid)
        if not c:
            abort(404)

        if request.method == "POST":
            f = request.form
            name = f.get("名称", "").strip()
            disp = int(f.get("表示順", 0))
            parent_id = f.get("親カテゴリID")
            parent_id = int(parent_id) if parent_id else None
            active = 1 if f.get("有効") == "1" else 0

            if parent_id == c.id:
                return "自分自身を親に指定できません。", 400
            if parent_id and is_descendant(s, c.id, parent_id):
                return "自分の子孫を親に指定できません（循環防止）。", 400
            if parent_id:
                depth = get_depth(s, parent_id)
                if depth >= 5:
                    return "これ以上子カテゴリを作れません（最大5階層）。", 400

            c.name = name or c.name
            c.display_order = disp
            c.parent_id = parent_id
            c.active = active
            c.updated_at = now_str()
            s.commit()
            return redirect(url_for("admin_categories"))

        cat_options = fetch_categories_with_depth(s)
        cat_options = [x for x in cat_options if x["id"] != c.id]
        return render_template("categories_edit.html", title="カテゴリ編集", cat=c, cat_options=cat_options)
    finally:
        s.close()


# --- 切替：カテゴリの有効/無効トグル --------------------------------------------
@app.route("/admin/categories/<int:cid>/toggle", methods=["POST"])
@require_admin
def admin_categories_toggle(cid):
    s = SessionLocal()
    try:
        c = s.get(Category, cid)
        if not c:
            abort(404)
        c.active = 0 if c.active == 1 else 1
        c.updated_at = now_str()
        s.commit()
        return redirect(url_for("admin_categories"))
    finally:
        s.close()


# --- 削除：カテゴリ削除（子や割当の存在チェック込み） ---------------------------
@app.route("/admin/categories/<int:cid>/delete", methods=["POST"])
@require_admin
def admin_categories_delete(cid):
    s = SessionLocal()
    try:
        c = s.get(Category, cid)
        if not c:
            abort(404)
        child_exists = s.query(Category.id).filter(Category.parent_id == cid).first()
        if child_exists:
            return "子カテゴリが存在するため削除できません。先に子を削除してください。", 400

        link_exists = s.query(ProductCategoryLink.product_id).filter(
            ProductCategoryLink.category_id == cid
        ).first()
        if link_exists:
            return "商品に割り当てがあるため削除できません。割り当て解除後に再実行してください。", 400

        s.delete(c)
        s.commit()
        return redirect(url_for("admin_categories"))
    finally:
        s.close()


# ---------------------------------------------------------------------
# 商品オプション管理（管理者）
# ---------------------------------------------------------------------

# --- 画面：商品オプション一覧 ---------------------------------------------------
@app.route("/admin/product-options")
@require_admin
def admin_product_options():
    s = SessionLocal()
    try:
        sid = current_store_id()
        options = (s.query(ProductOption)
                   .filter(ProductOption.store_id == sid)
                   .order_by(ProductOption.display_order.asc(), ProductOption.id.asc())
                   .all())
        
        # 各オプションの選択肢数と商品名を取得
        options_data = []
        for opt in options:
            choice_count = (s.query(OptionChoice)
                           .filter(OptionChoice.option_id == opt.id)
                           .count())
            
            # 複数商品対応：productsリレーションシップを使用
            if opt.products:
                product_names = [p.name for p in opt.products[:3]]  # 最初の3つまで表示
                if len(opt.products) > 3:
                    product_name = ", ".join(product_names) + f" 他{len(opt.products)-3}件"
                else:
                    product_name = ", ".join(product_names)
            else:
                product_name = "全商品共通"
            
            options_data.append({
                "id": opt.id,
                "option_name": opt.option_name,
                "product_name": product_name,
                "choice_count": choice_count,
                "required": bool(opt.required),
                "multiple": bool(opt.multiple),
                "active": bool(opt.active),
                "display_order": opt.display_order
            })
        
        return render_template("product_options_list.html", 
                             title="商品オプション管理", 
                             options=options_data)
    finally:
        s.close()


# --- 画面：商品オプション新規作成 -----------------------------------------------
@app.route("/admin/product-options/new", methods=["GET", "POST"])
@require_admin
def admin_product_options_new():
    s = SessionLocal()
    try:
        sid = current_store_id()
        
        if request.method == "POST":
            f = request.form
            option_name = f.get("option_name", "").strip()
            product_ids = request.form.getlist("product_ids")  # 複数選択
            display_order = int(f.get("display_order", 0))
            required = 1 if f.get("required") == "1" else 0
            multiple = 1 if f.get("multiple") == "1" else 0
            active = 1 if f.get("active") == "1" else 0
            
            if not option_name:
                flash("オプション名を入力してください。")
                return redirect(url_for("admin_product_options_new"))
            
            if not product_ids:
                flash("適用する商品を少なくとも1つ選択してください。")
                return redirect(url_for("admin_product_options_new"))
            
            # オプションを作成
            opt = ProductOption(
                store_id=sid,
                option_name=option_name,
                display_order=display_order,
                required=required,
                multiple=multiple,
                active=active,
                created_at=now_str(),
                updated_at=now_str()
            )
            s.add(opt)
            s.flush()  # IDを取得するためにflush
            
            # 中間テーブルに商品を紐付け
            for product_id in product_ids:
                apply = ProductOptionApply(
                    store_id=sid,
                    option_id=opt.id,
                    product_id=int(product_id)
                )
                s.add(apply)
            
            s.commit()
            
            flash(f"オプション「{option_name}」を作成しました。")
            return redirect(url_for("admin_product_options_edit", option_id=opt.id))
        
        # GET: 商品一覧を取得
        products = (s.query(Menu)
                   .filter(Menu.store_id == sid)
                   .order_by(Menu.name.asc())
                   .all())
        
        return render_template("product_options_new.html", 
                             title="商品オプション新規作成",
                             products=products)
    finally:
        s.close()


# --- 画面：商品オプション編集 ---------------------------------------------------
@app.route("/admin/product-options/<int:option_id>/edit", methods=["GET", "POST"])
@require_admin
def admin_product_options_edit(option_id):
    s = SessionLocal()
    try:
        opt = s.get(ProductOption, option_id)
        if not opt:
            abort(404)
        
        if request.method == "POST":
            f = request.form
            action = f.get("action")
            
            # オプション基本情報の更新
            if action == "update_option":
                opt.option_name = f.get("option_name", "").strip() or opt.option_name
                product_ids = request.form.getlist("product_ids")  # 複数選択
                opt.display_order = int(f.get("display_order", 0))
                opt.required = 1 if f.get("required") == "1" else 0
                opt.multiple = 1 if f.get("multiple") == "1" else 0
                opt.active = 1 if f.get("active") == "1" else 0
                opt.updated_at = now_str()
                
                # 既存の商品紐付けを削除
                s.query(ProductOptionApply).filter(ProductOptionApply.option_id == opt.id).delete()
                
                # 新しい商品紐付けを追加
                if product_ids:
                    for product_id in product_ids:
                        apply = ProductOptionApply(
                            store_id=opt.store_id,
                            option_id=opt.id,
                            product_id=int(product_id)
                        )
                        s.add(apply)
                
                s.commit()
                flash("オプション情報を更新しました。")
            
            # 選択肢の追加
            elif action == "add_choice":
                choice_name = f.get("choice_name", "").strip()
                extra_price = int(f.get("extra_price", 0))
                display_order = int(f.get("choice_display_order", 0))
                
                if choice_name:
                    choice = OptionChoice(
                        store_id=opt.store_id,
                        option_id=opt.id,
                        choice_name=choice_name,
                        extra_price=extra_price,
                        display_order=display_order,
                        active=1,
                        created_at=now_str(),
                        updated_at=now_str()
                    )
                    s.add(choice)
                    s.commit()
                    flash(f"選択肢「{choice_name}」を追加しました。")
            
            # 選択肢の更新
            elif action == "update_choice":
                choice_id = int(f.get("choice_id"))
                choice = s.get(OptionChoice, choice_id)
                if choice and choice.option_id == opt.id:
                    choice.choice_name = f.get("choice_name", "").strip() or choice.choice_name
                    choice.extra_price = int(f.get("extra_price", 0))
                    choice.display_order = int(f.get("choice_display_order", 0))
                    choice.active = 1 if f.get("choice_active") == "1" else 0
                    choice.updated_at = now_str()
                    s.commit()
                    flash("選択肢を更新しました。")
            
            # 選択肢の削除
            elif action == "delete_choice":
                choice_id = int(f.get("choice_id"))
                choice = s.get(OptionChoice, choice_id)
                if choice and choice.option_id == opt.id:
                    s.delete(choice)
                    s.commit()
                    flash("選択肢を削除しました。")
            
            return redirect(url_for("admin_product_options_edit", option_id=option_id))
        
        # GET: 選択肢一覧を取得
        choices = (s.query(OptionChoice)
                  .filter(OptionChoice.option_id == opt.id)
                  .order_by(OptionChoice.display_order.asc(), OptionChoice.id.asc())
                  .all())
        
        # 商品一覧を取得
        products = (s.query(Menu)
                   .filter(Menu.store_id == opt.store_id)
                   .order_by(Menu.name.asc())
                   .all())
        
        # このオプションに紐付けられている商品IDのリストを取得
        selected_product_ids = [apply.product_id for apply in 
                               s.query(ProductOptionApply)
                               .filter(ProductOptionApply.option_id == opt.id)
                               .all()]
        
        return render_template("product_options_edit.html", 
                             title="商品オプション編集",
                             option=opt,
                             choices=choices,
                             products=products,
                             selected_product_ids=selected_product_ids)
    finally:
        s.close()


# --- 削除：商品オプション削除 ---------------------------------------------------
@app.route("/admin/product-options/<int:option_id>/delete", methods=["POST"])
@require_admin
def admin_product_options_delete(option_id):
    s = SessionLocal()
    try:
        opt = s.get(ProductOption, option_id)
        if not opt:
            abort(404)
        
        # 注文履歴に使用されているかチェック
        used = (s.query(OrderOption.id)
               .filter(OrderOption.option_id == option_id)
               .first())
        
        if used:
            flash("このオプションは注文履歴で使用されているため削除できません。無効化してください。")
            return redirect(url_for("admin_product_options"))
        
        s.delete(opt)
        s.commit()
        flash("オプションを削除しました。")
        return redirect(url_for("admin_product_options"))
    finally:
        s.close()


# ---------------------------------------------------------------------
# プリンタ設定（自動検出API付き）
# ---------------------------------------------------------------------

# =========================
#  低レベルユーティリティ
# =========================

# --- 低レイヤ：TCPポート疎通チェック -------------------------------------------
def _port_open(ip: str, port: int, timeout: float = 0.25) -> bool:
    """TCP ポート疎通確認（短時間）"""
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(timeout)
        try:
            sock.connect((ip, port))
            return True
        except Exception:
            return False


# --- 低レイヤ：ARPテーブルから同一LAN候補IPを抽出 -------------------------------
def _arp_hosts() -> list[str]:
    """ARP テーブルから同一 LAN の候補 IP を抽出（Win/Unix 想定）"""
    try:
        out = subprocess.check_output(["arp", "-a"], encoding="utf-8", errors="ignore")
        ips = re.findall(r'(\d+\.\d+\.\d+\.\d+)', out)
        # ループバック/ブロードキャストを除外しつつ順序保持で重複排除
        return [ip for ip in dict.fromkeys(ips)
                if not (ip.startswith("127.") or ip.startswith("0.") or ip.endswith(".255"))]
    except Exception:
        return []


# --- 低レイヤ：候補IPへ指定ポート疎通（簡易スキャン） ---------------------------
def _scan_hosts_quick(hosts: list[str], ports: list[int] | None = None, per_host_timeout: float = 0.25):
    """
    候補 IP へ指定ポートの疎通確認。
    既定は ESC/POS RAW 9100 のみ（誤検出を避ける）。
    """
    if not ports:
        ports = [9100]

    results = []
    for ip in hosts:
        for p in ports:
            if not _port_open(ip, p, per_host_timeout):
                continue

            if p == 9100:
                kind = "escpos_tcp"
                conn = f"tcp://{ip}:{p}"
                name = f"ESC/POS {ip}"
            elif p == 631:
                # IPP はキュー名が必要なことが多いので参考候補扱い
                kind = "cups"
                conn = f"ipp://{ip}:{p}"
                name = f"IPP {ip}"
            else:
                kind = "escpos_tcp"
                conn = f"tcp://{ip}:{p}"
                name = f"Printer {ip}:{p}"

            results.append({
                "name": name,
                "kind": kind,
                "connection": conn,
                "ip": ip,
                "port": p,
                "width": 42,
                "source": "scan",
            })
    return results


# --- 自動検出：mDNS/Bonjourでプリンタ候補を探索 ---------------------------------
def _discover_mdns(timeout_sec: float = 3.0):
    """
    mDNS/Bonjour 探索（zeroconf が無ければ空）。
    IPP 系は `cups://<サービス名>` を接続情報に（UI の種別=cups と合致）。
    RAW/ESC/POS 系は escpos_tcp + tcp://IP:9100 として返す。
    """
    if not HAS_ZEROCONF:
        return [], {"enabled": False}

    zc = Zeroconf()
    found = {}

    SERVICE_TYPES = [
        "_ipp._tcp.local.",
        "_ipps._tcp.local.",
        "_printer._tcp.local.",
        "_pdl-datastream._tcp.local.",  # RAW 9100
        "_escpos._tcp.local.",
        "_star_printer._tcp.local.",
    ]

    class _Listener:
        def add_service(self, zc, typ, name):
            try:
                info = zc.get_service_info(typ, name, timeout=int(timeout_sec * 1000))
                if not info:
                    return

                # IP 抽出
                addrs = []
                for b in getattr(info, "addresses", []) or []:
                    if isinstance(b, (bytes, bytearray)) and len(b) == 4:
                        try:
                            addrs.append(socket.inet_ntoa(b))
                        except Exception:
                            pass

                base = name.split(".")[0]  # サービス表示名（CUPS キュー名になりがち）
                port = info.port or 0

                if typ.startswith("_ipp"):
                    # CUPS 側にキューがある前提。IP が無くても登録できる可能性。
                    key = ("cups", f"cups://{base}")
                    found[key] = {
                        "name": base,
                        "kind": "cups",
                        "connection": f"cups://{base}",
                        "ip": (addrs[0] if addrs else ""),
                        "port": port,
                        "width": 42,
                        "source": "mdns",
                        "service": typ,
                    }
                else:
                    # RAW/ESC/POS 系は IP 必須
                    if not addrs:
                        return
                    ip = addrs[0]
                    p = port or 9100
                    key = ("escpos_tcp", f"tcp://{ip}:{p}")
                    found[key] = {
                        "name": f"ESC/POS {ip}",
                        "kind": "escpos_tcp",
                        "connection": f"tcp://{ip}:{p}",
                        "ip": ip,
                        "port": p,
                        "width": 42,
                        "source": "mdns",
                        "service": typ,
                    }
            except Exception:
                # 個々のレコード取得失敗は無視
                pass

        def remove_service(self, *a, **kw): pass
        def update_service(self, *a, **kw): pass

    try:
        listener = _Listener()
        browsers = [ServiceBrowser(zc, t, listener) for t in SERVICE_TYPES]  # 参照保持
        time.sleep(timeout_sec)  # 受信待ち
    finally:
        with contextlib.suppress(Exception):
            zc.close()

    cands = list(found.values())
    return cands, {"enabled": True, "service_types": SERVICE_TYPES, "count": len(cands)}



# ---------------------------------------------------------------------
# プリンタ管理（一覧／登録／検出／一括登録）
# ---------------------------------------------------------------------

# --- プリンタ一覧（管理） ------------------------------------------------------
@app.route("/admin/printers")
@require_admin
def admin_printers():
    s = SessionLocal()
    try:
        sid = current_store_id()
        q = s.query(Printer)
        if sid is not None and hasattr(Printer, "store_id"):
            q = q.filter(Printer.store_id == sid)      # ★ 店舗で絞る
        rows = q.order_by(Printer.id).all()
        printers = [{"id": p.id, "名称": p.name, "種別": p.kind, "接続情報": p.connection,
                     "幅文字": p.width, "有効": p.enabled} for p in rows]
        return render_template(
            "printers.html",
            title="プリンタ",
            printers=printers,
            discover_info={"supported": True, "mdns": HAS_ZEROCONF}
        )
    finally:
        s.close()


# --- プリンタ新規登録（手動） ---------------------------------------------------
@app.route("/admin/printers/new", methods=["POST"])
@require_admin
def admin_printers_new():
    s = SessionLocal()
    try:
        f = request.form
        name = (f.get("名称") or "").strip()
        kind = (f.get("種別") or "").strip()
        conn = (f.get("接続情報") or "").strip()
        width = int(f.get("幅文字", 42))
        if not name or not kind:
            return "名称と種別は必須です", 400

        sid = current_store_id()
        rec = Printer(name=name, kind=kind, connection=conn, width=width, enabled=1)
        if sid is not None and hasattr(Printer, "store_id"):
            rec.store_id = sid                         # ★ 店舗IDを保存
        s.add(rec); s.commit()
        return redirect(url_for("admin_printers"))
    finally:
        s.close()


# --- プリンタ有効/無効トグル ----------------------------------------------------
@app.route("/admin/printers/<int:pid>/toggle", methods=["POST"])
@require_admin
def admin_printers_toggle(pid):
    s = SessionLocal()
    try:
        p = s.get(Printer, pid)
        if p:
            p.enabled = 0 if p.enabled == 1 else 1
            s.commit()
        return redirect(url_for("admin_printers"))
    finally:
        s.close()


# --- プリンタ自動検出API（JSON） ------------------------------------------------
@app.route("/admin/printers/discover", methods=["GET"], endpoint="admin_printers_discover")
@require_admin
def admin_printers_discover():
    """
    同一ネットワークからプリンタ候補を自動検出して返す（JSON）。
    mode: auto|mdns|scan / ports: "9100"（既定は 9100 のみ）
    """
    mode = (request.args.get("mode") or "auto").lower()
    cidr = request.args.get("cidr")
    ports_param = request.args.get("ports", "9100")   # ← 既定を 9100 のみに
    ports = []
    for x in (ports_param or "").split(","):
        try:
            n = int(x.strip())
            if 1 <= n <= 65535:
                ports.append(n)
        except Exception:
            pass

    candidates = []
    debug = {"mode": mode, "zeroconf": HAS_ZEROCONF, "ports": ports}

    # 1) mDNS（ある場合）
    if mode in ("auto", "mdns"):
        try:
            mdns_list, mdns_dbg = _discover_mdns(timeout_sec=3.0)
            candidates.extend(mdns_list)
            debug["mdns"] = mdns_dbg
        except Exception as e:
            debug["mdns_error"] = str(e)

    # 2) ARP ベースのクイックスキャン（既定 9100）
    if mode in ("auto", "scan"):
        hosts = []
        if cidr:
            try:
                net = ipaddress.ip_network(cidr, strict=False)
                hosts = [str(ip) for ip in net.hosts()]
            except Exception as e:
                debug["cidr_error"] = str(e)
        if not hosts:
            hosts = _arp_hosts()
        scan_list = _scan_hosts_quick(hosts, ports=ports)
        candidates.extend(scan_list)
        debug["scan_hosts"] = {"count": len(hosts)}

    # 既存登録との重複排除（同一店舗のみ）
    s = SessionLocal()
    sid = current_store_id()
    try:
        q = s.query(Printer)
        if sid is not None and hasattr(Printer, "store_id"):
            q = q.filter(Printer.store_id == sid)
        existing = {(p.connection or "").strip() for p in q.all()}
    finally:
        s.close()

    uniq = []
    seen = set()
    for c in candidates:
        key = (c.get("connection") or "").strip()
        if not key or key in seen or key in existing:
            continue
        seen.add(key)
        uniq.append(c)

    return jsonify(ok=True, count=len(uniq), candidates=uniq, debug=debug)



# --- プリンタ一括登録API（JSON） ------------------------------------------------
@app.route("/admin/printers/import", methods=["POST"], endpoint="admin_printers_import")
@require_admin
def admin_printers_import():
    """
    discover の candidates をそのまま DB 登録。
    kind は 'escpos_tcp' or 'cups' を推奨（UIの選択肢に一致）。
    既存接続情報と重複するものはスキップ。
    """
    s = SessionLocal()
    try:
        data = request.get_json(force=True) or {}
        items = data.get("items") or []
        sid = current_store_id()
        created = 0

        # 既存重複
        q = s.query(Printer)
        if sid is not None and hasattr(Printer, "store_id"):
            q = q.filter(Printer.store_id == sid)
        existing = {(p.connection or "").strip(): True for p in q.all()}

        for it in items:
            name = (it.get("name") or it.get("名称") or "プリンタ").strip()
            kind = (it.get("kind") or it.get("種別") or "").strip().lower()
            conn = (it.get("connection") or it.get("接続情報") or "").strip()
            width = int(it.get("width") or it.get("幅文字") or 42)
            if not conn or conn in existing:
                continue

            # kind 正規化（UI の選択肢に合わせる）
            if kind in ("escpos_tcp", "escpos", "raw", "tcp"):
                kind = "escpos_tcp"
            elif kind in ("cups", "ipp", "ipps"):
                kind = "cups"
            else:
                kind = "escpos_tcp"

            rec = Printer(name=name, kind=kind, connection=conn, width=width, enabled=1)
            if sid is not None and hasattr(Printer, "store_id"):
                rec.store_id = sid
            s.add(rec)
            existing[conn] = True
            created += 1

        s.commit()
        return jsonify(ok=True, created=created)
    except Exception as e:
        s.rollback()
        return jsonify(ok=False, error=str(e)), 400
    finally:
        s.close()


# ---------------------------------------------------------------------
# 印刷ルール管理
# ---------------------------------------------------------------------

# --- 印刷ルール一覧 -------------------------------------------------------------
@app.route("/admin/rules")
@require_admin
def admin_rules():
    s = SessionLocal()
    try:
        rules = s.query(PrintRule).order_by(PrintRule.id.desc()).all()
        out_rules = []
        for r in rules:
            cat_name = s.query(Category.name).filter(Category.id == r.category_id).scalar()
            menu_name = s.query(Menu.name).filter(Menu.id == r.menu_id).scalar()
            printer_name = s.query(Printer.name).filter(Printer.id == r.printer_id).scalar()
            out_rules.append({
                "id": r.id,
                "cat_name": cat_name,
                "menu_name": menu_name,
                "printer_name": printer_name
            })
        cats = s.query(Category.id, Category.name).filter(Category.active == 1)\
            .order_by(Category.display_order, Category.name).all()
        menu = s.query(Menu.id, Menu.name).order_by(Menu.display_order, Menu.name).all()
        printers = s.query(Printer.id, Printer.name).filter(Printer.enabled == 1).order_by(Printer.name).all()
        cats_dict = [{"id": c.id, "名称": c.name} for c in cats]
        menu_dict = [{"id": m.id, "名称": m.name} for m in menu]
        printers_dict = [{"id": p.id, "名称": p.name} for p in printers]
        return render_template("rules.html", title="印刷ルール",
                               rules=out_rules, cats=cats_dict, menu=menu_dict, printers=printers_dict)
    finally:
        s.close()


# --- 印刷ルール新規作成 ---------------------------------------------------------
@app.route("/admin/rules/new", methods=["POST"])
@require_admin
def admin_rules_new():
    f = request.form
    s = SessionLocal()
    try:
        cat_id = int(f["カテゴリID"]) if f.get("カテゴリID") else None
        menu_id = int(f["メニューID"]) if f.get("メニューID") else None
        pid = int(f["プリンタID"])
        r = PrintRule(category_id=cat_id, menu_id=menu_id, printer_id=pid)
        try:
            s.add(r)
            s.commit()
        except Exception:
            s.rollback()
        return redirect(url_for("admin_rules"))
    finally:
        s.close()


# --- 印刷ルール削除 -------------------------------------------------------------
@app.route("/admin/rules/<int:rid>/delete", methods=["POST"])
@require_admin
def admin_rules_delete(rid):
    s = SessionLocal()
    try:
        r = s.get(PrintRule, rid)
        if r:
            s.delete(r)
            s.commit()
        return redirect(url_for("admin_rules"))
    finally:
        s.close()


# ---------------------------------------------------------------------
# 開発ユーティリティ：store_id の NULL 補完
# ---------------------------------------------------------------------

# --- 開発：Order系の store_id を補完 --------------------------------------------
@app.route("/admin/dev/backfill_store_null")
@require_admin
def backfill_store_null():
    sid = current_store_id()
    if sid is None:
        return "店舗未選択", 400
    s = SessionLocal()
    try:
        # 1) ヘッダ: TableSeat.store_id から補完（まずはそこに合わせる）
        s.execute(text("""
            UPDATE "OrderHeader"
               SET store_id = (
                   SELECT ts.store_id FROM "TableSeat" ts
                    WHERE ts.id = "OrderHeader".table_id
               )
             WHERE store_id IS NULL
        """))

        # 2) まだ NULL が残るものを現店舗で埋める（テナント内で単一店舗運用なら有効）
        s.execute(text("""
            UPDATE "OrderHeader"
               SET store_id = :sid
             WHERE store_id IS NULL
        """), {"sid": sid})

        # 3) 明細: 親ヘッダに合わせる
        s.execute(text("""
            UPDATE "OrderItem"
               SET store_id = (
                   SELECT oh.store_id FROM "OrderHeader" oh
                    WHERE oh.id = "OrderItem".order_id
               )
             WHERE store_id IS NULL
        """))

        s.commit()
        return "backfill done", 200
    finally:
        s.close()


# ---------------------------------------------------------------------
# メンバー管理
# ---------------------------------------------------------------------

# --- メンバー追加（管理者用） ---------------------------------------------------
@app.route("/admin/members/new", methods=["GET", "POST"])
@require_admin
def admin_member_new():
    s = SessionLocal()
    try:
        store_id = int(session["store_id"]) if session.get("store_id") is not None else None
        if request.method == "POST":
            role = (request.form.get("role") or "").strip()  # "store_admin" / "employee" (既存互換: "admin" / "employee")
            name = (request.form.get("name") or "").strip()
            login_id = (request.form.get("login_id") or "").strip()
            password = (request.form.get("password") or "")
            password2 = (request.form.get("password2") or "")
            is_active = 1 if request.form.get("is_active") == "1" else 0

            errors = []
            if role not in ("store_admin", "admin", "employee"):
                errors.append("種別（管理者/従業員）を選択してください。")
            if not name:
                errors.append("氏名を入力してください。")
            if not login_id:
                errors.append("ログインIDを入力してください。")
            if not password:
                errors.append("パスワードを入力してください。")
            if password != password2:
                errors.append("確認用パスワードが一致しません。")

            # 店舗内ユニークチェック
            if role in ("store_admin", "admin"):
                exists = s.query(Admin.id).filter(
                    Admin.store_id == store_id,
                    Admin.login_id == login_id
                ).first()
                if exists:
                    errors.append("同じログインIDの店舗管理者が既に存在します。")
            else:
                exists = s.query(Employee.id).filter(
                    Employee.store_id == store_id,
                    Employee.login_id == login_id
                ).first()
                if exists:
                    errors.append("同じログインIDの従業員が既に存在します。")

            if errors:
                return render_template("member_new.html", errors=errors, form={
                    "role": role, "name": name, "login_id": login_id, "is_active": is_active
                })

            pw_hash = generate_password_hash(password)

            if role in ("store_admin", "admin"):
                s.add(Admin(
                    store_id=store_id, login_id=login_id,
                    password_hash=pw_hash, name=name, active=is_active,
                    created_at=now_str(), updated_at=now_str()
                ))
            else:
                s.add(Employee(
                    store_id=store_id, login_id=login_id,
                    password_hash=pw_hash, name=name, active=is_active,
                    role="staff", created_at=now_str(), updated_at=now_str()
                ))

            s.commit()
            flash("メンバーを追加しました。")
            return redirect(url_for("floor"))

        return render_template("member_new.html", errors=None, form={
            "role": "employee", "name": "", "login_id": "", "is_active": 1
        })
    finally:
        s.close()


# ---------------------------------------------------------------------
# 会計（簡易版 API）
# ---------------------------------------------------------------------

# --- 会計：単発完了（互換用） ---------------------------------------------------
@app.route("/admin/settle/<int:table_id>", methods=["POST"])
@require_any
def admin_settle(table_id):
    s = SessionLocal()
    try:
        order = (s.query(OrderHeader)
                 .filter(OrderHeader.table_id == table_id, OrderHeader.status.in_(["新規","調理中","提供済","会計中"]))
                 .order_by(OrderHeader.id.desc()).first())
        if not order:
            return jsonify({"ok": False, "error": "未会計の注文がありません"})
        order.status = "会計済"
        order.closed_at = now_str()
        t = s.get(TableSeat, table_id)
        if t:
            t.status = "空席"
        
        # 【★ 追加】会計完了時にQRコードトークンを無効化
        invalidate_qr_tokens_for_table(s, table_id)
        
        s.commit()
        return jsonify({"ok": True})
    finally:
        s.close()



# ---------------------------------------------------------------------
# QR トークン（生成・検証・発行）／order_token ヘルパ
# ---------------------------------------------------------------------

# --- QRトークン生成 -------------------------------------------------------------
def gen_qr_token(table_id: int, ttl_minutes: int | None = None) -> str:
    s = SessionLocal()
    try:
        rand = base64.urlsafe_b64encode(os.urandom(9)).decode().rstrip('=')
        payload = f'{table_id}.{rand}'
        token_value = f'{payload}.{sign_payload(payload)}'
        exp = None
        if ttl_minutes and ttl_minutes > 0:
            exp = (datetime.now() + timedelta(minutes=ttl_minutes)).strftime('%Y-%m-%d %H:%M:%S')
        q = QrToken(table_id=table_id, token=token_value, expires_at=exp, issued_at=now_str())
        s.add(q)
        s.commit()
        return token_value
    finally:
        s.close()


# --- QRトークン無効化（テーブルID指定） -------------------------------------------
def invalidate_qr_tokens_for_table(s, table_id: int):
    """
    指定された table_id に紐づく全ての QR トークンを削除し、無効化する。
    """
    try:
        deleted_count = s.query(QrToken).filter(QrToken.table_id == table_id).delete(synchronize_session=False)
        current_app.logger.info(f"[QR_INVALIDATE] Deleted {deleted_count} tokens for table_id={table_id}")
        return deleted_count
    except Exception as e:
        current_app.logger.error(f"[QR_INVALIDATE] Failed to delete tokens for table_id={table_id}: {e}")
        return 0


# --- QRトークン検証 -------------------------------------------------------------
def verify_token(token: str) -> int:
    """
    【修正】DBの存在チェックと有効期限チェックを厳密に行う。
    署名検証は残すが、DBに存在しないトークンは無効とする。
    """
    try:
        # 1. 署名検証（改ざんチェック）
        parts = token.split('.')
        if len(parts) != 3:
            return 0
        table_id, rand, sig = parts
        payload = f'{table_id}.{rand}'
        if sign_payload(payload) != sig:
            current_app.logger.warning(f"[QR_VERIFY] Signature mismatch for {token}")
            return 0
        
        # 2. DB存在チェックと有効期限チェック
        s = SessionLocal()
        try:
            # トークンがDBに存在し、かつ期限切れでないことを確認
            row = s.query(QrToken).filter(QrToken.token == token).first()
            if not row:
                current_app.logger.info(f"[QR_VERIFY] Token not found in DB: {token}")
                return 0
            
            # 有効期限チェック（expires_at が存在し、かつ現在時刻より過去の場合）
            if row.expires_at:
                # タイムゾーンを考慮した比較を行う（row.expires_atがタイムゾーン情報を持つ前提）
                # 既存コードの datetime.now() はタイムゾーンなしなので、UTCとして扱う
                if isinstance(row.expires_at, datetime):
                    exp_dt = row.expires_at
                else:
                    exp_dt = datetime.strptime(row.expires_at, '%Y-%m-%d %H:%M:%S')
                
                # 比較のためにタイムゾーンを付与（UTCとして扱う）
                now_utc = datetime.now(timezone.utc).replace(tzinfo=None) # DB保存形式に合わせる
                if exp_dt < now_utc:
                    current_app.logger.info(f"[QR_VERIFY] Token expired: {token}")
                    return 0
                    
            # 3. 成功: table_id を返す
            return int(row.table_id)
        finally:
            s.close()
    except Exception as e:
        current_app.logger.exception(f"[QR_VERIFY] Error during verification: {e}")
        return 0



# ---------------------------------------------------------------------
# セルフオーダー（恒久QRコード対応）
# ---------------------------------------------------------------------
# --- [ルート] 恒久QRコード対応：テーブルIDからトークンを生成・リダイレクト ----------------
@app.route("/t/<tenant_slug>/table/<int:table_id>")
def menu_page_by_table(tenant_slug, table_id):
    s = SessionLocal()
    try:
        # 【★ 修正】有効なトークンがあるかどうかにかかわらず、常に新しいトークンを生成し、
        #            古いトークンは全て無効化する（最も安全な運用）
        
        # 1. 強制的に既存のトークンを全て無効化
        invalidate_qr_tokens_for_table(s, table_id)
        s.commit() # トークン生成前にコミットが必要
        
        # 2. 新しいトークンを生成
        #    ※ TTL (Time To Live) は運用に合わせて任意で設定
        new_token = gen_qr_token(table_id, ttl_minutes=60 * 24) # 24時間有効で発行
        
        # 3. 新しいトークンでリダイレクト
        current_app.logger.info(f"[QR_PERM] Generated new token for table={table_id}. Redirecting.")
        return redirect(url_for("menu_page", tenant_slug=tenant_slug, token=new_token))
        
    except Exception as e:
        # current_app がない環境でのエラーを避けるため、try-exceptで囲む
        try:
            current_app.logger.exception(f"[QR_PERM] Error processing permanent QR for table={table_id}: {e}")
        except NameError:
            pass
        return "システムエラーが発生しました。", 500
    finally:
        s.close()



# --- QRトークン発行（管理） -----------------------------------------------------
@app.route("/admin/qrtoken/new", methods=["POST"])
@require_admin
def admin_new_token():
    table_id = int(request.form["table_id"])
    ttl = request.form.get("ttl_minutes")
    ttl_minutes = int(ttl) if ttl else None
    token = gen_qr_token(table_id, ttl_minutes)

    tenant_slug = session.get("tenant_slug") or DEFAULT_TENANT_SLUG
    url = url_for("menu_page", tenant_slug=tenant_slug, token=token, _external=True)

    accept = request.headers.get("Accept", "")
    if "application/json" in accept or request.headers.get("X-Requested-With") in ("XMLHttpRequest", "fetch"):
        return jsonify({"ok": True, "url": url})
    else:
        flash("QR を発行しました。テーブルカードに最新リンクを表示します。")
        return redirect(url_for("floor"))


# ===== 合流PIN・時刻・Cookie ヘルパ =====
# Note: These helpers manage the join PIN, time conversions, and cookie issuance.
import secrets
import random
from datetime import datetime, timedelta, timezone
from flask import request, make_response, current_app

PIN_TTL_MINUTES = 120  # PIN有効期限（分）
# Special value for join_pin used when a staff member starts the order.
# When the first guest arrives, this value will trigger automatic join and be converted to a real PIN.
STAFF_BYPASS_PIN = "STAFF"

def _now_utc() -> datetime:
    """Return current UTC time as tz-aware datetime."""
    return datetime.now(timezone.utc)

def _iso_utc(dt: datetime) -> str:
    """Convert a datetime to ISO8601 string with Z suffix in UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _parse_any_dt(value) -> datetime | None:
    """Normalize various date/time representations to tz-aware UTC datetime."""
    if not value:
        return None
    if isinstance(value, datetime):
        return (value.astimezone(timezone.utc)
                if value.tzinfo else value.replace(tzinfo=timezone.utc))
    s = str(value).strip()
    # Try ISO8601 (replace Z)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        pass
    # Try with and without timezone offset
    for fmt in ("%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s[:19] if "%z" not in fmt else s, fmt)
            if "%z" in fmt:
                return dt.astimezone(timezone.utc)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None

def _format_expiry_for_display(exp_value: str | datetime | None) -> str | None:
    """
    Convert expiry datetime to a human-readable string in JST (Asia/Tokyo).
    Accepts ISO strings or naive datetimes.
    """
    if not exp_value:
        return None
    dt = _parse_any_dt(exp_value)
    if not dt:
        return None
    try:
        from zoneinfo import ZoneInfo  # Python 3.9+
        jst = dt.astimezone(ZoneInfo("Asia/Tokyo"))
    except Exception:
        jst = (dt + timedelta(hours=9)).replace(tzinfo=None)
    return jst.strftime("%Y/%m/%d %H:%M")

def _new_join_pin() -> str:
    """Generate a 4-digit PIN as a string."""
    return f"{random.randint(0, 9999):04d}"

def _pin_expired(header) -> bool:
    """Return True if the PIN on the given header has expired."""
    exp_dt = _parse_any_dt(getattr(header, "join_pin_expires_at", None))
    if not exp_dt:
        return True
    return _now_utc() > exp_dt

def _ensure_pin(header):
    """
    Ensure the header has a valid join_pin and expiry.
    If missing or expired, generate a new PIN and set the expiry.
    """
    if (not getattr(header, "join_pin", None)) or _pin_expired(header):
        header.join_pin = _new_join_pin()
        header.join_pin_expires_at = _iso_utc(_now_utc() + timedelta(minutes=PIN_TTL_MINUTES))
    return header

# -- Staff bypass helpers -------------------------------------------------------
def _convert_staff_bypass_to_real_pin(header) -> bool:
    """
    If the given header currently has a staff bypass PIN (STAFF_BYPASS_PIN),
    rotate it to a normal 4-digit PIN with a fresh expiry and return True.
    Otherwise do nothing and return False.
    """
    if getattr(header, "join_pin", None) == STAFF_BYPASS_PIN:
        header.join_pin = _new_join_pin()
        header.join_pin_expires_at = _iso_utc(_now_utc() + timedelta(minutes=PIN_TTL_MINUTES))
        return True
    return False

def _is_staff_session() -> bool:
    """
    Returns True if the current session role corresponds to staff or higher.
    This helper parallels is_staff_or_higher but avoids name conflicts.
    """
    role = (session.get("role") or "").lower()
    return role in {
        "staff",
        "store_staff",
        "store_admin",
        "tenant_admin",
        "sysadmin",
        "admin",
        "manager",
    }

def _get_order_by_token(s, token: str):
    """Retrieve an order by its session_token (order_token)."""
    if not token:
        return None
    return s.query(OrderHeader).filter(OrderHeader.session_token == token).first()

def _issue_order_token_if_needed(resp, header, s=None):
    """
    Issue the order_token cookie and persist header.session_token.
    Use provided SQLAlchemy session s to flush/commit.
    """
    token = getattr(header, "session_token", None)
    if not token:
        token = secrets.token_urlsafe(24)
        header.session_token = token
        if s is not None:
            try:
                s.add(header)
                s.flush()
                s.commit()
            except Exception:
                current_app.logger.exception("[order_token] persist failed with provided session")
        else:
            # Use short-lived session if no session provided
            s2 = SessionLocal()
            try:
                s2.merge(header)
                s2.commit()
            except Exception:
                current_app.logger.exception("[order_token] persist failed (short session)")
            finally:
                try:
                    s2.close()
                except Exception:
                    pass

    secure_flag = bool(request.is_secure or request.headers.get("X-Forwarded-Proto", "http") == "https")
    resp.set_cookie(
        "order_token",
        token,
        max_age=60 * 60 * 8,
        path="/",
        secure=secure_flag,
        httponly=True,
        samesite="Lax",
    )
    return resp


# Helper: determine if current session role is staff or higher
def is_staff_or_higher() -> bool:
    """
    Returns True if the current session role corresponds to a staff or higher user.
    Staff roles bypass join PIN requirement on the menu page.
    """
    role = (session.get("role") or "").lower()
    return role in {
        "staff",
        "store_staff",
        "store_admin",
        "tenant_admin",
        "sysadmin",
        "admin",
        "manager",
    }


# ---------------------------------------------------------------------
# 空の注文ヘッダを安全に掃除する（明細0件 or 全キャンセルなら物理削除）
#
# ・未会計かつ…
#   - 明細が1件も無い もしくは
#   - 正数量の「未キャンセル」明細が1件も無い（= 全キャンセル）
#   場合のみ削除します。
# ・会計済み / クローズ済み は触りません。
# 戻り値: True=削除した / False=削除しなかった
# ---------------------------------------------------------------------
def _delete_order_if_empty(s: Session, header: "OrderHeader") -> bool:
    if not header:
        return False

    # 会計済・クローズは触らない
    if getattr(header, "status", "") == "会計済" or getattr(header, "closed_at", None):
        return False

    # 明細を取得
    try:
        items = s.query(OrderItem).filter(OrderItem.order_id == header.id).all()
    except Exception:
        # モデルが取れない等は安全側で削除しない
        return False

    # 明細0件 → 削除可
    if not items:
        can_delete = True
    else:
        active_qty_sum = 0
        for it in items:
            try:
                qty = int(getattr(it, "qty", 0) or 0)
            except Exception:
                qty = 0

            # キャンセル判定
            is_cancel = False
            try:
                is_cancel = _is_cancel_item(it)
            except Exception:
                st = (getattr(it, "status", None) or "").lower()
                is_cancel = any(k in st for k in ("取消", "ｷｬﾝｾﾙ", "キャンセル", "cancel", "void"))

            if is_cancel and qty > 0:
                continue
            active_qty_sum += qty

        can_delete = (active_qty_sum <= 0)

    if not can_delete:
        return False

    # セッショントークン類をクリアしてから物理削除
    try:
        header.session_token = None
        header.join_pin = None
        header.join_pin_expires_at = None
        s.delete(header)
        s.commit()
        current_app.logger.info("[reset-session] deleted order header id=%s (empty/net-zero)", header.id)
        return True
    except Exception:
        s.rollback()
        current_app.logger.exception("[reset-session] failed to delete order header id=%s", header.id)
        return False


# -------------------------------------------------------------
# 注文に紐づく「お客様詳細」を初期化（存在すれば安全に）
#  - 単一カラム型: customer_name / customer_phone / guest_count などを None/0 に
#  - T_お客様詳細: order_id / table_id に紐づくレコードを物理削除
# -------------------------------------------------------------
def _reset_customer_info_for_order(s: Session, header: "OrderHeader") -> None:
    if not header:
        return

    # --- OrderHeader 側の単一フィールドをクリア（存在すれば） ---
    for name, val in [
        ("customer_id", None),
        ("customer_name", None),
        ("customer_phone", None),
        ("customer_email", None),
        ("customer_note", None),
        ("guest_count", 0),
        ("人数", 0),
        ("人数合計", 0),
    ]:
        if hasattr(header, name):
            try:
                setattr(header, name, val)
            except Exception:
                pass

    # --- T_お客様詳細 を物理削除 ---
    try:
        if hasattr(header, "id") and header.id is not None:
            s.query(T_お客様詳細).filter(T_お客様詳細.order_id == header.id).delete(synchronize_session=False)
            s.flush()

        if getattr(header, "table_id", None):
            s.query(T_お客様詳細).filter(
                T_お客様詳細.table_id == header.table_id,
                T_お客様詳細.order_id == None  # noqa: E711
            ).delete(synchronize_session=False)
            s.flush()
    except Exception:
        current_app.logger.exception("[reset-session] delete T_お客様詳細 failed (best-effort)")


# -------------------------------------------------------------
# テーブル単位で T_お客様詳細 を物理削除（フォールバック）
# -------------------------------------------------------------
def _purge_customer_detail_for_table(s: Session, *, table_id: int):
    Model = globals().get("T_お客様詳細")
    if not Model or table_id is None:
        return
    try:
        n = s.query(Model).filter(Model.table_id == table_id).delete(synchronize_session=False)
        s.flush()
        current_app.logger.info("[reset-session] purged T_お客様詳細 by table_id=%s, n=%s", table_id, n)
    except Exception:
        current_app.logger.exception("[reset-session] purge by table_id failed")


# ---------------------------------------------------------------------
# セッション初期化（スタッフ以上専用）
# ---------------------------------------------------------------------
@app.route("/orders/<int:order_id>/reset_session", methods=["POST"])
def reset_order_session(order_id: int):
    if not is_staff_or_higher():
        abort(403)

    wants_json = "application/json" in (request.headers.get("Accept", "") or "").lower()
    s = SessionLocal()
    try:
        header = s.get(OrderHeader, order_id)
        if not header:
            if wants_json:
                return jsonify({"ok": False, "error": "order_not_found"}), 404
            flash("セッション初期化に失敗：注文が見つかりません。", "error")
            return redirect(url_for("floor"))

        # --- お客様詳細を初期化 ---
        try:
            _reset_customer_info_for_order(s, header)
            if getattr(header, "table_id", None):
                _purge_customer_detail_for_table(s, table_id=header.table_id)
            s.flush()
        except Exception:
            current_app.logger.exception("[reset-session] customer info reset failed")

        # --- 空 or 全キャンセルなら削除 ---
        deleted = _delete_order_if_empty(s, header)
        if not deleted:
            if wants_json:
                return jsonify({"ok": False, "error": "not_empty_or_closed"}), 400
            flash("セッション初期化に失敗：明細が残っているか、会計済み/クローズ済みです。", "error")
            return redirect(url_for("floor"))

        # --- 成功: Cookie削除 + フロアへリダイレクト ---
        try:
            mark_floor_changed()
        except Exception:
            pass

        if wants_json:
            resp = make_response(jsonify({"ok": True, "redirect": url_for("floor")}))
            for name in ("order_token", "customer_token", "guest_token"):
                resp.delete_cookie(name, path="/")
            return resp

        flash("未注文セッションを初期化しました。", "success")
        resp = redirect(url_for("floor"))
        for name in ("order_token", "customer_token", "guest_token"):
            resp.delete_cookie(name, path="/")
        return resp

    finally:
        try:
            s.close()
        except Exception:
            pass



# ---------------------------------------------------------------------
# セルフオーダー（来店客）ページ表示
# ---------------------------------------------------------------------
# --- [ルート] セルフオーダー（来店客）ページ表示 --------------------------------
@app.route("/t/<tenant_slug>/m/<token>")
def menu_page(tenant_slug, token):
    s = SessionLocal()
    try:
        qt = s.query(QrToken).filter_by(token=token).first()
        if not qt:
            return "無効なQRです。", 410

        store_id = qt.store_id
        if not store_id:
            return "QRトークンに店舗IDが紐づいていません。", 500

        # --- 1) cookie の注文が今回の QR と不整合なら捨てる --------------------------------
        order_token = request.cookies.get("order_token")
        header = _get_order_by_token(s, order_token)

        if header and (
            getattr(header, "table_id", None) != qt.table_id
            or getattr(header, "store_id", None) != store_id
        ):
            # 別テーブル/別店舗 → 使わない
            header = None

        # --- 2) 同テーブルの注文を検索 ------------------------------------
        if not header:
            # アクティブな注文を検索（会計中も含む）
            # 会計済みの注文も検索対象に含めて、次のブロックで処理する
            open_statuses = ["新規", "調理中", "提供中", "提供済", "会計中", "open", "pending", "in_progress", "serving", "会計済", "closed"]
            header = (
                s.query(OrderHeader)
                 .filter(
                     OrderHeader.table_id == qt.table_id,
                     OrderHeader.status.in_(open_statuses)
                 )
                 .order_by(OrderHeader.id.desc())
                 .first()
            )
            
        # 【★ 追加】会計済みの注文に紐づく order_token が残っていたら削除してリダイレクト
        # 注文が会計済み（status="会計済" または closed_at がセットされている）かつ
        # セッションに紐づく order_token が存在する場合に処理
        if header and (getattr(header, "status", "") == "会計済" or getattr(header, "closed_at", None)):
            # order_token が Cookie に存在する場合のみ削除処理を行う
            if request.cookies.get("order_token"):
                resp = make_response(redirect(url_for("menu_page", tenant_slug=tenant_slug, token=token)))
                resp.delete_cookie("order_token")
                current_app.logger.info(f"[MENU_PAGE] Deleted order_token for closed order: {header.id}")
                return resp
            # order_token が存在しない場合は、新しい注文を始めるために header を None にリセット
            header = None

        # スタッフ以上の場合はPIN不要（合流PINロジックをスキップ）
        if is_staff_or_higher():
            # スタッフ用: 注文ヘッダが無ければ新規作成しPINを発行（非表示）
            if not header:
                header = OrderHeader(
                    table_id=qt.table_id,
                    store_id=store_id,
                    status="新規",
                    opened_at=now_str(),
                )
                s.add(header)
                s.flush()
                # Mark header as started by staff: sentinel join_pin; do not set expiry.
                header.join_pin = STAFF_BYPASS_PIN
                # For staff-started orders we do not set an expiry until converted.
                header.join_pin_expires_at = None
                s.commit()
            # メニュー・カテゴリ等を構築して即戻る（PINは表示しない）
            store_info = s.get(Store, store_id)
            categories_raw = (
                s.query(Category)
                 .filter(Category.store_id == store_id, Category.active == 1)
                 .order_by(Category.parent_id, Category.display_order)
                 .all()
            )
            categories_json_safe = [
                {"id": c.id, "name": c.name, "parent_id": (c.parent_id if c.parent_id is not None else None)}
                for c in categories_raw
            ]
            from collections import defaultdict
            category_tree = defaultdict(list)
            for c in categories_json_safe:
                parent_key = str(c["parent_id"]) if c["parent_id"] is not None else "root"
                category_tree[parent_key].append(c)
            menus = []
            if "root" in category_tree and category_tree["root"]:
                first_category_id = category_tree["root"][0]["id"]
                menu_ids = s.query(ProductCategoryLink.product_id) \
                            .filter(ProductCategoryLink.category_id == first_category_id).all()
                if menu_ids:
                    menu_ids = [m[0] for m in menu_ids]
                    menus = (
                        s.query(Menu)
                         .filter(Menu.id.in_(menu_ids), Menu.available == 1)
                         .order_by(Menu.display_order)
                         .all()
                    )
            menu_list = []
            for m in menus:
                eff_rate = resolve_effective_tax_rate_for_menu(s, m.id, m.tax_rate)
                price_excl = int(m.price)
                price_incl = display_price_incl_from_excl(price_excl, eff_rate)
                menu_list.append({
                    "id": m.id,
                    "name": m.name,
                    "price_excl": price_excl,
                    "price_incl": price_incl,
                    "photo_url": m.photo_url,
                    "description": m.description,
                    "is_market_price": getattr(m, "時価", 0),
                })
            template_vars = {
                "store": store_info,
                "store_name": store_info.name if store_info else "",
                "table": {"id": qt.table_id},
                "order": {"id": header.id},
                "categories": categories_json_safe,
                "category_tree": dict(category_tree),
                "menus": menu_list,
                "csrf_token": session.get("csrf_token"),
                "qr_token": token,
                "show_join_pin": False,
                "join_pin": None,
                "join_pin_expires_at": None,
                "tenant_slug": tenant_slug,
                "token": token,
                # スタッフ以上かどうかをテンプレートに渡す
                "staff_mode": is_staff_or_higher(),
            }
            resp = make_response(render_template("menu_page.html", **template_vars))
            return _issue_order_token_if_needed(resp, header, s)

        # ---------------------------------------------------------------------
        # 来店客の場合: この店舗で合流PINが必要かどうかを判定
        # デフォルトでは 1 (=必須) とし、Store に require_join_pin 列が存在する場合のみその値を参照します。
        require_pin = 1
        try:
            store_rec = s.get(Store, store_id)
            if store_rec is not None and hasattr(store_rec, "require_join_pin"):
                # 0 または falsy 値の場合は PIN 不要とみなす
                require_pin = store_rec.require_join_pin or 0
        except Exception:
            # 何らかの例外が発生しても PIN 必須とする
            require_pin = 1

        # === 合流PIN ロジック開始 ===
        # この端末に紐づく cookie が現在の注文と一致しているかどうか
        has_cookie_for_this_order = False
        if header:
            has_cookie_for_this_order = bool(order_token and (order_token == getattr(header, "session_token", None)))

        # 合流PIN関連のフラグと値を初期化
        show_join_pin = False
        join_pin_value = None
        join_pin_expires = None

        # A) ヘッダが無い場合（=最初の来店客）
        if not header:
            header = OrderHeader(
                table_id=qt.table_id,
                store_id=store_id,
                status="新規",
                opened_at=now_str(),
            )
            s.add(header)
            s.flush()
            if require_pin:
                # 初回アクセス時にPINを発行
                _ensure_pin(header)
                s.commit()
                show_join_pin = True
                join_pin_value = header.join_pin
                join_pin_expires = _format_expiry_for_display(header.join_pin_expires_at)
            else:
                # PIN不要の場合はPINを発行・表示しない
                s.commit()
                show_join_pin = False
                join_pin_value = None
                join_pin_expires = None

        # B) ヘッダは存在するが、この端末はまだ注文に参加していない場合
        elif not has_cookie_for_this_order:
            # 店舗設定でPINが不要な場合、PIN確認無しで自動的に合流する
            if not require_pin:
                resp = make_response(
                    redirect(url_for("menu_page", tenant_slug=tenant_slug, token=token))
                )
                return _issue_order_token_if_needed(resp, header, s)
            # If this is a guest (non-staff) and the order was started by staff (STAFF_BYPASS_PIN),
            # automatically convert the sentinel to a real PIN and join the guest.
            if getattr(header, "join_pin", None) == STAFF_BYPASS_PIN:
                _convert_staff_bypass_to_real_pin(header)
                s.commit()
                resp = make_response(redirect(url_for("menu_page", tenant_slug=tenant_slug, token=token)))
                return _issue_order_token_if_needed(resp, header, s)

            # Otherwise enforce normal PIN logic: update the PIN if expired
            _ensure_pin(header)
            s.commit()
            pin_input = (request.args.get("join_pin") or "").strip()
            if pin_input:
                if (pin_input == str(header.join_pin)) and (not _pin_expired(header)):
                    # PIN一致 → Cookie付与してリダイレクト
                    resp = make_response(redirect(url_for("menu_page", tenant_slug=tenant_slug, token=token)))
                    return _issue_order_token_if_needed(resp, header, s)
                else:
                    # PIN不一致または期限切れ → 入力画面にエラーメッセージを表示
                    return render_template(
                        "join_pin_input.html",
                        tenant_slug=tenant_slug,
                        token=token,
                        order_id=header.id,
                        pin_expired=_pin_expired(header),
                    )
            # PIN未入力 → 入力フォーム表示
            return render_template(
                "join_pin_input.html",
                tenant_slug=tenant_slug,
                token=token,
                order_id=header.id,
                pin_expired=_pin_expired(header),
            )

        # C) ヘッダがあり、この端末はすでに注文に参加済み → show_join_pinはFalse
        else:
            show_join_pin = False
            # join_pin_valueおよびexpiresはNoneのまま

        # === 合流PIN ロジック終了 ===

        store_info = s.get(Store, store_id)

        # ===== カテゴリ・メニュー構築（既存ロジック） =====
        categories_raw = (
            s.query(Category)
             .filter(Category.store_id == store_id, Category.active == 1)
             .order_by(Category.parent_id, Category.display_order)
             .all()
        )

        # JSON セーフに整形
        categories_json_safe = [
            {"id": c.id, "name": c.name, "parent_id": (c.parent_id if c.parent_id is not None else None)}
            for c in categories_raw
        ]

        from collections import defaultdict
        category_tree = defaultdict(list)
        for c in categories_json_safe:
            parent_key = str(c["parent_id"]) if c["parent_id"] is not None else "root"
            category_tree[parent_key].append(c)

        # 最初のルートカテゴリのメニューを用意（任意）
        menus = []
        if "root" in category_tree and category_tree["root"]:
            first_category_id = category_tree["root"][0]["id"]
            menu_ids = s.query(ProductCategoryLink.product_id) \
                        .filter(ProductCategoryLink.category_id == first_category_id).all()
            if menu_ids:
                menu_ids = [m[0] for m in menu_ids]
                menus = (
                    s.query(Menu)
                     .filter(Menu.id.in_(menu_ids), Menu.available == 1)
                     .order_by(Menu.display_order)
                     .all()
                )

        menu_list = []
        for m in menus:
            eff_rate = resolve_effective_tax_rate_for_menu(s, m.id, m.tax_rate)
            price_excl = int(m.price)
            price_incl = display_price_incl_from_excl(price_excl, eff_rate)
            menu_list.append({
                "id": m.id,
                "name": m.name,
                "price_excl": price_excl,
                "price_incl": price_incl,
                "photo_url": m.photo_url,
                "description": m.description,
            })

        template_vars = {
            "store": store_info,
            "store_name": store_info.name if store_info else "",
            "table": {"id": qt.table_id},
            "order": {"id": header.id},
            "categories": categories_json_safe,
            "category_tree": dict(category_tree),
            "menus": menu_list,
            "csrf_token": session.get("csrf_token"),
            "qr_token": token,  # QRトークンをテンプレートに渡す
            # --- 合流PIN表示制御をテンプレートに渡す ---
            "show_join_pin": show_join_pin,
            "join_pin": join_pin_value,
            "join_pin_expires_at": join_pin_expires,
            "tenant_slug": tenant_slug,
            "token": token,
            # スタッフ以上かどうかをテンプレートに渡す
            "staff_mode": is_staff_or_higher(),
        }

        resp = make_response(render_template("menu_page.html", **template_vars))
        # 正しい注文のセッショントークンを Cookie にセット
        return _issue_order_token_if_needed(resp, header, s)

    finally:
        s.close()



# ---------------------------------------------------------------------
# 注文API（パブリック：QR側）
# ---------------------------------------------------------------------
# --- [API] 注文API（パブリック：QR側） --------------------------------
@app.route("/api/order", methods=["POST"])
def api_order():
    """
    パブリック（QR）側の注文API
    POST JSON:
      { "token": "<qr token>", "items": [{"menu_id": 3, "qty": 2, "memo": "辛め"}] }
    """
    import math
    import logging

    data = request.get_json(force=True) or {}
    token = (data.get("token") or "").strip()
    items = data.get("items") or []
    if not token or not isinstance(items, list) or not items:
        return jsonify({"ok": False, "error": "token/items required"}), 400

    # 💡 verify_tokenから取得したtable_idをログに出力
    table_id = verify_token(token)
    app.logger.info(f"DEBUG: verify_tokenから取得したtable_id: {table_id}")

    if not table_id:
        return jsonify({"ok": False, "error": "invalid token"}), 403

    try:
        table_id = int(table_id)
    except Exception:
        return jsonify({"ok": False, "error": "invalid table_id"}), 400

    s = SessionLocal()
    try:
        t = s.get(TableSeat, table_id)
        if not t:
            # 💡 table_idが有効なIDでない場合はエラーを返す
            app.logger.warning(f"table not found for table_id: {table_id}")
            return jsonify({"ok": False, "error": "table not found"}), 404

        # === 追加：この時点で store_id / tenant_id を確定（新規ヘッダ作成で必須） ==========
        store_id = (
            getattr(t, "store_id", None)
            or session.get("store_id")
            or (current_store_id() if "current_store_id" in globals() else None)
        )
        tenant_id = getattr(t, "tenant_id", None) or session.get("tenant_id")
        if store_id is None:
            app.logger.error("[api_order] store_id could not be resolved (table_id=%s)", table_id)
            return jsonify({"ok": False, "error": "store not resolved"}), 500
        # =======================================================================

        # クッキーから注文トークンを取得
        order_token = request.cookies.get("order_token")
        order = None
        
        # クッキーに注文トークンがある場合、それを使用
        if order_token:
            order = _get_order_by_token(s, order_token)
            # 注文が会計済みの場合はエラー
            if order:
                status = getattr(order, "status", "").lower()
                if status in {"closed", "settled", "paid", "canceled", "cancelled", "void", "会計済", "支払済", "支払い済", "クローズ", "終了"}:
                    return jsonify({"ok": False, "error": "このセッションは終了しました"}), 410
                # テーブルが一致しない場合もエラー
                if getattr(order, "table_id", None) != table_id:
                    return jsonify({"ok": False, "error": "セッションが無効です"}), 403
        
        # 既存アクティブオーダー検索（クッキーにない場合）
        if not order:
            order = (
                s.query(OrderHeader)
                 .filter(
                     OrderHeader.table_id == table_id,
                     OrderHeader.status.in_(["新規", "調理中", "提供済", "会計中"])
                 )
                 .order_by(OrderHeader.id.desc())
                 .first()
            )

        created_new_order = False
        if not order:
            # ★ 新規ヘッダ作成時に store_id / tenant_id / session_token を必ず埋める
            kwargs = dict(
                store_id=store_id,
                table_id=table_id,
                status="新規",
                subtotal=0,
                tax=0,
                total=0,
                opened_at=now_str(),
            )
            if tenant_id is not None:
                kwargs["tenant_id"] = tenant_id
            # session_token 列がある場合のみセット
            if hasattr(OrderHeader, "session_token"):
                kwargs["session_token"] = (order_token or token)

            order = OrderHeader(**kwargs)
            s.add(order)
            try:
                t.status = "着席"
            except Exception:
                app.logger.debug("[api_order] could not set table status", exc_info=True)
            s.flush()  # ← order.id を確定（ここで NOT NULL 違反していた）
            created_new_order = True
            app.logger.info("[api_order] created new order id=%s (table_id=%s, store_id=%s, tenant_id=%s)",
                            order.id, table_id, store_id, tenant_id)
        else:
            # 既存ヘッダに store_id / tenant_id が無ければ補完しておく（将来の安全策）
            try:
                changed = False
                if hasattr(order, "store_id") and getattr(order, "store_id", None) is None and store_id is not None:
                    order.store_id = store_id; changed = True
                if hasattr(order, "tenant_id") and getattr(order, "tenant_id", None) is None and tenant_id is not None:
                    order.tenant_id = tenant_id; changed = True
                if changed:
                    s.flush()
            except Exception:
                app.logger.debug("[api_order] could not backfill store/tenant for existing header", exc_info=True)

            app.logger.info("[api_order] reuse active order id=%s (table_id=%s)", order.id, table_id)

        # ========= ここから【顧客詳細へ order_id 紐付け + 詳細ログ】 =========
        try:
            # モデル解決（T_お客様詳細 / M_顧客詳細 / CustomerDetail のどれか）
            TCustomerDetail = (
                globals().get("T_お客様詳細")
                or globals().get("M_顧客詳細")
                or globals().get("CustomerDetail")
            )
            if TCustomerDetail is None:
                app.logger.warning("[api_order] TCustomerDetail model not found (T_お客様詳細 / M_顧客詳細 / CustomerDetail)")
            else:
                app.logger.debug(
                    "[api_order] TCustomerDetail=%s (has order_id? %s, has table_id? %s, has store_id? %s)",
                    getattr(TCustomerDetail, "__tablename__", str(TCustomerDetail)),
                    hasattr(TCustomerDetail, "order_id"),
                    hasattr(TCustomerDetail, "table_id"),
                    hasattr(TCustomerDetail, "store_id"),
                )

                cd = None
                # 1) order_id で既存を探す
                if hasattr(TCustomerDetail, "order_id"):
                    cd = (
                        s.query(TCustomerDetail)
                         .filter(getattr(TCustomerDetail, "order_id") == order.id)
                         .order_by(getattr(TCustomerDetail, "id").desc())
                         .first()
                    )
                    app.logger.debug("[api_order] search by order_id=%s -> %s", order.id, "hit" if cd else "none")
                else:
                    app.logger.warning("[api_order] TCustomerDetail has no order_id column!")

                # 2) 無ければ table_id の孤児を拾う
                if cd is None and hasattr(TCustomerDetail, "table_id"):
                    q = s.query(TCustomerDetail).filter(getattr(TCustomerDetail, "table_id") == table_id)
                    if hasattr(TCustomerDetail, "order_id"):
                        q = q.filter(getattr(TCustomerDetail, "order_id") == None)  # noqa: E711
                        app.logger.debug("[api_order] searching orphan rows for table_id=%s", table_id)
                    cd = q.order_by(getattr(TCustomerDetail, "id").asc()).first()
                    app.logger.debug("[api_order] orphan search -> %s", "hit" if cd else "none")

                created_new_cd = False
                # 3) まだ無ければ新規作成
                if cd is None:
                    cd = TCustomerDetail()
                    created_new_cd = True
                    if hasattr(TCustomerDetail, "store_id") and store_id is not None:
                        setattr(cd, "store_id", store_id)
                    if hasattr(TCustomerDetail, "table_id"):
                        setattr(cd, "table_id", table_id)
                    # 人数列初期化（存在する列だけ）
                    for col in ("大人男性", "大人女性", "子ども男", "子ども女", "合計人数"):
                        if hasattr(TCustomerDetail, col):
                            setattr(cd, col, 0)
                    s.add(cd)
                    app.logger.info("[api_order] created new customer_detail (table_id=%s)", table_id)

                # 4) 最終セット（order_id / table_id / store_id）
                if hasattr(TCustomerDetail, "order_id"):
                    before = getattr(cd, "order_id", None)
                    setattr(cd, "order_id", order.id)
                    after = getattr(cd, "order_id", None)
                    app.logger.debug("[api_order] set cd.order_id: %s -> %s", before, after)
                else:
                    app.logger.warning("[api_order] cannot set order_id (column not present)")

                if hasattr(TCustomerDetail, "table_id"):
                    setattr(cd, "table_id", table_id)
                if hasattr(TCustomerDetail, "store_id") and store_id is not None:
                    setattr(cd, "store_id", store_id)

                s.flush()
                app.logger.info(
                    "[api_order] bound customer_detail id=%s to order=%s (created_new_order=%s created_new_cd=%s)",
                    getattr(cd, "id", None), order.id, created_new_order, created_new_cd
                )
        except Exception as e:
            app.logger.exception("[api_order] bind TCustomerDetail failed: %s", e)
        # ========= 顧客詳細紐付け ここまで =========

        # --- 明細作成に使う store_id は、上で確定した値をそのまま使う ---
        app.logger.debug("[api_order] store_id for order items: %s", store_id)

        subtotal = int(order.subtotal or 0)
        taxsum   = int(order.tax or 0)
        added    = 0
        new_items_for_print = []

        for it in items:
            try:
                mid = int(it.get("menu_id"))
                qty = int(it.get("qty", 1))
            except Exception:
                app.logger.debug("[api_order] skip item (invalid menu_id/qty): %s", it)
                continue
            if qty <= 0:
                app.logger.debug("[api_order] skip item (qty<=0): %s", it)
                continue

            memo = (it.get("memo") or "").strip()
            actual_price = it.get("actual_price")  # 時価商品の実際価格
            m = s.get(Menu, mid)
            if not (m and m.available == 1):
                app.logger.debug("[api_order] skip item (menu not available): id=%s", mid)
                continue

            rate = resolve_effective_tax_rate_for_menu(s, mid, m.tax_rate)
            unit = int(m.price)  # 税抜保存単価
            
            # 時価商品の場合、actual_priceを使用
            if actual_price is not None:
                unit = int(actual_price)
                app.logger.info("[api_order] market price item: menu_id=%s actual_price=%s", mid, unit)

            new_item = OrderItem(
                order_id=order.id,
                menu_id=mid,
                qty=qty,
                unit_price=unit,   # 税抜単価
                tax_rate=rate,
                memo=memo,
                status="新規",
                added_at=now_str(),
            )
            # 店舗IDを設定（Pythonの属性名を使用）
            if store_id is not None and hasattr(new_item, "store_id"):
                new_item.store_id = store_id
            
            # actual_priceをOrderItemに保存
            if actual_price is not None and hasattr(new_item, 'actual_price'):
                new_item.actual_price = int(actual_price)
            s.add(new_item)
            s.flush()  # IDを確定
            
            # 進捗データを初期化（新規=数量で開始）
            try:
                progress_seed_if_needed(s, new_item)
            except Exception as e:
                app.logger.warning("[api_order] progress_seed failed for item %s: %s", new_item.id, e)
            
            new_items_for_print.append(new_item)

            subtotal += unit * qty
            per_unit_tax = int(math.floor(unit * rate))
            taxsum += per_unit_tax * qty
            added  += 1

        if added == 0:
            app.logger.warning("[api_order] no valid items -> rollback")
            s.rollback()
            return jsonify({"ok": False, "error": "no valid items"}), 400

        order.subtotal = subtotal
        order.tax      = taxsum
        order.total    = subtotal + taxsum

        app.logger.debug("[api_order] commit: order_id=%s subtotal=%s tax=%s total=%s",
                         order.id, order.subtotal, order.tax, order.total)

        s.commit()
        mark_floor_changed()

        try:
            # 💡 非同期印刷を削除し、同期的に印刷ジョブを呼び出す
            trigger_print_job(order.id, items_to_print=new_items_for_print)
        except Exception:
            app.logger.exception("[api_order] failed to print")

        return jsonify({
            "ok": True,
            "order_id": order.id,
            "subtotal": order.subtotal,
            "tax": order.tax,
            "total": order.total
        })
    except Exception as e:
        s.rollback()
        app.logger.error("[api_order] error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": "internal error"}), 500
    finally:
        s.close()



# ---------------------------------------------------------------------
# アイテム追加API（パブリック／スタッフ共通の土台・未実装部あり）
# ---------------------------------------------------------------------
# --- [API] アイテム追加API（パブリック／スタッフ共通の土台） -------------------------------
@app.post("/api/order/add_item")
def api_add_item():
    s = SessionLocal()
    try:
        data = request.get_json(silent=True) or request.form
        order_id = data.get("order_id")
        header = None

        # --- 1) 明示の order_id があれば最初に試す ------------------------------------------
        if order_id:
            header = s.get(OrderHeader, int(order_id))

        # --- 2) ダメなら Cookie で復元 ------------------------------------------------------
        if not header:
            header = _get_order_by_token(s, request.cookies.get("order_token"))

        if not header:
            return jsonify(ok=False, error="order not found"), 404

        # …以降は header を基準に処理…
        ...
    finally:
        s.close()



# ========================================
# 1. テーブル移動履歴モデル
# ========================================

class T_テーブル移動履歴(Base):
    __tablename__ = "T_テーブル移動履歴"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column("テナントID", Integer, ForeignKey("M_テナント.id"), nullable=True)
    store_id = Column("店舗ID", Integer, ForeignKey("M_店舗.id"), nullable=True)
    
    # 移動情報
    moved_at = Column("移動日時", DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    from_table_id = Column("移動元テーブルID", Integer, nullable=False)
    to_table_id = Column("移動先テーブルID", Integer, nullable=True)  # swapの場合はNULL
    mode = Column("移動モード", String, nullable=False)  # deny/merge/merge_new/swap
    
    # 注文情報
    order_id = Column("注文ID", Integer, nullable=True)
    order_status = Column("注文状態", String, nullable=True)
    item_count = Column("明細数", Integer, nullable=True)
    
    # 金額情報
    subtotal = Column("小計", Integer, nullable=True)
    tax = Column("税額", Integer, nullable=True)
    total = Column("合計", Integer, nullable=True)
    paid = Column("既払", Integer, nullable=True)
    remaining = Column("残額", Integer, nullable=True)
    
    # 人数情報
    adult_male = Column("大人男性", Integer, nullable=True)
    adult_female = Column("大人女性", Integer, nullable=True)
    child_male = Column("子ども男", Integer, nullable=True)
    child_female = Column("子ども女", Integer, nullable=True)
    
    # 実行者
    staff_id = Column("スタッフID", Integer, nullable=True)
    staff_name = Column("スタッフ名", String, nullable=True)
    
    # メモ
    memo = Column("メモ", String, nullable=True)
    
    # 取消関連
    is_cancelled = Column("取消済み", Integer, default=0, nullable=False)  # 0=未取消, 1=取消済み
    cancelled_at = Column("取消日時", DateTime(timezone=True), nullable=True)
    cancelled_by_staff_id = Column("取消実行者ID", Integer, nullable=True)
    cancelled_by_staff_name = Column("取消実行者名", String, nullable=True)
    
    # 明細追跡（JSON形式）
    source_items_snapshot = Column("移動元明細JSON", Text, nullable=True)
    dest_items_snapshot = Column("移動先明細JSON", Text, nullable=True)
    
    # 追加の注文ID情報
    dest_order_id = Column("移動先注文ID", Integer, nullable=True)  # merge/swapの場合
    new_order_id = Column("新規注文ID", Integer, nullable=True)    # merge_newの場合



# ========================================
# 2. admin_table_move 関数に履歴記録処理を追加
# （既存の admin_table_move 関数の最後、s.commit() の前に追加）
# ========================================

def _record_table_move_history(s, sid, from_table_id, to_table_id, mode, order_from, order_to=None, new_order_id=None):
    """
    テーブル移動履歴を記録する
    
    Args:
        s: SQLAlchemy session
        sid: 店舗ID
        from_table_id: 移動元テーブルID
        to_table_id: 移動先テーブルID
        mode: 移動モード (move/merge/merge_new/swap)
        order_from: 移動元の注文オブジェクト
        order_to: 移動先の注文オブジェクト (merge/swapの場合)
        new_order_id: 新規作成された注文ID (merge_newの場合)
    
    Returns:
        history_id: 作成された履歴レコードのID (失敗時はNone)
    """
    try:
        THistory = globals().get("T_テーブル移動履歴")
        if THistory is None:
            current_app.logger.warning("[table_move_history] T_テーブル移動履歴 not found")
            return None
        
        TItem = globals().get("T_注文明細") or globals().get("OrderItem")
        TPay = globals().get("T_支払") or globals().get("PaymentRecord")
        TCD = globals().get("T_お客様詳細")
        TMenu = globals().get("Menu") or globals().get("M_メニュー")
        
        # 移動元の注文情報を取得
        order_id = getattr(order_from, "id", None) if order_from else None
        order_status = getattr(order_from, "status", None) if order_from else None
        
        # 明細数を取得
        item_count = 0
        if order_id and TItem:
            item_count = s.query(TItem).filter(getattr(TItem, "order_id") == order_id).count()
        
        # 金額情報を取得
        subtotal = getattr(order_from, "subtotal", None) or getattr(order_from, "小計", None) if order_from else None
        tax = getattr(order_from, "tax", None) or getattr(order_from, "税額", None) if order_from else None
        total = getattr(order_from, "total", None) or getattr(order_from, "合計", None) if order_from else None
        
        # 既払・残額を取得
        paid = 0
        if order_id and TPay:
            from sqlalchemy import func
            paid = s.query(func.coalesce(func.sum(getattr(TPay, "amount", None) or getattr(TPay, "金額", None)), 0)) \
                .filter(getattr(TPay, "order_id") == order_id).scalar() or 0
        
        remaining = int(total or 0) - int(paid or 0) if total else None
        
        # 人数情報を取得
        adult_male = None
        adult_female = None
        child_male = None
        child_female = None
        
        if TCD and order_id:
            cd = s.query(TCD).filter(getattr(TCD, "order_id") == order_id).first()
            if cd:
                adult_male = getattr(cd, "大人男性", None)
                adult_female = getattr(cd, "大人女性", None)
                child_male = getattr(cd, "子ども男", None)
                child_female = getattr(cd, "子ども女", None)
        
        # スタッフ情報を取得
        staff_id = session.get("user_id")
        staff_name = session.get("username")
        
        # ===== 明細スナップショットを作成 =====
        def _create_snapshot(order_obj):
            """注文の明細スナップショットをJSON形式で作成"""
            if not order_obj:
                return None
            
            oid = getattr(order_obj, "id", None)
            if not oid:
                return None
            
            snapshot = {
                "order_id": oid,
                "items": [],
                "customer_detail": {},
                "payments": []
            }
            
            # 明細情報
            if TItem:
                items = s.query(TItem).filter(getattr(TItem, "order_id") == oid).all()
                for item in items:
                    item_data = {
                        "id": getattr(item, "id", None),
                        "menu_id": getattr(item, "menu_id", None) or getattr(item, "メニューID", None),
                        "qty": getattr(item, "qty", None) or getattr(item, "数量", None),
                        "unit_price": getattr(item, "unit_price", None) or getattr(item, "単価", None),
                        "tax_rate": getattr(item, "tax_rate", None) or getattr(item, "税率", None),
                        "status": getattr(item, "status", None) or getattr(item, "状態", None),
                    }
                    
                    # メニュー名を取得
                    if TMenu and item_data["menu_id"]:
                        menu = s.get(TMenu, item_data["menu_id"])
                        if menu:
                            item_data["menu_name"] = getattr(menu, "name", None) or getattr(menu, "メニュー名", None)
                    
                    snapshot["items"].append(item_data)
            
            # お客様詳細
            if TCD:
                cd = s.query(TCD).filter(getattr(TCD, "order_id") == oid).first()
                if cd:
                    snapshot["customer_detail"] = {
                        "id": getattr(cd, "id", None),
                        "adult_male": getattr(cd, "大人男性", None),
                        "adult_female": getattr(cd, "大人女性", None),
                        "child_male": getattr(cd, "子ども男", None),
                        "child_female": getattr(cd, "子ども女", None),
                    }
            
            # 支払い情報
            if TPay:
                payments = s.query(TPay).filter(getattr(TPay, "order_id") == oid).all()
                for pay in payments:
                    snapshot["payments"].append({
                        "id": getattr(pay, "id", None),
                        "amount": getattr(pay, "amount", None) or getattr(pay, "金額", None),
                        "method": getattr(pay, "method", None) or getattr(pay, "支払方法", None),
                    })
            
            return json.dumps(snapshot, ensure_ascii=False)
        
        source_snapshot = _create_snapshot(order_from)
        dest_snapshot = _create_snapshot(order_to) if order_to else None
        
        # 履歴レコードを作成
        history = THistory()
        if hasattr(THistory, "tenant_id"):
            history.tenant_id = session.get("tenant_id")
        if hasattr(THistory, "store_id"):
            history.store_id = sid
        
        history.moved_at = datetime.now(timezone.utc)
        history.from_table_id = from_table_id
        history.to_table_id = to_table_id
        history.mode = mode
        
        history.order_id = order_id
        history.order_status = order_status
        history.item_count = item_count
        
        history.subtotal = int(subtotal) if subtotal else None
        history.tax = int(tax) if tax else None
        history.total = int(total) if total else None
        history.paid = int(paid) if paid else None
        history.remaining = int(remaining) if remaining else None
        
        history.adult_male = int(adult_male) if adult_male else None
        history.adult_female = int(adult_female) if adult_female else None
        history.child_male = int(child_male) if child_male else None
        history.child_female = int(child_female) if child_female else None
        
        history.staff_id = staff_id
        history.staff_name = staff_name
        
        # 新しい列を設定
        history.source_items_snapshot = source_snapshot
        history.dest_items_snapshot = dest_snapshot
        history.dest_order_id = getattr(order_to, "id", None) if order_to else None
        history.new_order_id = new_order_id
        
        s.add(history)
        s.flush()
        
        history_id = getattr(history, "id", None)
        
        current_app.logger.info("[table_move_history] recorded: id=%s from=%s to=%s mode=%s order=%s dest_order=%s new_order=%s",
                               history_id, from_table_id, to_table_id, mode, order_id, 
                               history.dest_order_id, history.new_order_id)
        
        return history_id
        
    except Exception as e:
        current_app.logger.exception("[table_move_history] failed to record: %s", e)
        # 履歴記録失敗は致命的ではないので続行



# ---------------------------------------------------------------------
# テーブル移動（スタッフ操作）
#   - mode: "deny"(既定) | "merge" | "swap"
#   - from_table のアクティブ注文を to_table に移動 / 併合 / 交換
#   - 直近QRトークンを付替（swap時は相互交換）
#   - 移動後：元テーブルのお客様詳細をリセット（孤児含む）
# ---------------------------------------------------------------------
# --- [スタッフ] テーブル移動：deny / merge / merge_new / swap -------------------
@app.route("/admin/table/move", methods=["POST"])
@require_staff
def admin_table_move():
    from sqlalchemy import func, and_
    from datetime import datetime, timezone
    s = SessionLocal()

    # ===== 内部ヘルパ =====
    def _models():
        TOrder = globals().get("T_注文") or globals().get("OrderHeader")
        TItem  = globals().get("T_注文明細") or globals().get("T_注文詳細") or globals().get("OrderItem")
        TPay   = globals().get("T_支払") or globals().get("PaymentRecord")
        TCD    = globals().get("T_お客様詳細")
        TQR    = globals().get("QrToken")
        return TOrder, TItem, TPay, TCD, TQR

    def _active_statuses():
        return ["新規", "調理中", "提供済", "会計中", "open", "pending", "in_progress", "serving", "unpaid"]

    def _get_active_order(Model, store_id, table_id):
        q = s.query(Model).filter(getattr(Model, "table_id") == table_id)
        if hasattr(Model, "store_id"):
            q = q.filter(getattr(Model, "store_id") == store_id)
        if hasattr(Model, "status"):
            q = q.filter(getattr(Model, "status").in_(_active_statuses()))
        return q.order_by(getattr(Model, "id").desc()).first()

    def _rebind_latest_qr(Qr, store_id, from_table_id, to_table_id):
        if Qr is None:
            return None
        changed = None
        try:
            if hasattr(Qr, "issued_at"):
                sub = (s.query(Qr.table_id, func.max(Qr.issued_at).label("mx"))
                         .filter(Qr.store_id == store_id, Qr.table_id == from_table_id)
                         .group_by(Qr.table_id)).subquery()
                latest = (s.query(Qr)
                            .join(sub, and_(Qr.table_id == sub.c.table_id,
                                            Qr.issued_at == sub.c.mx))
                            .filter(Qr.store_id == store_id).first())
            else:
                sub = (s.query(Qr.table_id, func.max(Qr.id).label("mx"))
                         .filter(Qr.store_id == store_id, Qr.table_id == from_table_id)
                         .group_by(Qr.table_id)).subquery()
                latest = (s.query(Qr)
                            .join(sub, and_(Qr.table_id == sub.c.table_id,
                                            Qr.id == sub.c.mx))
                            .filter(Qr.store_id == store_id).first())
            if latest:
                changed = {"id": getattr(latest, "id", None), "before": from_table_id, "after": to_table_id}
                latest.table_id = to_table_id
        except Exception:
            current_app.logger.exception("[table_move] QR rebind failed (from=%s to=%s)", from_table_id, to_table_id)
        return changed

    def _reset_customer_detail_for_table(TCD, table_id):
        if TCD is None:
            return {"orphans": 0, "by_table": 0}
        orphans = s.query(TCD).filter(TCD.table_id == table_id, TCD.order_id == None)\
                   .delete(synchronize_session=False)  # noqa: E711
        by_table = s.query(TCD).filter(TCD.table_id == table_id)\
                   .delete(synchronize_session=False)
        current_app.logger.info("[table_move][cleanup] table_id=%s -> orphans=%s, by_table=%s",
                                table_id, orphans, by_table)
        return {"orphans": orphans, "by_table": by_table}

    # ★ 固定：あなたの人数列名に合わせて“必ず合算”する
    FIXED_CD_NUMERIC_COLS = ["大人男性", "大人女性", "子ども男", "子ども女"]

    # ★ お客様詳細を order_id 単位で 1 行に合算（固定列版）
    def _coalesce_customer_detail(TCD, *, order_id: int, table_id: int):
        """
        同一 order_id の T_お客様詳細 を合算して 1 行にする。
        対象列は FIXED_CD_NUMERIC_COLS（= 大人男性/大人女性/子ども男/子ども女）。
        """
        if TCD is None or not order_id:
            return {"rows": 0, "into": None, "sums": {}}

        # 対象列の存在チェック
        numeric_cols = [c for c in FIXED_CD_NUMERIC_COLS if hasattr(TCD, c)]
        if not numeric_cols:
            current_app.logger.warning("[table_move][coalesce] no numeric columns found on T_お客様詳細")
            return {"rows": 0, "into": None, "sums": {}}

        rows = (s.query(TCD)
                  .filter(getattr(TCD, "order_id") == order_id)
                  .order_by(getattr(TCD, "id").asc())
                  .all())
        if not rows:
            return {"rows": 0, "into": None, "sums": {}}

        sums = {k: 0 for k in numeric_cols}
        for r in rows:
            for k in numeric_cols:
                try:
                    sums[k] += int(getattr(r, k) or 0)
                except Exception:
                    pass

        base = rows[-1]  # 最後の1行に合算結果を書き戻す
        setattr(base, "order_id", order_id)
        if table_id is not None and hasattr(base, "table_id"):
            setattr(base, "table_id", table_id)
        for k, v in sums.items():
            try:
                setattr(base, k, int(v))
            except Exception:
                pass

        # その他の行は削除
        for r in rows[:-1]:
            s.delete(r)

        s.flush()
        current_app.logger.info(
            "[table_move][coalesce] order=%s table=%s rows=%s -> into_id=%s sums=%s",
            order_id, table_id, len(rows), getattr(base, "id", None), sums
        )
        return {"rows": len(rows), "into": getattr(base, "id", None), "sums": sums}

    # 合計を明細から再計算してヘッダへ反映
    def _recalc_order_totals_from_items(order_id, TOrder, TItem):
        if not (TOrder and TItem and order_id):
            return None
        items = s.query(TItem).filter(getattr(TItem, "order_id") == order_id).all()

        def _num(x, default=0):
            try:
                return int(x)
            except Exception:
                try:
                    return float(x or 0)
                except Exception:
                    return default

        sub = 0
        tax = 0
        for it in items:
            unit = (_num(getattr(it, "unit_price", None)) or _num(getattr(it, "単価", None)))
            qty  = (_num(getattr(it, "qty", None))         or _num(getattr(it, "数量", None), 1))
            rate = (getattr(it, "tax_rate", None) if hasattr(it, "tax_rate") else getattr(it, "税率", None))
            rate = float(rate or 0)
            sub += unit * qty
            tax += int(unit * rate) * qty
        tot = int(sub + tax)

        h = s.get(TOrder, order_id)
        if h:
            for attr in ("subtotal", "小計"):
                if hasattr(TOrder, attr):
                    setattr(h, attr, int(sub))
            for attr in ("tax", "税額"):
                if hasattr(TOrder, attr):
                    setattr(h, attr, int(tax))
            for attr in ("total", "合計"):
                if hasattr(TOrder, attr):
                    setattr(h, attr, tot)
            s.flush()
            current_app.logger.info("[table_move][recalc] order=%s subtotal=%s tax=%s total=%s",
                                    order_id, sub, tax, tot)
        return {"小計": int(sub), "税額": int(tax), "合計": tot}

    # NOT NULL なら table_id を None にしない
    def _set_table_id_nullable_safe(model_obj, model_cls, table_id_or_none, fallback_id):
        if not hasattr(model_cls, "table_id"):
            return
        try:
            col = getattr(model_cls, "table_id").property.columns[0]
            is_nullable = getattr(col, "nullable", True)
        except Exception:
            is_nullable = True
        setattr(model_obj, "table_id", table_id_or_none if is_nullable else fallback_id)

    try:
        sid = current_store_id()
        if sid is None:
            return jsonify({"ok": False, "error": "store not selected"}), 400

        data = request.get_json(silent=True) or request.form
        from_table_id = int(data.get("from_table_id") or 0)
        to_table_id   = int(data.get("to_table_id") or 0)
        mode          = (data.get("mode") or "deny").lower().strip()  # "deny" | "merge" | "merge_new" | "swap"

        if not from_table_id or not to_table_id or from_table_id == to_table_id:
            return jsonify({"ok": False, "error": "invalid table ids"}), 400
        if mode not in ("deny", "merge", "merge_new", "swap"):
            return jsonify({"ok": False, "error": "invalid mode"}), 400

        src = s.get(TableSeat, from_table_id)
        dst = s.get(TableSeat, to_table_id)
        if not src or not dst:
            return jsonify({"ok": False, "error": "table not found"}), 404
        if getattr(src, "store_id", None) != sid or getattr(dst, "store_id", None) != sid:
            return jsonify({"ok": False, "error": "cross-store move not allowed"}), 403

        TOrder, TItem, TPay, TCD, TQR = _models()

        src_order = _get_active_order(TOrder, sid, from_table_id)
        if not src_order:
            return jsonify({"ok": False, "error": "no active order on source table"}), 404
        dst_order = _get_active_order(TOrder, sid, to_table_id)

        # deny: 先客がいれば拒否
        if mode == "deny" and dst_order:
            return jsonify({"ok": False, "error": "destination already has active order",
                            "dest_order_id": getattr(dst_order, "id", None)}), 409

        result = {
            "ok": True,
            "mode": mode,
            "from": from_table_id,
            "to": to_table_id,
            "src_order_id": getattr(src_order, "id", None),
            "dst_order_id": getattr(dst_order, "id", None),
        }

        # ===== merge：to 側に集約（既存の伝票を残したまま） =====
        if mode == "merge" and dst_order:
            src_oid = getattr(src_order, "id")
            dst_oid = getattr(dst_order, "id")

            # 明細/支払 to 側へ
            if TItem is not None:
                s.query(TItem).filter(getattr(TItem, "order_id") == src_oid)\
                    .update({getattr(TItem, "order_id"): dst_oid}, synchronize_session=False)
            if TPay is not None:
                s.query(TPay).filter(getattr(TPay, "order_id") == src_oid)\
                    .update({getattr(TPay, "order_id"): dst_oid}, synchronize_session=False)
            # お客様詳細：to 側へ付替 + 合算
            if TCD is not None:
                s.query(TCD).filter(getattr(TCD, "order_id") == src_oid)\
                    .update({getattr(TCD, "table_id"): to_table_id,
                             getattr(TCD, "order_id"): dst_oid}, synchronize_session=False)
                _reset_customer_detail_for_table(TCD, from_table_id)
                result["customer_detail"] = _coalesce_customer_detail(TCD, order_id=dst_oid, table_id=to_table_id)

            # src をクローズ（table_id None 禁止対策）
            if hasattr(TOrder, "status"):
                setattr(src_order, "status", "会計済(統合)")
            _set_table_id_nullable_safe(src_order, TOrder, None, from_table_id)

            # to 代表維持 + 合計再計算
            setattr(dst_order, "table_id", to_table_id)
            _recalc_order_totals_from_items(dst_oid, TOrder, TItem)

            _rebind_latest_qr(TQR, sid, from_table_id, to_table_id)
            try:
                if hasattr(src, "status"): src.status = "空席"
                if hasattr(dst, "status"): dst.status = "着席"
            except Exception:
                pass

            # 履歴記録
            history_id = _record_table_move_history(s, sid, from_table_id, to_table_id, mode, src_order, dst_order)
            result["history_id"] = history_id

            s.commit()
            mark_floor_changed()
            result["merged_to"] = dst_oid
            return jsonify(result)

        # ===== merge_new：新しい注文IDを発行して両方を統合 =====
        if mode == "merge_new" and dst_order:
            src_oid = getattr(src_order, "id")
            dst_oid = getattr(dst_order, "id")

            # 新規ヘッダを作成
            new_h = TOrder()
            if hasattr(TOrder, "store_id"):
                setattr(new_h, "store_id", sid)
            setattr(new_h, "table_id", to_table_id)
            if hasattr(TOrder, "status"):
                setattr(new_h, "status", getattr(dst_order, "status", None) or "新規")
            now = datetime.now(timezone.utc)
            for attr in ("opened_at", "created_at", "作成日時", "開始日時"):
                if hasattr(TOrder, attr):
                    setattr(new_h, attr, now)
            s.add(new_h)
            s.flush()
            new_oid = getattr(new_h, "id")

            # src/dst の明細・支払・お客様詳細を新IDへ付替 + 合算
            if TItem is not None:
                s.query(TItem).filter(getattr(TItem, "order_id").in_([src_oid, dst_oid]))\
                    .update({getattr(TItem, "order_id"): new_oid}, synchronize_session=False)
            if TPay is not None:
                s.query(TPay).filter(getattr(TPay, "order_id").in_([src_oid, dst_oid]))\
                    .update({getattr(TPay, "order_id"): new_oid}, synchronize_session=False)
            if TCD is not None:
                s.query(TCD).filter(getattr(TCD, "order_id").in_([src_oid, dst_oid]))\
                    .update({getattr(TCD, "order_id"): new_oid,
                             getattr(TCD, "table_id"): to_table_id}, synchronize_session=False)
                _reset_customer_detail_for_table(TCD, from_table_id)
                result["customer_detail"] = _coalesce_customer_detail(TCD, order_id=new_oid, table_id=to_table_id)

            # 旧2伝票を会計済(統合)へ
            if hasattr(TOrder, "status"):
                setattr(src_order, "status", "会計済(統合)")
                setattr(dst_order, "status", "会計済(統合)")
            _set_table_id_nullable_safe(src_order, TOrder, None, from_table_id)
            _set_table_id_nullable_safe(dst_order, TOrder, None, to_table_id)

            # 新規ヘッダに合計を反映
            setattr(new_h, "table_id", to_table_id)
            _recalc_order_totals_from_items(new_oid, TOrder, TItem)

            _rebind_latest_qr(TQR, sid, from_table_id, to_table_id)
            try:
                if hasattr(src, "status"): src.status = "空席"
                if hasattr(dst, "status"): dst.status = "着席"
            except Exception:
                pass

            # 履歴記録（merge_newの場合は新規注文IDも記録）
            history_id = _record_table_move_history(s, sid, from_table_id, to_table_id, mode, src_order, dst_order, new_order_id=new_oid)
            result["history_id"] = history_id

            s.commit()
            mark_floor_changed()
            result.update({"merged_new": True, "new_order_id": new_oid})
            return jsonify(result)

        # ===== swap：2つのアクティブ注文を入替 =====
        if mode == "swap" and dst_order:
            src_oid = getattr(src_order, "id")
            dst_oid = getattr(dst_order, "id")

            setattr(src_order, "table_id", to_table_id)
            setattr(dst_order, "table_id", from_table_id)

            if TCD is not None:
                s.query(TCD).filter(getattr(TCD, "order_id") == src_oid)\
                    .update({getattr(TCD, "table_id"): to_table_id}, synchronize_session=False)
                s.query(TCD).filter(getattr(TCD, "order_id") == dst_oid)\
                    .update({getattr(TCD, "table_id"): from_table_id}, synchronize_session=False)
                _reset_customer_detail_for_table(TCD, from_table_id)
                _reset_customer_detail_for_table(TCD, to_table_id)

            _rebind_latest_qr(TQR, sid, from_table_id, to_table_id)
            _rebind_latest_qr(TQR, sid, to_table_id, from_table_id)

            try:
                if hasattr(src, "status") and hasattr(dst, "status"):
                    src.status, dst.status = dst.status, src.status
            except Exception:
                pass

            # 履歴記録
            history_id = _record_table_move_history(s, sid, from_table_id, to_table_id, mode, src_order, dst_order)
            result["history_id"] = history_id

            s.commit()
            mark_floor_changed()
            result["swapped"] = True
            return jsonify(result)

        # ===== 通常 move =====
        setattr(src_order, "table_id", to_table_id)

        if TCD is not None:
            s.query(TCD).filter(getattr(TCD, "order_id") == getattr(src_order, "id"))\
                .update({getattr(TCD, "table_id"): to_table_id}, synchronize_session=False)
            _reset_customer_detail_for_table(TCD, from_table_id)

        _rebind_latest_qr(TQR, sid, from_table_id, to_table_id)

        try:
            if hasattr(src, "status"): src.status = "空席"
            if hasattr(dst, "status"): dst.status = "着席"
        except Exception:
            pass

        # 履歴記録
        history_id = _record_table_move_history(s, sid, from_table_id, to_table_id, mode, src_order, dst_order)
        result["history_id"] = history_id

        s.commit()
        mark_floor_changed()
        result["moved_to"] = to_table_id
        return jsonify(result)

    except Exception as e:
        s.rollback()
        current_app.logger.exception("admin_table_move failed")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        s.close()



# ========================================
# 3. テーブル移動履歴ページのルート
# ========================================

@app.route("/admin/table_move_history")
@require_store_admin
def admin_table_move_history():
    """
    テーブル移動履歴を表示するページ
    """
    from datetime import datetime, timezone, timedelta
    from sqlalchemy import or_, func
    
    sid = current_store_id()
    if sid is None:
        return redirect(url_for("admin_login"))
    
    # パラメータ
    from_date_str = request.args.get("from_date")
    to_date_str = request.args.get("to_date")
    table_id = request.args.get("table_id", type=int)
    
    def _parse_date(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except Exception:
            return None
    
    from_date = _parse_date(from_date_str) if from_date_str else None
    to_date = _parse_date(to_date_str) + timedelta(days=1) if to_date_str else None
    
    # モデル取得
    THistory = globals().get("T_テーブル移動履歴")
    TableSeat = globals().get("TableSeat") or globals().get("T_テーブル席")
    
    if THistory is None:
        return render_template(
            "admin_table_move_history.html",
            title="テーブル移動履歴",
            error="T_テーブル移動履歴 テーブルが存在しません。",
            histories=[],
            tables=[],
            store_name=session.get("store_name"),
            sid=sid,
        )
    
    s = SessionLocal()
    try:
        # テーブル一覧を取得
        tables = []
        if TableSeat:
            qt = s.query(TableSeat)
            if hasattr(TableSeat, "store_id"):
                qt = qt.filter(getattr(TableSeat, "store_id") == sid)
            tables = qt.order_by(getattr(TableSeat, "table_no", TableSeat.id).asc()).all()
        
        # 履歴を取得
        q = s.query(THistory)
        if hasattr(THistory, "store_id"):
            q = q.filter(getattr(THistory, "store_id") == sid)
        
        # 期間フィルター
        if from_date and hasattr(THistory, "moved_at"):
            q = q.filter(getattr(THistory, "moved_at") >= from_date)
        if to_date and hasattr(THistory, "moved_at"):
            q = q.filter(getattr(THistory, "moved_at") < to_date)
        
        # テーブルフィルター
        if table_id:
            q = q.filter(
                or_(
                    getattr(THistory, "from_table_id") == table_id,
                    getattr(THistory, "to_table_id") == table_id
                )
            )
        
        # 並び順（新しい順）
        histories = q.order_by(getattr(THistory, "moved_at").desc()).limit(500).all()
        
        # テーブル番号マップを作成
        table_no_map = {}
        if TableSeat:
            for t in tables:
                tid = getattr(t, "id", None)
                tno = getattr(t, "table_no", None) or getattr(t, "テーブル番号", None) or str(tid)
                table_no_map[tid] = tno
        
        # 履歴に テーブル番号を追加
        for h in histories:
            from_tid = getattr(h, "from_table_id", None)
            to_tid = getattr(h, "to_table_id", None)
            
            setattr(h, "from_table_no", table_no_map.get(from_tid, str(from_tid)))
            setattr(h, "to_table_no", table_no_map.get(to_tid, str(to_tid)) if to_tid else "-")
            
            # 合計人数を計算
            total_people = 0
            for attr in ("adult_male", "adult_female", "child_male", "child_female"):
                val = getattr(h, attr, None)
                if val:
                    total_people += int(val)
            setattr(h, "total_people", total_people)
        
        # 統計情報
        total_moves = len(histories)
        
        return render_template(
            "admin_table_move_history.html",
            title="テーブル移動履歴",
            histories=histories,
            tables=tables,
            table_no_map=table_no_map,
            current_table_id=table_id,
            from_date=from_date_str or "",
            to_date=to_date_str or "",
            total_moves=total_moves,
            store_name=session.get("store_name"),
            sid=sid,
            csrf_token=session.get("_csrf_token"),
        )
    
    finally:
        s.close()



# ---------------------------------------------------------------------
# スタッフ用 UI / ルーティング
# ---------------------------------------------------------------------
# --- [スタッフ] ルート：/staff → /staff/floor に転送 -----------------------------------------
@app.route("/staff")
@require_staff
def staff_root():
    return redirect(url_for("staff_floor"))


# --- [スタッフ] 注文ページ：時価商品の価格入力対応 -----------------------------------
@app.route("/staff/order/<int:table_id>")
@require_staff
def staff_order(table_id):
    """
    スタッフ用注文ページ：時価商品の価格入力機能付き
    """
    sid = current_store_id()
    if sid is None:
        return redirect(url_for("staff_login"))
    
    s = SessionLocal()
    try:
        # テーブル存在確認
        t = s.get(TableSeat, table_id)
        if not t or (hasattr(t, "store_id") and t.store_id != sid):
            return "Table not found", 404
        
        # 進行中の注文を取得
        order_id = None
        q = s.query(OrderHeader).filter(OrderHeader.table_id == table_id)
        if hasattr(OrderHeader, "store_id"):
            q = q.filter(OrderHeader.store_id == sid)
        orders = q.order_by(OrderHeader.id.desc()).all()
        for o in orders:
            status = getattr(o, "status", "")
            if status not in ["会計済", "closed", "paid"]:
                order_id = o.id
                break
        
        # スタッフ名を取得
        staff_name = session.get("username", "Unknown")
        
        return render_template(
            "staff_order.html",
            table={"id": table_id, "no": getattr(t, "table_no", table_id)},
            table_id=table_id,
            table_no=getattr(t, "table_no", table_id),
            order_id=order_id,
            staff_name=staff_name,
            title=f"テーブル {getattr(t, 'table_no', table_id)} - 注文"
        )
    finally:
        s.close()


# ---------------------------------------------------------------------
# スタッフ用フロア画面（店舗縛り＋詳細デバッグ付き）
# ---------------------------------------------------------------------
# --- [スタッフ] フロア画面：T_注文/T_注文明細 ベースで現在合計を表示 ------------------------------
@app.route("/staff/floor")
@require_staff
def staff_floor():
    from sqlalchemy import func
    sid = current_store_id()
    if sid is None:
        return redirect(url_for("staff_login"))

    # 未会計として扱うステータス（order["状態"] の表示/ボタン判定に使用）
    ACTIVE_ORDER_STATUSES = {
        "open", "pending", "in_progress", "serving", "unpaid",
        "新規", "調理中", "提供済", "会計中"
    }

    # --- ローカルフォールバック: 取消（負数量）を含めたサマリをDBから算出 ---
    def __financials_including_negatives(s, order_id: int):
        """
        合計=明細のネット（負数量込み、内税）
        ただし『正数量の取消ラベル行』は除外する
        既払=支払記録合計（返金はマイナス）
        残額=合計-既払
        """
        import math
        Item   = globals().get("OrderItem")
        Pay    = globals().get("PaymentRecord") or globals().get("T_支払")
        Header = globals().get("OrderHeader")

        total_incl = 0
        if Item is not None:
            items = (
                s.query(Item)
                 .filter(getattr(Item, "order_id") == order_id)
                 .order_by(getattr(Item, "id").asc())
                 .all()
            )
            for d in items or []:
                qty = int(getattr(d, "qty", None) or getattr(d, "数量", None) or 0)
                if qty == 0:
                    continue

                # 取消ラベル判定（日本語/英語いずれも）
                st_raw = (getattr(d, "status", None) or getattr(d, "状態", None) or "")
                st_low = str(st_raw).lower()
                is_cancel_label = (
                    ("取消" in st_low) or ("ｷｬﾝｾﾙ" in st_low) or ("キャンセル" in st_low)
                    or ("cancel" in st_low) or ("void" in st_low)
                )

                # 正数量かつ取消ラベルは除外（会計前の“状態=取消”対応）
                # 監査用の負数量明細（qty<0）はネット計算に含める
                if qty > 0 and is_cancel_label:
                    continue

                unit_excl = int(getattr(d, "unit_price", None) or getattr(d, "税抜単価", None) or 0)
                rate = float(getattr(d, "tax_rate", None) or 0.10)
                unit_tax  = math.floor(unit_excl * rate)
                unit_incl = unit_excl + unit_tax
                total_incl += unit_incl * qty

        # 既払（返金はマイナス）
        paid = 0
        if Pay is not None:
            col_amount = getattr(Pay, "amount", None) or getattr(Pay, "金額", None)
            if col_amount is not None:
                agg = (
                    s.query(func.coalesce(func.sum(col_amount), 0))
                     .filter(getattr(Pay, "order_id") == order_id)
                )
                if hasattr(Pay, "store_id") and sid is not None:
                    agg = agg.filter(getattr(Pay, "store_id") == sid)
                paid = int(agg.scalar() or 0)

        remaining = int(total_incl) - int(paid)

        # （任意）ヘッダへも反映して整合を保つ（副作用を避けたい場合はコメントアウト可）
        if Header is not None:
            h = s.get(Header, order_id)
            if h:
                if hasattr(h, "total"): h.total = int(total_incl)
                if hasattr(h, "合計"):  setattr(h, "合計", int(total_incl))

        if bool(current_app.config.get("DEBUG_TOTALS", False)):
            current_app.logger.debug(
                "[staff.fin] order_id=%s total=%s paid=%s remaining=%s",
                order_id, int(total_incl), int(paid), int(remaining)
            )

        return {"total": int(total_incl), "paid": int(paid), "remaining": int(remaining)}

    s = SessionLocal()
    try:
        # テーブル一覧（店舗スコープ）
        qt = s.query(TableSeat)
        if hasattr(TableSeat, "store_id"):
            qt = qt.filter(TableSeat.store_id == sid)
        tables = qt.order_by(getattr(TableSeat, "table_no", TableSeat.id).asc()).all()

        out = []
        for seat in tables:
            tdict = {
                "id": getattr(seat, "id"),
                "テーブル番号": getattr(seat, "table_no", None) or getattr(seat, "テーブル番号", None) or getattr(seat, "id"),
                "状態": getattr(seat, "status", "") or getattr(seat, "状態", "") or "空席",
                "order": None,
            }

            # 既存ヘルパから“伝票の存在”だけ把握
            summary = _calc_order_summary_from_T(s, store_id=sid, table_id=getattr(seat, "id"))

            if summary:
                st_label = summary.get("状態")
                is_active = (st_label in ACTIVE_ORDER_STATUSES) or (st_label is None)

                if is_active:
                    # 取消（正数量の取消ラベルは除外、負数量は反映）で再集計
                    fin = None
                    try:
                        inc = globals().get("_order_financials_including_negatives")
                        if callable(inc):
                            fin = inc(s, int(summary["id"]))
                    except Exception:
                        fin = None
                    if not fin:
                        fin = __financials_including_negatives(s, int(summary["id"]))

                    tdict["order"] = {
                        "id": int(summary["id"]),
                        "状態": st_label or "",
                        "合計": int(fin.get("total", 0)),
                        "既払": int(fin.get("paid", 0)),
                        "残額": int(fin.get("remaining", 0)),
                    }
                else:
                    tdict["order"] = None
                    if not tdict["状態"]:
                        tdict["状態"] = "空席"

            out.append(tdict)

        return render_template(
            "staff_floor.html",
            tables=out,
            staff_name=session.get("staff_name") or session.get("user_name") or "スタッフ",
            debug_info="",
            title="スタッフ：テーブル一覧",
        )
    finally:
        s.close()




# ---------------------------------------------------------------------
# スタッフ用：旧 URL 互換 → 新ルートへ転送
# ---------------------------------------------------------------------
# --- [スタッフ] 旧URL互換：テーブル画面→新メニュールートへ転送 -------------------------------
@app.route("/staff/table/<int:table_id>")
@require_staff
def staff_table(table_id: int):
    # 互換用：旧エンドポイントに来たら、menu_page へ橋渡しする新ルートへ転送
    return redirect(url_for('staff_open_menu', table_id=table_id))



# ---------------------------------------------------------------------
# スタッフ用：指定テーブルでメニューを開く（menu_page へリダイレクト）
# ---------------------------------------------------------------------
# --- [スタッフ] 指定テーブルのメニューを開く（menu_page へリダイレクト） -----------------------
@app.route("/staff/open/<int:table_id>/menu", endpoint="staff_open_menu")
@require_staff
def staff_open_menu(table_id: int):
    s = SessionLocal()
    try:
        # --- 1) テーブル＆店舗チェック --------------------------------------------------------
        t = s.get(TableSeat, table_id)
        if not t:
            abort(404, "テーブルが見つかりません。")
        store_id = getattr(t, "store_id", None) or session.get("store_id")
        if not store_id:
            abort(400, "store_id が決定できません。（テーブル or セッション）")

        # --- 2) tenant_slug を決定（セッション→Storeリレーションの順） -------------------------
        tenant_slug = session.get("tenant_slug")
        if not tenant_slug:
            st = s.get(Store, store_id)
            tenant_slug = (getattr(st, "tenant_slug", None)
                           or getattr(getattr(st, "tenant", None), "slug", None))
            if not tenant_slug:
                abort(400, "tenant_slug が取得できません。ログイン/店舗設定をご確認ください。")

        # --- 3) このテーブル用の QR トークンを取得 or 作成 -------------------------------------
        qt = (s.query(QrToken)
                .filter(QrToken.store_id == store_id,
                        QrToken.table_id == table_id,
                        getattr(QrToken, "disabled", 0) == 0)
                .order_by(QrToken.id.desc())
                .first())
        if not qt:
            import secrets
            token = secrets.token_urlsafe(24).replace("_", "-")
            qt = QrToken(store_id=store_id,
                         table_id=table_id,
                         token=token,
                         **({"disabled": 0} if "disabled" in QrToken.__table__.columns else {}))
            s.add(qt)
            s.flush()

        # --- 4) /t/<tenant_slug>/m/<token> へリダイレクト（= menu_page） -----------------------
        return redirect(url_for("menu_page", tenant_slug=tenant_slug, token=qt.token))

    finally:
        s.close()



# ---------------------------------------------------------------------
# スタッフ注文API（table_id 指定版）
# ---------------------------------------------------------------------
# --- [スタッフAPI] 注文登録（table_id 指定版 / 同期印刷対応） -----------------------------------
@app.route("/staff/api/order", methods=["POST"])
@require_staff
def staff_api_order():
    """
    スタッフ用の注文 API（/api/order の table_id 版）
    POST JSON:
      { "table_id": 1, "items": [{"menu_id": 3, "qty": 2, "memo": "辛め"}] }
    """
    import math  # floor を使うため（ローカルインポート）

    data = request.get_json(force=True) or {}
    table_id_raw = data.get("table_id")
    items = data.get("items") or []
    if table_id_raw is None or not isinstance(items, list) or not items:
        return jsonify({"ok": False, "error": "table_id/items required"}), 400

    # table_id を int に正規化
    try:
        table_id = int(table_id_raw)
    except Exception:
        return jsonify({"ok": False, "error": "invalid table_id"}), 400

    s = SessionLocal()
    try:
        # テーブル存在確認
        t = s.get(TableSeat, table_id)
        if not t:
            return jsonify({"ok": False, "error": "table not found"}), 404

        # ★ デバッグ：テーブル情報
        try:
            app.logger.debug("[staff_api_order] table_id=%s store_id=%s status=%s",
                             table_id, getattr(t, "store_id", None), getattr(t, "status", None))
        except Exception:
            pass

        # ★ お客様詳細モデルを柔軟に解決（T_お客様詳細 / M_顧客詳細 / CustomerDetail）
        TCustomerDetail = (
            globals().get("T_お客様詳細")
            or globals().get("M_顧客詳細")
            or globals().get("CustomerDetail")
        )
        if TCustomerDetail is None:
            app.logger.warning("[staff_api_order] TCustomerDetail model not found (T_お客様詳細 / M_顧客詳細 / CustomerDetail)")
        else:
            app.logger.debug(
                "[staff_api_order] TCustomerDetail resolved: %s (has order_id? %s, has table_id? %s, has store_id? %s)",
                getattr(TCustomerDetail, "__tablename__", str(TCustomerDetail)),
                hasattr(TCustomerDetail, "order_id"),
                hasattr(TCustomerDetail, "table_id"),
                hasattr(TCustomerDetail, "store_id"),
            )

        # ★ 店舗IDの取得（なければ None）
        try:
            store_id = getattr(t, "store_id", None) or current_store_id()
        except Exception:
            store_id = None
        app.logger.debug("[staff_api_order] resolved store_id=%s", store_id)

        # 既存オーダー検索（日本語ステータス）
        order = (
            s.query(OrderHeader)
             .filter(
                 OrderHeader.table_id == table_id,
                 OrderHeader.status.in_(["新規", "調理中", "提供済", "会計中"])
             )
             .order_by(OrderHeader.id.desc())
             .first()
        )
        app.logger.debug("[staff_api_order] active order found? %s", bool(order))

        new_order_created = False
        if not order:
            order = OrderHeader(
                table_id=table_id,
                status="新規",
                subtotal=0,
                tax=0,
                total=0,
                opened_at=now_str(),
            )
            s.add(order)
            try:
                t.status = "着席"
            except Exception:
                app.logger.debug("[staff_api_order] table status set failed (non-fatal)")
            s.flush()  # order.id を確定
            new_order_created = True
            app.logger.info("[staff_api_order] created new order id=%s for table_id=%s", order.id, table_id)
        else:
            app.logger.info("[staff_api_order] reuse active order id=%s for table_id=%s", order.id, table_id)

        # ★ ここで必ず T_お客様詳細 に order_id を紐付ける
        #    - 既に order_id 行があればそれを使用
        #    - 無ければ table_id の孤児行を拾って order_id を埋める
        #    - それも無ければ新規作成（人数系は 0 初期化）
        bound_cd_id = None
        if TCustomerDetail is not None:
            try:
                # 1) order_id で既存を探す
                cd = None
                if hasattr(TCustomerDetail, "order_id"):
                    cd = (
                        s.query(TCustomerDetail)
                         .filter(getattr(TCustomerDetail, "order_id") == order.id)
                         .order_by(getattr(TCustomerDetail, "id").desc())
                         .first()
                    )
                    app.logger.debug("[staff_api_order] search by order_id=%s -> %s", order.id, "hit" if cd else "none")
                else:
                    app.logger.warning("[staff_api_order] TCustomerDetail has no order_id column!")

                # 2) なければ table_id 孤児を拾う
                if cd is None:
                    q = s.query(TCustomerDetail).filter(
                        getattr(TCustomerDetail, "table_id") == table_id
                    )
                    if hasattr(TCustomerDetail, "order_id"):
                        q = q.filter(getattr(TCustomerDetail, "order_id") == None)  # noqa: E711
                        app.logger.debug("[staff_api_order] searching orphan rows for table_id=%s", table_id)
                    cd = q.order_by(getattr(TCustomerDetail, "id").asc()).first()
                    app.logger.debug("[staff_api_order] search orphan by table_id=%s -> %s", table_id, "hit" if cd else "none")

                # 3) まだ無ければ新規作成
                created_new_cd = False
                if cd is None:
                    cd = TCustomerDetail()
                    created_new_cd = True
                    if hasattr(TCustomerDetail, "store_id") and store_id is not None:
                        setattr(cd, "store_id", store_id)
                    if hasattr(TCustomerDetail, "table_id"):
                        setattr(cd, "table_id", table_id)
                    # 人数列の初期化（存在する列だけ）
                    for col in ("大人男性", "大人女性", "子ども男", "子ども女", "合計人数"):
                        if hasattr(TCustomerDetail, col):
                            setattr(cd, col, 0)
                    s.add(cd)
                    app.logger.info("[staff_api_order] created new TCustomerDetail (table_id=%s)", table_id)

                # 4) 最終セット
                if hasattr(TCustomerDetail, "order_id"):
                    before = getattr(cd, "order_id", None)
                    setattr(cd, "order_id", order.id)
                    after = getattr(cd, "order_id", None)
                    app.logger.debug("[staff_api_order] set cd.order_id: %s -> %s", before, after)
                else:
                    app.logger.warning("[staff_api_order] cannot set order_id because column not present")

                if hasattr(TCustomerDetail, "table_id"):
                    setattr(cd, "table_id", table_id)
                if hasattr(TCustomerDetail, "store_id") and store_id is not None:
                    setattr(cd, "store_id", store_id)

                s.flush()
                bound_cd_id = getattr(cd, "id", None)
                app.logger.info("[staff_api_order] bound customer_detail id=%s to order=%s (created_new=%s)",
                                bound_cd_id, order.id, created_new_cd)
            except Exception as e:
                app.logger.exception("[staff_api_order] bind TCustomerDetail failed: %s", e)

        subtotal = int(order.subtotal or 0)
        taxsum   = int(order.tax or 0)
        added    = 0

        # ★ 新しく追加される明細を保持するリスト
        new_items_for_print = []

        for it in items:
            # アイテムバリデーション
            try:
                mid = int(it.get("menu_id"))
                qty = int(it.get("qty", 1))
            except Exception:
                app.logger.debug("[staff_api_order] skip item (invalid menu_id/qty): %s", it)
                continue
            if qty <= 0:
                app.logger.debug("[staff_api_order] skip item (qty<=0): %s", it)
                continue

            memo = (it.get("memo") or "").strip()
            actual_price = it.get("actual_price")  # 時価商品の実際価格
            m = s.get(Menu, mid)
            if not (m and m.available == 1):
                app.logger.debug("[staff_api_order] skip item (menu not available): id=%s", mid)
                continue

            rate = resolve_effective_tax_rate_for_menu(s, mid, m.tax_rate)  # 例: 0.10
            unit = int(m.price)  # 税抜保存単価
            
            # 時価商品の場合、actual_priceを使用
            if actual_price is not None:
                unit = int(actual_price)
                app.logger.info("[staff_api_order] market price item: menu_id=%s actual_price=%s", mid, unit)

            new_item = OrderItem(
                order_id=order.id,
                menu_id=mid,
                qty=qty,
                unit_price=unit,   # 税抜単価
                tax_rate=rate,
                memo=memo,
                status="新規",
                added_at=now_str(),
            )
            
            # actual_priceをOrderItemに保存
            if actual_price is not None and hasattr(new_item, 'actual_price'):
                new_item.actual_price = int(actual_price)
            s.add(new_item)
            new_items_for_print.append(new_item)  # ★ 新しい明細をリストに追加

            # 金額集計（1個ごと端数処理）
            subtotal += unit * qty
            per_unit_tax = int(math.floor(unit * rate))
            taxsum += per_unit_tax * qty
            added  += 1

        # 1件も有効アイテムが無ければ 400（新規作成していたらロールバックで空注文を消す）
        if added == 0:
            app.logger.warning("[staff_api_order] no valid items (rollback). new_order_created=%s", new_order_created)
            s.rollback()
            return jsonify({"ok": False, "error": "no valid items"}), 400

        order.subtotal = subtotal
        order.tax      = taxsum
        order.total    = subtotal + taxsum

        # ★ コミット前に最終ログ
        app.logger.debug("[staff_api_order] commit: order_id=%s subtotal=%s tax=%s total=%s cd_id=%s",
                         order.id, order.subtotal, order.tax, order.total, bound_cd_id)

        s.commit()  # ← コミット後に印刷トリガ
        mark_floor_changed()

        # ★ 非同期印刷を削除し、同期的に印刷ジョブを呼び出す
        try:
            trigger_print_job(order.id, items_to_print=new_items_for_print)
        except Exception:
            # 印刷起動の失敗はレスポンスに影響させない
            app.logger.exception("[staff_api_order] failed to print")

        return jsonify({
            "ok": True,
            "order_id": order.id,
            "subtotal": order.subtotal,
            "tax": order.tax,
            "total": order.total
        })
    except Exception as e:
        s.rollback()
        app.logger.error("[staff_api_order] error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": "internal error"}), 500
    finally:
        s.close()



# --- 明細( OrderItem )の状態を更新するAPI -----------------------------------
@app.post("/staff/api/order_item/<int:item_id>/status")
@require_staff
def staff_api_progress_update(item_id: int):
    return _progress_update_core(item_id)

@app.route("/api/order_item/<int:item_id>/status", methods=["POST"])
@require_any
def api_progress_update(item_id: int):
    return _progress_update_core(item_id)


def _norm_status(st: str) -> str:
    """UIから来る多様な表記を cooking / served / cancel / new の4種に正規化"""
    s = (st or "").strip().lower()
    # 日本語・表記ゆれを吸収
    if s in ("調理中", "cooking"):
        return "cooking"
    if s in ("提供済", "served"):
        return "served"
    if s in ("取消", "ｷｬﾝｾﾙ", "キャンセル", "cancel", "void"):
        return "cancel"
    if s in ("新規", "new"):
        return "new"
    return s  # その他（不明）はそのまま返す


def _progress_update_core(item_id: int):
    from datetime import datetime
    s = SessionLocal()
    try:
        j = request.get_json(force=True) or {}
        raw_status = (j.get("status") or "").strip()
        count  = int(j.get("count") or 1)
        if count <= 0:
            return jsonify(ok=False, error="count must be >= 1"), 400

        action = _norm_status(raw_status)
        if action not in {"cooking","served","cancel","new"}:
            return jsonify(ok=False, error="invalid status"), 400

        OrderItem, Menu = _models()
        it = s.get(OrderItem, item_id)
        if not it:
            return jsonify(ok=False, error="item not found"), 404

        # 進捗行シード（未作成なら qty_new=元数量）
        progress_seed_if_needed(s, it)

        # 取消時は負行作成に必要な税率を先に確保
        tax_rate = None
        if action == "cancel":
            menu_id = _get_any(it, "menu_id", "メニューid", "商品id")
            menu = s.get(Menu, menu_id) if menu_id is not None else None
            tax_rate = _guess_tax_rate(src_item=it, menu=menu)

        # 実移動
        try:
            p_after, moved = progress_move(s, it, action, count)
        except ValueError as e:
            s.rollback()
            return jsonify(ok=False, error=str(e)), 400

        # ★★★ 追加：明細の status を進捗カウンタに同期 ★★★
        try:
            n = p_after.get("qty_new", 0)
            c = p_after.get("qty_cooking", 0)
            sv = p_after.get("qty_served", 0)
            cx = p_after.get("qty_canceled", 0)
            
            # 優先順位：取消 > 提供済 > 調理中 > 新規
            new_status = None
            if cx > 0:
                new_status = "取消"
            elif sv > 0:
                new_status = "提供済"
            elif c > 0:
                new_status = "調理中"
            elif n > 0:
                new_status = "新規"
            
            if new_status:
                old_status = _get_any(it, "status", "状態", default=None)
                _set_first(it, ["status", "状態"], new_status)
                if hasattr(it, "updated_at"):
                    it.updated_at = datetime.utcnow()
                current_app.logger.debug("[PROGRESS-API][SYNC] item_id=%s status: %s -> %s", 
                                       item_id, old_status, new_status)
        except Exception as e:
            current_app.logger.warning("[PROGRESS-API][SYNC] failed to sync status: %s", e)
            # status 同期失敗は致命的ではないので続行

        neg_id = None
        if action == "cancel" and moved > 0:
            # 実際に動いた数だけマイナス行を作る
            OrderItemModel, _ = _models()
            neg = OrderItemModel()
            _copy_if_exists(neg, it, [
                (["order_id","注文id","注文ID"], ["order_id","注文id","注文ID"]),
                (["menu_id","メニューid","商品id"], ["menu_id","メニューid","商品id"]),
                (["store_id","店舗ID"], ["store_id","店舗ID"]),
                (["tenant_id"], ["tenant_id"]),
                (["name","名称"], ["name","名称"]),
                (["unit_price","単価","税抜単価"], ["unit_price","単価","税抜単価"]),
                (["税込単価"], ["税込単価","price_incl"]),
            ])
            _set_first(neg, ["qty","数量"], -int(moved))
            _set_first(neg, ["税率","tax_rate"], float(tax_rate if tax_rate is not None else 0.10))
            _set_first(neg, ["status","状態"], "取消")

            # 親リンク or メモ
            parent_set = False
            for name in ["parent_item_id","親明細ID","元明細ID"]:
                if hasattr(neg, name):
                    setattr(neg, name, item_id)
                    parent_set = True
                    break
            if not parent_set:
                memo_old = _get_any(neg, "memo","メモ","備考","備考欄", default="") or ""
                _set_first(neg, ["memo","メモ","備考","備考欄"], (memo_old + " ").strip() + f"cancel_of:{item_id}")

            now = datetime.utcnow()
            if hasattr(neg, "created_at"): neg.created_at = now
            if hasattr(neg, "updated_at"): neg.updated_at = now
            if hasattr(neg, "追加日時"):   setattr(neg, "追加日時", now)
            s.add(neg); s.flush()
            neg_id = getattr(neg, "id", None)

        # 自動確定（提供済+取消 == 元数量 → 明細.status=提供済）
        finalized = progress_finalize_if_done(s, it)

        s.commit()
        mark_floor_changed()
        return jsonify(ok=True, progress=p_after, moved=int(moved), finalized=bool(finalized), negative_item_id=neg_id)

    except Exception as e:
        s.rollback()
        current_app.logger.exception("progress_update error: %s", e)
        return jsonify(ok=False, error="internal error"), 500
    finally:
        s.close()



# --- 伝票( OrderHeader )の状態を更新するAPI：管理者のみ許可 -----------------------------------
@app.route("/staff/api/order/<int:order_id>/status", methods=["POST"])
@require_store_admin   # ★ 管理者（店舗管理者）以上のみ
def staff_update_order_status(order_id):
    """
    JSON: { "status": "新規|調理中|提供済|会計中|会計済" ... }
    ※ 管理者以上のみ利用可能
    """
    data = request.get_json(force=True) or {}
    new_label = (data.get("status") or "").strip()
    ALLOWED = {
        "新規", "調理中", "提供済", "会計中", "会計済",
        "open", "in_progress", "serving", "unpaid", "closed", "paid"
    }
    if new_label not in ALLOWED:
        return jsonify({"ok": False, "error": "invalid status"}), 400

    s = SessionLocal()
    try:
        h = s.get(OrderHeader, order_id)
        if not h:
            return jsonify({"ok": False, "error": "order not found"}), 404

        sid = current_store_id()
        if hasattr(OrderHeader, "store_id") and sid is not None:
            if getattr(h, "store_id", None) != sid:
                return jsonify({"ok": False, "error": "forbidden"}), 403

        # ステータス更新
        if hasattr(h, "status"):
            h.status = new_label

        # 会計済にしたら閉鎖日時を記録（該当カラムがあれば）
        from datetime import datetime, timezone
        if new_label in {"会計済", "closed", "paid"}:
            for f in ("closed_at", "精算日時"):
                if hasattr(h, f):
                    setattr(h, f, datetime.now(timezone.utc))

        s.commit()
        mark_floor_changed()
        return jsonify({"ok": True})
    except Exception as e:
        s.rollback()
        app.logger.exception("[staff_update_order_status] %s", e)
        return jsonify({"ok": False, "error": "internal error"}), 500
    finally:
        s.close()




# ---------------------------------------------------------------------
# レガシーURLアダプタ（/menu, /menu/<token> → 正規ルートへ転送）
# ---------------------------------------------------------------------
# --- レガシーURLアダプタ（menu_page_legacy） ------------------------------------
@app.route("/menu")
@app.route("/menu/<token>")
def menu_page_legacy(token=None):
    # セッションや g から tenant_slug を取得
    slug = session.get("tenant_slug")
    if not slug and getattr(g, "tenant", None):
        slug = g.tenant.get("slug")

    if not slug:
        app.logger.warning("[menu_page_legacy] tenant_slug missing; session=%r g.tenant=%r",
                           dict(session), getattr(g, "tenant", None))
        return "tenant_slug が不明です。ログインし直してください。", 400

    # エンドポイント 'menu_page' に合わせて転送
    try:
        return redirect(url_for("menu_page", tenant_slug=slug, token=token))
    except Exception as e:
        app.logger.error("[menu_page_legacy] url_for(menu_page, tenant_slug=%s, token=%s) failed: %s",
                         slug, token, e)
        return redirect(f"/{slug}/menu" + (f"?token={token}" if token else ""))



# ---------------------------------------------------------------------
# 初期化画面（無効化告知）
# ---------------------------------------------------------------------
# --- 初期化画面（admin_initdb） --------------------------------------------------
@app.route("/admin/initdb")
def admin_initdb():
    return render_template_string("""
<!doctype html><html lang="ja"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>初期化(無効)</title>
<style>body{font-family:sans-serif;margin:20px}</style>
</head><body>
  <h2>初期化機能は無効化されています</h2>
  <p>このアプリは「既存の DB を参照するモード」で動作します。テーブル作成やデモ投入は行いません。</p>
  <p>DATABASE_URL: <code>***hidden***</code></p>
  <p><a href="{{ url_for('floor') }}">フロアへ戻る</a></p>
</body></html>
    """, db_url=DATABASE_URL), 403



# =============================================================================
# 売上集計機能：日付ヘルパ
# =============================================================================
# --- 日付ヘルパ：今日の文字列（_today_str） --------------------------------------
def _today_str():
    return datetime.now().strftime("%Y-%m-%d")

# --- 日付ヘルパ：期間取得（_range_from_params） -----------------------------------
def _range_from_params(start_key="start", end_key="end", default_days=30):
    """クエリ文字列から [start, end] を取り、未指定なら過去 default_days を返す（YYYY-MM-DD）"""
    end = (request.args.get(end_key) or _today_str()).strip()
    start = (request.args.get(start_key) or (_today_str())).strip()
    if not request.args.get(start_key) or not request.args.get(end_key):
        # どちらか欠けたら過去 default_days に丸める
        end = _today_str()
        start = (datetime.now() - timedelta(days=default_days)).strftime("%Y-%m-%d")
    return start, end

# --- 日付ヘルパ：終端時刻付与（_end_of_day） --------------------------------------
def _end_of_day(end_date_str: str) -> str:
    """期間境界（文字列比較用に終端 23:59:59 を付ける）"""
    return end_date_str + " 23:59:59"



# ---------------------------------------------------------------------
# 売上集計（HTMLダッシュボード）※ endpoint 名は "sales_report"
# ---------------------------------------------------------------------
# --- 売上集計（HTMLダッシュボード：sales_report） -------------------------------
@app.route("/admin/sales")
@app.route("/admin/sales-report")
@app.route("/reports/sales", endpoint="sales_report")
@require_store_admin
def sales_report():
    s = SessionLocal()
    try:
        import math, json
        from collections import defaultdict

        # 期間（未指定なら当月1日〜本日）
        today = _today_str()
        start = (request.args.get("start") or today[:8] + "01").strip()
        end   = (request.args.get("end")   or today).strip()
        end_dt = _end_of_day(end)

        EXCLUDED = {"統合済", "integrated", "merged"}
        sid = current_store_id()

        # ===== 期間内の伝票（統合済は除外、会計済みのみ） =====
        q = s.query(OrderHeader).filter(
            OrderHeader.closed_at.isnot(None),
            OrderHeader.closed_at >= start,
            OrderHeader.closed_at <= end_dt
        )
        if hasattr(OrderHeader, "status"):
            q = q.filter(OrderHeader.status == "会計済")
            q = q.filter(~OrderHeader.status.in_(EXCLUDED))
        if hasattr(OrderHeader, "store_id") and sid is not None:
            q = q.filter(OrderHeader.store_id == sid)
        orders = q.all()
        order_ids = [getattr(o, "id", None) for o in orders if getattr(o, "id", None) is not None]

        # ===== デバッグ用入れ物 =====
        dbg = {
            "period": {"start": start, "end": end_dt},
            "orders_count": len(orders),
            "first_order_ids": order_ids[:10],
            "hist_model": None,
            "hist_cols_found": [],
            "hist_rows_scanned": 0,
            "guests_per_order_preview": {},  # 先頭10件だけ
            "guests_total": None,
            "note": []
        }

        # ===== 人数：T_お客様詳細履歴 から取得（合計人数優先 / 最新レコード） =====
        def _to_int(x, default=0):
            try:
                return int(x)
            except Exception:
                try:
                    return int(float(x))
                except Exception:
                    return default

        # 履歴モデルを推測
        Hist = (
            globals().get("CustomerDetailHistory")
            or globals().get("TCustomerDetailHistory")
            or globals().get("CustomerInfoHistory")
            or globals().get("VisitHistory")
            or globals().get("T_お客様詳細履歴")
        )
        dbg["hist_model"] = getattr(Hist, "__name__", str(Hist))

        guests_map = {oid: None for oid in order_ids}

        if Hist and order_ids:
            qh = s.query(Hist)

            # 使えるカラム確認
            cols = [
                c for c in ("order_id","table_id","store_id","created_at","updated_at",
                            "合計人数","大人男性","大人女性","子ども男","子ども女")
                if hasattr(Hist, c)
            ]
            dbg["hist_cols_found"] = cols

            if hasattr(Hist, "store_id") and sid is not None:
                qh = qh.filter(Hist.store_id == sid)

            # 期間で緩く絞る
            if hasattr(Hist, "created_at"):
                qh = qh.filter(Hist.created_at >= start, Hist.created_at <= end_dt)

            if hasattr(Hist, "order_id"):
                qh = qh.filter(Hist.order_id.in_(order_ids))
            elif hasattr(Hist, "table_id"):
                tbl_ids = list({getattr(o, "table_id", None) for o in orders if getattr(o, "table_id", None) is not None})
                if tbl_ids:
                    qh = qh.filter(Hist.table_id.in_(tbl_ids))
                    dbg["note"].append("order_id が履歴に無いので table_id でフォールバックしています。")

            rows = qh.all()
            dbg["hist_rows_scanned"] = len(rows)
            app.logger.debug("[sales_report] Hist rows scanned: %s", len(rows))

            # 同一 order_id の最新を採用
            latest = {}
            for r in rows:
                oid = getattr(r, "order_id", None)
                if oid is None:
                    continue
                # ソートキー：created_at > id
                if hasattr(r, "created_at") and getattr(r, "created_at", None):
                    skey = str(getattr(r, "created_at"))
                else:
                    skey = f"{getattr(r, 'id', 0):08d}"
                if (oid not in latest) or (skey > latest[oid][0]):
                    latest[oid] = (skey, r)

            for oid, (_k, r) in latest.items():
                # 1) 合計人数があれば優先
                total = None
                if hasattr(r, "合計人数"):
                    total = _to_int(getattr(r, "合計人数"))
                # 2) なければ列合算
                if total is None:
                    a = _to_int(getattr(r, "大人男性", 0))
                    b = _to_int(getattr(r, "大人女性", 0))
                    c = _to_int(getattr(r, "子ども男", 0))
                    d = _to_int(getattr(r, "子ども女", 0))
                    ssum = a + b + c + d
                    total = ssum if ssum > 0 else None
                if total is not None:
                    guests_map[oid] = total

            # プレビュー（先頭10件）
            for oid in order_ids[:10]:
                dbg["guests_per_order_preview"][oid] = guests_map.get(oid)

        else:
            if not Hist:
                msg = "履歴モデルが見つかりません（CustomerDetailHistory / TCustomerDetailHistory / CustomerInfoHistory / VisitHistory / T_お客様詳細履歴 のいずれかを定義してください）"
                dbg["note"].append(msg)
                app.logger.debug("[sales_report] %s", msg)
            if not order_ids:
                app.logger.debug("[sales_report] 対象注文が 0 件です。")

        # guests を最終決定（履歴が無いものは最低 1 名）
        guests_total = 0
        for o in orders:
            oid = getattr(o, "id", None)
            g = guests_map.get(oid)
            if g is None:
                g = 1
            guests_total += max(0, int(g))
        dbg["guests_total"] = guests_total
        app.logger.debug("[sales_report] guests_total=%s / orders=%s", guests_total, len(orders))

        # ===== 概要（OrderItemから再計算：取り消しを除外） =====
        # 全注文の明細を取得
        qi_all = (
            s.query(OrderItem)
             .filter(OrderItem.order_id.in_(order_ids))
             .all()
        )
        
        # 注文IDごとに明細をグループ化
        from collections import defaultdict
        items_by_order = defaultdict(list)
        for it in qi_all:
            oid = getattr(it, "order_id", None)
            if oid:
                items_by_order[oid].append(it)
        
        # 各注文の実売上を計算
        total_subtotal = 0
        total_tax = 0
        total_total = 0
        for oid in order_ids:
            items = items_by_order.get(oid, [])
            totals = _calculate_order_totals(items)
            total_subtotal += totals["subtotal"]
            total_tax += totals["tax"]
            total_total += totals["total"]
        
        overview = {
            "count_orders": len(orders),
            "subtotal": total_subtotal,
            "tax":      total_tax,
            "total":    total_total,
            "guests":   guests_total,
        }

        # ===== 支払方法別 =====
        by_method_rows = (
            s.query(PaymentMethod.name, func.sum(PaymentRecord.amount).label("amount"))
             .join(PaymentRecord, PaymentRecord.method_id == PaymentMethod.id)
             .filter(PaymentRecord.paid_at >= start,
                     PaymentRecord.paid_at <= end_dt)
             .group_by(PaymentMethod.name)
             .all()
        )
        by_method = [{"name": (n or "-"), "amount": int(a or 0)} for (n, a) in by_method_rows]

        # ===== 日別（OrderItemから再計算：取り消しを除外） =====
        daily = defaultdict(lambda: {"orders": 0, "subtotal": 0, "tax": 0, "total": 0, "guests": 0})
        for o in orders:
            closed_at = getattr(o, "closed_at", None)
            if closed_at:
                # datetime型を文字列に変換してから日付部分を取得
                day = str(closed_at)[:10] if isinstance(closed_at, datetime) else str(closed_at)[:10]
            else:
                day = ""
            if not day:
                continue
            
            oid = getattr(o, "id", None)
            if not oid:
                continue
            
            # この注文の明細から実売上を計算
            items = items_by_order.get(oid, [])
            totals = _calculate_order_totals(items)
            
            d = daily[day]
            d["orders"]   += 1
            d["subtotal"] += totals["subtotal"]
            d["tax"]      += totals["tax"]
            d["total"]    += totals["total"]
            
            # 注文ごとの人数を加算（履歴が無ければ 1）
            g = guests_map.get(oid) if oid is not None else None
            if g is None:
                g = 1
            try:
                d["guests"] += max(0, int(g))
            except Exception:
                d["guests"] += 0

        # avg_per_guest を付与してテンプレへ
        days = []
        for k, v in sorted(daily.items()):
            guests = int(v.get("guests", 0) or 0)
            avg = (int(v["total"]) // guests) if guests > 0 else 0
            days.append({
                "day": k,
                "orders": v["orders"],
                "subtotal": v["subtotal"],
                "tax": v["tax"],
                "total": v["total"],
                "guests": guests,
                "avg_per_guest": avg,
            })

        # ===== メニュー別 =====
        qi = (
            s.query(OrderItem, OrderHeader)
             .join(OrderHeader, OrderItem.order_id == OrderHeader.id)
             .filter(OrderHeader.opened_at >= start,
                     OrderHeader.opened_at <= end_dt)
        )
        if hasattr(OrderHeader, "status"):
            qi = qi.filter(~OrderHeader.status.in_(EXCLUDED))
        if hasattr(OrderHeader, "store_id") and sid is not None:
            qi = qi.filter(OrderHeader.store_id == sid)
        item_rows = qi.all()

        agg = defaultdict(lambda: {"qty": 0, "excl": 0, "tax": 0, "incl": 0})
        for it, _oh in item_rows:
            # ★★★ 追加: 取消判定ロジック ★★★
            qty = int(getattr(it, "qty", 0) or 0)
            if qty == 0:
                continue  # 数量0はスキップ

            # 明細の状態を取得（status または 状態）
            st = (getattr(it, "status", None) or getattr(it, "状態", None) or "")
            st_low = str(st).lower()
            is_cancel_label = (
                ("取消" in st_low) or ("ｷｬﾝｾﾙ" in st_low) or ("キャンセル" in st_low)
                or ("cancel" in st_low) or ("void" in st_low)
            )

            # 正数量かつ取消ラベルは集計除外。負数量は "取消" でも必ず集計。
            if qty > 0 and is_cancel_label:
                continue
            # ★★★ ここまで追加 ★★★

            name = (getattr(getattr(it, "menu", None), "name", None) or f"#{getattr(it, 'menu_id', '')}")
            excl = int(getattr(it, "unit_price", 0) or 0) * qty
            rate = float(getattr(it, "tax_rate", 0.0) or 0.0)
            tax  = int(math.floor(excl * rate))
            a = agg[name]
            a["qty"]  += qty
            a["excl"] += excl
            a["tax"]  += tax
            a["incl"] += excl + tax
        by_menu = [{"name": k, **v} for k, v in sorted(agg.items())]

        # orders も渡す（テンプレ側の保険で使用可）
        return render_template(
            "sales_report.html",
            start=start, end=end,
            orders=orders,
            overview=overview,
            by_method=by_method,
            days=days,                      # ← guests/avg_per_guest 付き
            by_menu=by_menu,
            debug_info=json.dumps(dbg, ensure_ascii=False)
        )
    finally:
        s.close()



# ---------------------------------------------------------------------
# API: 日別売上
# ---------------------------------------------------------------------
# --- API：日別売上（/api/sales/daily） -------------------------------------------
@app.route("/api/sales/daily")
@require_store_admin
def api_sales_daily():
    from collections import defaultdict
    from datetime import datetime
    s = SessionLocal()
    try:
        sid = current_store_id()
        start, end = _range_from_params("start_date", "end_date", default_days=30)
        end_dt = _end_of_day(end)

        # 除外ステータス（統合済は売上・件数に載せない）
        EXCLUDED_STATUSES = {"統合済", "integrated", "merged"}

        # ---- 期間内の会計済み伝票を取得（closed_atベース）
        q = (
            s.query(OrderHeader)
             .filter(
                 OrderHeader.closed_at.isnot(None),
                 OrderHeader.closed_at >= start,
                 OrderHeader.closed_at <= end_dt
             )
        )
        if hasattr(OrderHeader, "store_id") and sid is not None:
            q = q.filter(OrderHeader.store_id == sid)
        if hasattr(OrderHeader, "status"):
            q = q.filter(OrderHeader.status == "会計済")
            q = q.filter(~OrderHeader.status.in_(list(EXCLUDED_STATUSES)))

        orders = q.all()

        def _ival(x):
            try:
                return int(x or 0)
            except Exception:
                try:
                    return int(float(x or 0))
                except Exception:
                    return 0

        # 日別集計の土台（売上・件数）
        daily = defaultdict(lambda: {
            "order_count": 0,
            "total_sales": 0,
            "subtotal":    0,
            "tax_amount":  0,
            "guests":      0,   # ← あとで埋める
        })

        order_day = {}   # order_id -> 'YYYY-MM-DD'
        order_ids = []
        for o in orders:
            closed_at = getattr(o, "closed_at", None)
            if closed_at:
                # datetime型を文字列に変換してから日付部分を取得
                day = str(closed_at)[:10] if isinstance(closed_at, datetime) else str(closed_at)[:10]
            else:
                day = ""
            if not day:
                # closed_atが無い/壊れている場合は opened_at にフォールバック
                opened_at = getattr(o, "opened_at", None)
                if opened_at:
                    day = str(opened_at)[:10] if isinstance(opened_at, datetime) else str(opened_at)[:10]
                else:
                    day = ""
                if not day:
                    continue
            
            oid = getattr(o, "id", None)
            if oid is not None:
                order_day[oid] = day
                order_ids.append(oid)
        
        # 全注文の明細を取得してOrderItemから再計算（取り消しを除外）
        if order_ids:
            qi_all = s.query(OrderItem).filter(OrderItem.order_id.in_(order_ids)).all()
            items_by_order = defaultdict(list)
            for it in qi_all:
                oid = getattr(it, "order_id", None)
                if oid:
                    items_by_order[oid].append(it)
            
            # 各注文の実売上を計算して日別に集計
            for oid in order_ids:
                day = order_day.get(oid)
                if not day:
                    continue
                items = items_by_order.get(oid, [])
                totals = _calculate_order_totals(items)
                
                d = daily[day]
                d["order_count"] += 1
                d["total_sales"] += totals["total"]
                d["subtotal"]    += totals["subtotal"]
                d["tax_amount"]  += totals["tax"]

        # ---- 来客者数集計（重複防止：各伝票につき1レコードのみ採用）
        # 1) 履歴テーブルがあれば「最新」を採用
        GHist = (globals().get("GuestDetailHistory")
                 or globals().get("TCustomerDetailHistory")
                 or globals().get("CustomerInfoHistory")
                 or globals().get("VisitHistory")
                 or globals().get("T_お客様詳細履歴"))

        def _best_key(r):
            """最新判定キー（created_at > updated_at > id）"""
            for nm in ("created_at", "updated_at"):
                if hasattr(r, nm) and getattr(r, nm, None):
                    try:
                        # 文字列/Datetime どちらでも文字列比較で安定するように
                        return str(getattr(r, nm))
                    except Exception:
                        pass
            return f"{getattr(r, 'id', 0):020d}"

        used_orders = set()
        if GHist is not None and order_ids:
            qg = s.query(GHist).filter(getattr(GHist, "order_id").in_(order_ids))
            if hasattr(GHist, "store_id") and sid is not None:
                qg = qg.filter(getattr(GHist, "store_id") == sid)
            # 期間で緩く絞る（created_at があれば）
            if hasattr(GHist, "created_at"):
                qg = qg.filter(getattr(GHist, "created_at") >= start,
                               getattr(GHist, "created_at") <= end_dt)

            latest = {}  # order_id -> record
            for r in qg.all():
                oid = getattr(r, "order_id", None)
                if oid not in order_day:
                    continue
                key = _best_key(r)
                if (oid not in latest) or (key > _best_key(latest[oid])):
                    latest[oid] = r

            for oid, r in latest.items():
                day = order_day.get(oid)
                if not day:
                    continue
                # 合計人数を優先
                total = 0
                for nm in ("合計人数", "total", "人数"):
                    if hasattr(r, nm):
                        try:
                            total = int(getattr(r, nm) or 0)
                            break
                        except Exception:
                            pass
                # 無ければ内訳合算
                if total <= 0:
                    for nm in ("大人男性","adult_male","men",
                               "大人女性","adult_female","women",
                               "子ども男","boys","子供男",
                               "子ども女","girls","子供女"):
                        if hasattr(r, nm):
                            total += _ival(getattr(r, nm))
                # 未記録は安全側で 1 人扱い
                if total <= 0:
                    total = 1
                daily[day]["guests"] += total
                used_orders.add(oid)

        # 2) 履歴が無い or 拾えなかった伝票は「現在値」から補完（同様に1件/伝票）
        remaining = [oid for oid in order_ids if oid not in used_orders]
        if remaining:
            GCur = globals().get("GuestDetail") or globals().get("T_お客様詳細")
            if GCur is not None:
                qg2 = s.query(GCur).filter(getattr(GCur, "order_id").in_(remaining))
                if hasattr(GCur, "store_id") and sid is not None:
                    qg2 = qg2.filter(getattr(GCur, "store_id") == sid)

                # 最新（updated_at or id）で1件/伝票を選ぶ
                chosen = {}
                for r in qg2.all():
                    oid = getattr(r, "order_id", None)
                    if oid not in order_day:
                        continue
                    key = _best_key(r)
                    if (oid not in chosen) or (key > _best_key(chosen[oid])):
                        chosen[oid] = r

                for oid, r in chosen.items():
                    day = order_day.get(oid)
                    if not day:
                        continue
                    total = 0
                    for nm in ("合計人数", "total", "人数"):
                        if hasattr(r, nm):
                            try:
                                total = int(getattr(r, nm) or 0); break
                            except Exception:
                                pass
                    if total <= 0:
                        for nm in ("大人男性","adult_male","men",
                                   "大人女性","adult_female","women",
                                   "子ども男","boys","子供男",
                                   "子ども女","girls","子供女"):
                            if hasattr(r, nm):
                                total += _ival(getattr(r, nm))
                    if total <= 0:
                        total = 1
                    daily[day]["guests"] += total
                    used_orders.add(oid)

        # 3) それでも人数が入らなかった伝票は 1 人で埋める
        for oid in order_ids:
            if oid not in used_orders:
                day = order_day.get(oid)
                if day:
                    daily[day]["guests"] += 1

        # ---- 出力成形（客単価: total_sales / guests の整数割）
        out = []
        for k in sorted(daily.keys()):
            v = daily[k]
            guests = int(v.get("guests", 0))
            avg = (int(v["total_sales"]) // guests) if guests > 0 else 0
            out.append({
                "date": k,
                "order_count": v["order_count"],
                "subtotal": v["subtotal"],
                "tax_amount": v["tax_amount"],
                "total_sales": v["total_sales"],
                "guests": guests,
                "avg_sales_per_guest": avg,  # 客単価（税込）
            })

        return jsonify({
            "status": "success",
            "data": out,
            "period": {"start": start, "end": end}
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})
    finally:
        s.close()




# ---------------------------------------------------------------------
# API: 月別売上（closed_at の先頭7桁でグルーピング）
# ---------------------------------------------------------------------
# --- API：月別売上（/api/sales/monthly） -----------------------------------------
@app.route("/api/sales/monthly")
@require_store_admin
def api_sales_monthly():
    s = SessionLocal()
    try:
        year = str(request.args.get("year", datetime.now().year))

        rows = (s.query(OrderHeader)
                  .filter(OrderHeader.closed_at.isnot(None),
                          (OrderHeader.closed_at >= f"{year}-01-01"),
                          (OrderHeader.closed_at <= f"{year}-12-31 23:59:59"),
                          OrderHeader.status == "会計済")
                  .all())

        # 注文IDを収集
        order_ids = [getattr(o, "id", None) for o in rows if getattr(o, "id", None) is not None]
        order_month = {}  # order_id -> 'YYYY-MM'
        for o in rows:
            oid = getattr(o, "id", None)
            if oid:
                month = (str(o.closed_at or ""))[:7]  # YYYY-MM
                order_month[oid] = month
        
        # 全注文の明細を取得してOrderItemから再計算（取り消しを除外）
        monthly = defaultdict(lambda: {"order_count":0, "total_sales":0, "subtotal":0, "tax_amount":0})
        if order_ids:
            qi_all = s.query(OrderItem).filter(OrderItem.order_id.in_(order_ids)).all()
            items_by_order = defaultdict(list)
            for it in qi_all:
                oid = getattr(it, "order_id", None)
                if oid:
                    items_by_order[oid].append(it)
            
            # 各注文の実売上を計算して月別に集計
            for oid in order_ids:
                month = order_month.get(oid)
                if not month:
                    continue
                items = items_by_order.get(oid, [])
                totals = _calculate_order_totals(items)
                
                m = monthly[month]
                m["order_count"] += 1
                m["total_sales"] += totals["total"]
                m["subtotal"]    += totals["subtotal"]
                m["tax_amount"]  += totals["tax"]

        out = []
        for k in sorted(monthly.keys()):
            v = monthly[k]
            out.append({"month": k, **v})

        return jsonify({"status":"success","data":out,"year":year})
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})
    finally:
        s.close()



# ---------------------------------------------------------------------
# API: 商品別売上
# ---------------------------------------------------------------------
# --- API：商品別売上（/api/sales/products） --------------------------------------
from collections import defaultdict
import math

@app.route("/api/sales/products")
@require_store_admin
def api_sales_products():
    """
    メニュー別売上（会計済みのみ）
    - 正数量かつ取消系ラベルを含む明細は除外（会計前の取り消し）
    - 負数量（監査の相殺行）は取消ラベルでもそのままネットに反映
    - 税は「単価×税率」を floor して数量分積み上げ（内税想定）

    /api/sales/products?debug=1 で詳細ログ＋レスポンスにデバッグを含める
    """
    from collections import defaultdict
    import math

    # 入口ログ
    try:
        qs = request.query_string.decode("utf-8", errors="ignore")
    except Exception:
        qs = ""
    current_app.logger.info("[/api/sales/products] ENTER q=%s", qs)

    # periodを安全に文字列化
    def _to_iso_safe(x):
        try:
            return x.isoformat()
        except Exception:
            return str(x)

    s = SessionLocal()
    try:
        debug_mode = str(request.args.get("debug", "0")).lower() in ("1", "true", "yes", "on")

        # 期間
        start, end = _range_from_params("start_date", "end_date", default_days=30)
        end_dt = _end_of_day(end)

        # 店舗
        sid = current_store_id()

        # クエリ（Menu JOINでN+1回避）
        q = (
            s.query(OrderItem, OrderHeader, Menu)
             .join(OrderHeader, OrderItem.order_id == OrderHeader.id)
             .join(Menu, Menu.id == OrderItem.menu_id)
             .filter(
                 OrderHeader.closed_at.isnot(None),
                 OrderHeader.closed_at >= start,
                 OrderHeader.closed_at <= end_dt,
             )
        )
        if hasattr(OrderHeader, "status"):
            q = q.filter(OrderHeader.status.in_(["会計済", "closed", "paid"])) \
                 .filter(~OrderHeader.status.in_(["統合済", "integrated", "merged"]))
        if hasattr(OrderHeader, "store_id") and sid is not None:
            q = q.filter(OrderHeader.store_id == sid)

        rows = q.all()

        if debug_mode:
            current_app.logger.info("[/api/sales/products] rows=%s sid=%s period=%s..%s",
                                    len(rows), sid, start, end_dt)

        agg = defaultdict(lambda: {
            "total_qty": 0,
            "total_sales": 0,
            "total_sales_incl": 0,
            "tax_total": 0,
            "sum_unit_price": 0,
            "sum_unit_price_incl": 0,
            "count_unit_price": 0,
        })

        cancel_words_ja = ("取消", "ｷｬﾝｾﾙ", "キャンセル", "削除")
        cancel_words_en = ("cancel", "void", "voided")

        dbg = {
            "seen": 0,
            "included": 0,
            "excluded_cancel_posqty": 0,
            "skipped_zero_qty": 0,
            "neg_qty_kept": 0,
            "no_cancel_but_posqty_kept": 0,
            "examples": []
        }
        EXAMPLE_LIMIT = 20

        for it, oh, m in rows:
            dbg["seen"] += 1

            qty = int(getattr(it, "qty", 0) or getattr(it, "数量", 0) or 0)
            if qty == 0:
                dbg["skipped_zero_qty"] += 1
                if debug_mode and len(dbg["examples"]) < EXAMPLE_LIMIT:
                    dbg["examples"].append({
                        "decision": "skip_zero_qty",
                        "order_id": getattr(it, "order_id", None),
                        "item_id": getattr(it, "id", None),
                        "menu_id": getattr(it, "menu_id", None),
                        "menu_name": getattr(m, "name", None),
                        "qty": qty,
                        "status": getattr(it, "status", None) or getattr(it, "状態", None),
                        "memo": getattr(it, "memo", None) or getattr(it, "メモ", None),
                    })
                continue

            st  = (getattr(it, "status", None) or getattr(it, "状態", None) or "")
            mm  = (getattr(it, "memo",   None) or getattr(it, "メモ",   None) or "")
            s_all = f"{st} {mm}"
            is_cancel = (any(w in s_all for w in cancel_words_ja)
                         or any(w in s_all.lower() for w in cancel_words_en))

            if qty > 0 and is_cancel:
                dbg["excluded_cancel_posqty"] += 1
                if debug_mode and len(dbg["examples"]) < EXAMPLE_LIMIT:
                    dbg["examples"].append({
                        "decision": "excluded_cancel_posqty",
                        "order_id": getattr(it, "order_id", None),
                        "item_id": getattr(it, "id", None),
                        "menu_id": getattr(it, "menu_id", None),
                        "menu_name": getattr(m, "name", None),
                        "qty": qty,
                        "status": st, "memo": mm,
                    })
                if debug_mode:
                    current_app.logger.info(
                        "[sales-products] EXCLUDE (cancel&pos) oid=%s iid=%s menu=%s qty=%s status='%s' memo='%s'",
                        getattr(it, "order_id", None), getattr(it, "id", None),
                        getattr(m, "name", None), qty, st, mm
                    )
                continue

            unit_excl = int(
                getattr(it, "unit_price", None)
                or getattr(it, "税抜単価", None)
                or getattr(it, "price_excl", None)
                or getattr(it, "price", None)
                or 0
            )
            rate = float(
                getattr(it, "tax_rate", None)
                or getattr(getattr(it, "menu", None), "tax_rate", None)
                or getattr(m, "tax_rate", None)
                or 0.10
            )
            unit_tax  = math.floor(unit_excl * rate)
            unit_incl = unit_excl + unit_tax

            name = (getattr(m, "name", None)
                    or getattr(getattr(it, "menu", None), "name", None)
                    or f"#{getattr(it, 'menu_id', None)}")

            a = agg[name]
            a["total_qty"]           += qty
            a["total_sales"]         += unit_excl * qty
            a["tax_total"]           += unit_tax  * qty
            a["total_sales_incl"]    += unit_incl * qty
            a["sum_unit_price"]      += unit_excl
            a["sum_unit_price_incl"] += unit_incl
            a["count_unit_price"]    += 1

            dbg["included"] += 1
            if qty < 0:
                dbg["neg_qty_kept"] += 1
            else:
                dbg["no_cancel_but_posqty_kept"] += 1

            if debug_mode and len(dbg["examples"]) < EXAMPLE_LIMIT:
                dbg["examples"].append({
                    "decision": "included",
                    "order_id": getattr(it, "order_id", None),
                    "item_id": getattr(it, "id", None),
                    "menu_id": getattr(it, "menu_id", None),
                    "menu_name": getattr(m, "name", None),
                    "qty": qty,
                    "status": st, "memo": mm,
                    "unit_excl": unit_excl, "rate": rate,
                })

        out = []
        for name, v in sorted(agg.items(), key=lambda x: x[1]["total_sales_incl"], reverse=True):
            cnt = v["count_unit_price"] or 1
            avg_excl = v["sum_unit_price"] / cnt
            avg_incl = v["sum_unit_price_incl"] / cnt
            out.append({
                "product_name": name,
                "total_qty": int(v["total_qty"]),
                "total_sales": int(v["total_sales"]),
                "total_sales_incl": int(v["total_sales_incl"]),
                "tax_total": int(v["tax_total"]),
                "avg_price": round(avg_excl, 2),
                "avg_price_incl": round(avg_incl, 2),
            })

        resp = {
            "status": "success",
            "data": out,
            "period": {"start": _to_iso_safe(start), "end": _to_iso_safe(end)}
        }
        if debug_mode:
            resp["debug"] = {
                "summary": dbg | {"examples_count": len(dbg["examples"])},
                "examples": dbg["examples"],
                "cancel_words": {"ja": cancel_words_ja, "en": cancel_words_en},
            }
        return jsonify(resp)

    except Exception as e:
        current_app.logger.exception("[/api/sales/products] ERROR: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        s.close()



# ---------------------------------------------------------------------
# API: 支払方法別売上
# ---------------------------------------------------------------------
# --- API：支払方法別売上（/api/sales/payment_methods） ----------------------------
@app.route("/api/sales/payment_methods")
@require_store_admin
def api_sales_payment_methods():
    s = SessionLocal()
    try:
        start, end = _range_from_params("start_date", "end_date", default_days=30)
        end_dt = _end_of_day(end)

        rows = (
            s.query(PaymentMethod.name, PaymentRecord.amount, PaymentRecord.paid_at)
             .join(PaymentRecord, PaymentRecord.method_id == PaymentMethod.id)
             .filter(PaymentRecord.paid_at >= start,
                     PaymentRecord.paid_at <= end_dt)
             .all()
        )

        agg = defaultdict(lambda: {"transaction_count":0, "total_amount":0})
        for name, amount, _paid in rows:
            key = name or "-"
            agg[key]["transaction_count"] += 1
            agg[key]["total_amount"]      += int(amount or 0)

        out = []
        for k, v in sorted(agg.items(), key=lambda x: x[1]["total_amount"], reverse=True):
            out.append({"payment_method": k, **v})

        return jsonify({"status":"success","data":out,"period":{"start":start,"end":end}})
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})
    finally:
        s.close()



# ---------------------------------------------------------------------
# API: 売上サマリー
# ---------------------------------------------------------------------
# --- API：売上サマリー（/api/sales/summary） -------------------------------------
@app.route("/api/sales/summary")
@require_store_admin
def api_sales_summary():
    s = SessionLocal()
    try:
        start, end = _range_from_params("start_date", "end_date", default_days=30)
        end_dt = _end_of_day(end)

        rows = (s.query(OrderHeader)
                  .filter(OrderHeader.closed_at.isnot(None),
                          OrderHeader.closed_at >= start,
                          OrderHeader.closed_at <= end_dt,
                          OrderHeader.status == "会計済")
                  .all())

        total_orders = len(rows)
        total_sales  = sum(int(o.total or 0)    for o in rows)
        total_sub    = sum(int(o.subtotal or 0) for o in rows)
        total_tax    = sum(int(o.tax or 0)      for o in rows)
        avg_order    = (total_sales/total_orders) if total_orders else 0

        today = _today_str()
        today_rows = [o for o in rows if (o.closed_at or "")[:10] == today]
        today_orders = len(today_rows)
        today_sales  = sum(int(o.total or 0) for o in today_rows)

        return jsonify({
            "status":"success",
            "data":{
                "period":{"start":start,"end":end},
                "total_orders": total_orders,
                "total_sales":  total_sales,
                "total_subtotal": total_sub,
                "total_tax": total_tax,
                "avg_order_value": round(avg_order, 2),
                "today_orders": today_orders,
                "today_sales": today_sales
            }
        })
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})
    finally:
        s.close()



# ---------------------------------------------------------------------
# エクスポート: 日別 CSV
# ---------------------------------------------------------------------
# --- エクスポート：日別CSV（/api/sales/export/daily） -----------------------------
@app.route("/api/sales/export/daily")
@require_store_admin
def export_daily_sales():
    s = SessionLocal()
    try:
        start, end = _range_from_params("start_date", "end_date", default_days=30)
        end_dt = _end_of_day(end)

        rows = (s.query(OrderHeader)
                  .filter(OrderHeader.closed_at.isnot(None),
                          OrderHeader.closed_at >= start,
                          OrderHeader.closed_at <= end_dt,
                          OrderHeader.status == "会計済")
                  .all())

        daily = defaultdict(lambda: {"order_count":0, "total_sales":0, "subtotal":0, "tax_amount":0})
        for o in rows:
            day = (o.closed_at or "")[:10]
            d = daily[day]
            d["order_count"] += 1
            d["total_sales"] += int(o.total or 0)
            d["subtotal"]    += int(o.subtotal or 0)
            d["tax_amount"]  += int(o.tax or 0)

        import csv, io
        output = io.StringIO()
        w = csv.writer(output)
        w.writerow(["日付","注文数","売上合計","税抜合計","税額"])
        for k in sorted(daily.keys()):
            v = daily[k]
            w.writerow([k, v["order_count"], v["total_sales"], v["subtotal"], v["tax_amount"]])

        resp = make_response(output.getvalue())
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = f'attachment; filename=daily_sales_{start}_to_{end}.csv'
        return resp
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})
    finally:
        s.close()



# ---------------------------------------------------------------------
# エクスポート: 商品別 CSV
# ---------------------------------------------------------------------
# --- エクスポート：商品別CSV（/api/sales/export/products） ------------------------
@app.route("/api/sales/export/products")
@require_store_admin
def export_products_sales():
    s = SessionLocal()
    try:
        start, end = _range_from_params("start_date", "end_date", default_days=30)
        end_dt = _end_of_day(end)

        item_rows = (
            s.query(OrderItem, OrderHeader)
             .join(OrderHeader, OrderItem.order_id == OrderHeader.id)
             .filter(OrderHeader.closed_at.isnot(None),
                     OrderHeader.closed_at >= start,
                     OrderHeader.closed_at <= end_dt,
                     OrderHeader.status == "会計済")
             .all()
        )

        agg = defaultdict(lambda: {"total_qty":0, "total_sales":0, "sum_unit_price":0, "count_unit_price":0})
        for it, _oh in item_rows:
            name = it.menu.name if it.menu else f"#{it.menu_id}"
            qty  = int(it.qty or 0)
            unit = int(it.unit_price or 0)
            agg[name]["total_qty"]       += qty
            agg[name]["total_sales"]     += qty * unit
            agg[name]["sum_unit_price"]  += unit
            agg[name]["count_unit_price"]+= 1

        import csv, io
        output = io.StringIO()
        w = csv.writer(output)
        w.writerow(["商品名","販売数量","売上合計","平均単価"])
        for k, v in sorted(agg.items(), key=lambda x: x[1]["total_sales"], reverse=True):
            avg = (v["sum_unit_price"]/v["count_unit_price"]) if v["count_unit_price"] else 0
            w.writerow([k, v["total_qty"], v["total_sales"], round(avg,2)])

        resp = make_response(output.getvalue())
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = f'attachment; filename=products_sales_{start}_to_{end}.csv'
        return resp
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})
    finally:
        s.close()


# ---------------------------------------------------------------------
# API: 時間別売上（指定日の 00-23 時）
# ---------------------------------------------------------------------
# --- API：時間別売上（/api/sales/hourly, api_sales_hourly） -----------------------
@app.route("/api/sales/hourly")
@require_store_admin
def api_sales_hourly():
    s = SessionLocal()
    try:
        date = (request.args.get("date") or _today_str()).strip()
        start = date
        end   = date
        end_dt = _end_of_day(end)

        rows = (s.query(OrderHeader)
                  .filter(OrderHeader.closed_at.isnot(None),
                          OrderHeader.closed_at >= start,
                          OrderHeader.closed_at <= end_dt,
                          OrderHeader.status == "会計済")
                  .all())

        buckets = {h: {"hour": f"{h:02d}:00", "order_count":0, "total_sales":0} for h in range(24)}
        for o in rows:
            ts = (o.closed_at or "00:00:00")[11:13]  # "YYYY-MM-DD HH"
            try:
                hh = int(ts)
            except Exception:
                hh = 0
            buckets[hh]["order_count"] += 1
            buckets[hh]["total_sales"] += int(o.total or 0)

        out = [buckets[h] for h in range(24)]
        return jsonify({"status":"success","data":out,"date":date})
    except Exception as e:
        return jsonify({"status":"error","message":str(e)})
    finally:
        s.close()


# =========================================================
# 店員呼び出しAPI
# =========================================================
# 店員呼び出し状態を保持するグローバル変数（簡易実装）
_staff_calls = []  # [{"table_no": "1", "timestamp": 1234567890, "store_id": 1}]
_staff_calls_lock = threading.Lock()

@app.route("/api/staff_call", methods=["POST"])
def api_staff_call():
    """
    お客様が店員を呼び出すAPI
    POST JSON:
      { "token": "<qr token>", "table_no": "<テーブル番号>" }
    
    レスポンス:
      { "ok": true, "table_no": "<テーブル番号>" }
    """
    global _staff_calls
    try:
        data = request.get_json(force=True) or {}
        token = data.get("token", "")
        table_no = data.get("table_no", "不明")
        store_id = None
        
        # トークン検証（既存のQRトークン検証ロジックを使用）
        s = SessionLocal()
        try:
            # QRトークンからテーブル情報を取得
            qr = s.query(QrToken).filter(QrToken.token == token).first()
            if qr and qr.table_id:
                table = s.get(TableSeat, qr.table_id)
                if table:
                    table_no = getattr(table, "テーブル番号", table_no)
                    store_id = getattr(table, "store_id", None) or getattr(qr, "store_id", None)
        except Exception as e:
            app.logger.warning(f"[api_staff_call] token validation warning: {e}")
        finally:
            s.close()
        
        # 呼び出しを記録
        with _staff_calls_lock:
            _staff_calls.append({
                "table_no": table_no,
                "timestamp": int(time.time()),
                "store_id": store_id
            })
            # 古い呼び出しを削除（60秒以上前）
            cutoff = int(time.time()) - 60
            _staff_calls = [c for c in _staff_calls if c["timestamp"] > cutoff]
        
        # ログに記録
        app.logger.info(f"[STAFF_CALL] テーブル {table_no} から店員呼び出し")
        
        return jsonify({"ok": True, "table_no": table_no})
    
    except Exception as e:
        app.logger.error(f"[api_staff_call] error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/staff_call/poll", methods=["GET"])
@require_any
def api_staff_call_poll():
    """
    KDSが店員呼び出しをポーリングするAPI
    レスポンス:
      { "ok": true, "calls": [{"table_no": "1", "timestamp": 1234567890}] }
    """
    global _staff_calls
    try:
        sid = current_store_id()
        since = int(request.args.get("since", "0"))
        
        with _staff_calls_lock:
            # 店舗IDでフィルタリング
            calls = [
                {"table_no": c["table_no"], "timestamp": c["timestamp"]}
                for c in _staff_calls
                if c["timestamp"] > since and (sid is None or c["store_id"] == sid)
            ]
        
        return jsonify({"ok": True, "calls": calls})
    
    except Exception as e:
        app.logger.error(f"[api_staff_call_poll] error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# =========================================================
# メニューオプション取得API
# =========================================================
@app.route("/api/menu/<int:menu_id>/options")
def api_menu_options(menu_id: int):
    """
    指定されたメニューに適用されるオプションと選択肢を返す
    """
    s = SessionLocal()
    try:
        # 商品を取得
        menu = s.get(Menu, menu_id)
        if not menu:
            return jsonify({"ok": False, "error": "Menu not found"}), 404
        
        # この商品に適用されるオプションを取得（既に辞書形式で返される）
        store_id = menu.store_id
        options_data = get_product_options(s, menu_id, store_id)
        
        return jsonify({"ok": True, "options": options_data})
    except Exception as e:
        app.logger.error(f"[api_menu_options] error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        s.close()


# =========================================================
# 会計取消（再開）API：会計済 → 会計中 に戻す（席が未再利用のときだけ）
# =========================================================
@app.route("/admin/order/<int:order_id>/reopen", methods=["POST"])
@require_store_admin
def admin_reopen_order(order_id: int):
    """
    会計済(= closed/paid/会計済) の伝票を「会計中」に戻す。
    ただし、当該テーブルがすでに再利用（=別のオープンな伝票が存在 or テーブル状態が占有）されている場合は 409 を返す。

    返却:
      200: {
        ok: True,
        restored_guests: bool,            # ヘッダ側の来客情報を履歴から復元できたか
        totals_recalculated: bool,        # 再計算できたか（取消含めてネット再計算）
        restored_detail: {...}            # 現在値テーブルへ upsert した来客詳細（あれば）
      }
      4xx/5xx: { ok: False, error: "..." }
    """
    s = SessionLocal()
    try:
        sid = current_store_id()

        # --- 伝票取得 & 店舗スコープ検証
        h = s.get(OrderHeader, order_id)
        if not h:
            return jsonify({"ok": False, "error": "order not found"}), 404
        if hasattr(OrderHeader, "store_id") and sid is not None:
            if getattr(h, "store_id", None) != sid:
                return jsonify({"ok": False, "error": "forbidden"}), 403

        # --- 会計済み判定（日本語/英語どちらも許容）
        st_raw = (getattr(h, "status", "") or "").strip()
        st_low = st_raw.lower()
        CLOSED_RAW = {"会計済"}
        CLOSED_LOW = {"closed", "paid"}
        is_closed = (st_raw in CLOSED_RAW) or (st_low in CLOSED_LOW)
        if not is_closed:
            return jsonify({"ok": False, "error": "order is not closed"}), 400

        # --- テーブル再利用チェック（同一テーブルに他のアクティブ伝票があればNG）
        table_id = getattr(h, "table_id", None)
        t = s.get(TableSeat, table_id) if table_id else None

        ACTIVE_ORDER_STATUSES = {
            "open", "pending", "in_progress", "serving", "unpaid",
            "会計中", "新規", "調理中", "提供済"
        }

        q_active = s.query(OrderHeader).filter(
            OrderHeader.table_id == table_id,
            OrderHeader.id != order_id
        )
        if hasattr(OrderHeader, "store_id") and sid is not None:
            q_active = q_active.filter(OrderHeader.store_id == sid)
        if hasattr(OrderHeader, "status"):
            q_active = q_active.filter(OrderHeader.status.in_(list(ACTIVE_ORDER_STATUSES)))

        other_active_exists = bool(s.query(q_active.exists()).scalar())

        # TableSeat が占有表示でも、他のアクティブ伝票が無ければ許容
        table_in_use = False
        if t is not None:
            t_status = (getattr(t, "status", "") or "").strip()
            if t_status in {"着席", "使用中", "occupied"}:
                table_in_use = other_active_exists

        if other_active_exists or table_in_use:
            return jsonify({"ok": False, "error": "table already reused"}), 409

        # --- 再開：状態を「会計中」に戻し、閉鎖日時クリア
        if hasattr(h, "status"):
            h.status = "会計中"
        for f in ("closed_at", "精算日時", "closedAt"):
            if hasattr(h, f):
                try:
                    setattr(h, f, None)
                except Exception:
                    app.logger.debug("[admin_reopen_order] clear %s failed", f)

        # テーブル側の状態も着席に寄せる（存在する場合）
        if t is not None and hasattr(t, "status"):
            try:
                t.status = "着席"
            except Exception:
                app.logger.debug("[admin_reopen_order] set TableSeat.status failed")

        # --- 来客情報 復元（ヘッダ）※任意ヘルパ
        restored_guests = False
        if "restore_customer_detail_from_history" in globals():
            try:
                restored_guests = bool(restore_customer_detail_from_history(s, order_id))
            except Exception as e:
                app.logger.exception("[admin_reopen_order] restore header guests failed: %s", e)

        # --- 来客情報 現在値テーブルへ upsert（注文画面で 0 にならないように）
        restored_detail = {}
        if "_upsert_guest_detail_from_history" in globals():
            try:
                restored_detail = _upsert_guest_detail_from_history(s, order_id) or {}
            except Exception as e:
                app.logger.exception("[admin_reopen_order] upsert current guests failed: %s", e)

        # --- ★ 金額再計算（取消を含めてネットで再計算：DBベース）
        totals_recalculated = False
        try:
            _recalc_order_totals_with_negatives_db(s, order_id, h)   # ← ここが重要
            totals_recalculated = True
        except Exception as e:
            app.logger.exception("[admin_reopen_order] recalc totals (including negatives) failed: %s", e)

        s.commit()
        mark_floor_changed()

        return jsonify({
            "ok": True,
            "restored_guests": restored_guests,
            "totals_recalculated": totals_recalculated,
            "restored_detail": restored_detail
        })

    except Exception as e:
        s.rollback()
        app.logger.exception("[admin_reopen_order] %s", e)
        return jsonify({"ok": False, "error": "internal error"}), 500
    finally:
        s.close()



# ---------------------------------------------------------------------
# システム管理者用：店舗IDマスター管理画面
# ---------------------------------------------------------------------
# --- 店舗IDマスター一覧（store_master_list） --------------------------------------
@app.route("/sysadmin/store_master")
@require_sysadmin
def store_master_list():
    """店舗IDマスター一覧"""
    s = SessionLocal()
    try:
        stores = s.query(M_店舗IDマスター).order_by(M_店舗IDマスター.店舗ID).all()
        return render_template("store_master_list.html", stores=stores)
    finally:
        s.close()


# --- 店舗IDマスター新規登録（store_master_add） -----------------------------------
@app.route("/sysadmin/store_master/add", methods=["GET", "POST"])
@require_sysadmin
def store_master_add():
    """店舗IDマスター新規登録"""
    s = SessionLocal()
    try:
        if request.method == "POST":
            store_code = (request.form.get("store_code") or "").strip()
            store_name = (request.form.get("store_name") or "").strip()
            note = (request.form.get("note") or "").strip()
            
            if not store_code or not store_name:
                flash("店舗コードと店舗名は必須です。")
                return render_template("store_master_form.html")
            
            # 重複チェック
            existing = s.query(M_店舗IDマスター).filter(
                M_店舗IDマスター.店舗コード == store_code
            ).first()
            if existing:
                flash("この店舗コードは既に登録されています。")
                return render_template("store_master_form.html")
            
            new_store = M_店舗IDマスター(
                店舗コード=store_code,
                店舗名=store_name,
                有効フラグ=1,
                備考=note
            )
            s.add(new_store)
            s.commit()
            flash("店舗IDマスターに登録しました。")
            return redirect(url_for("store_master_list"))
        
        return render_template("store_master_form.html")
    finally:
        s.close()


# --- 店舗IDマスター編集（store_master_edit） --------------------------------------
@app.route("/sysadmin/store_master/<int:store_id>/edit", methods=["GET", "POST"])
@require_sysadmin
def store_master_edit(store_id):
    """店舗IDマスター編集"""
    s = SessionLocal()
    try:
        store = s.get(M_店舗IDマスター, store_id)
        if not store:
            abort(404)
        
        if request.method == "POST":
            store_code = (request.form.get("store_code") or "").strip()
            store_name = (request.form.get("store_name") or "").strip()
            note = (request.form.get("note") or "").strip()
            active = int(request.form.get("active", 1))
            
            if not store_code or not store_name:
                flash("店舗コードと店舗名は必須です。")
                return render_template("store_master_form.html", store=store)
            
            # 重複チェック（自分以外）
            existing = s.query(M_店舗IDマスター).filter(
                M_店舗IDマスター.店舗コード == store_code,
                M_店舗IDマスター.店舗ID != store_id
            ).first()
            if existing:
                flash("この店舗コードは既に登録されています。")
                return render_template("store_master_form.html", store=store)
            
            store.店舗コード = store_code
            store.店舗名 = store_name
            store.有効フラグ = active
            store.備考 = note
            s.commit()
            flash("店舗IDマスターを更新しました。")
            return redirect(url_for("store_master_list"))
        
        return render_template("store_master_form.html", store=store)
    finally:
        s.close()


# --- 店舗IDマスター削除（store_master_delete） ------------------------------------
@app.route("/sysadmin/store_master/<int:store_id>/delete", methods=["POST"])
@require_sysadmin
def store_master_delete(store_id):
    """店舗IDマスター削除"""
    s = SessionLocal()
    try:
        store = s.get(M_店舗IDマスター, store_id)
        if not store:
            abort(404)
        
        s.delete(store)
        s.commit()
        flash("店舗IDマスターを削除しました。")
        return redirect(url_for("store_master_list"))
    finally:
        s.close()


# ---------------------------------------------------------------------
# 開発者ツール画面（システム管理者用）
# ---------------------------------------------------------------------
# --- 開発者ツール画面（dev_tools） -----------------------------------------------
@app.route("/dev_tools")
@require_sysadmin
def dev_tools():
    import os
    from flask import abort
    if os.getenv("ENABLE_DEV_TOOLS") != "1":
        abort(404)
    """開発者ツール画面"""
    s = SessionLocal()
    try:
        # 統計情報を取得
        tenant_count = s.query(M_テナント).count()
        store_count = s.query(Store).count()
        store_master_count = s.query(M_店舗IDマスター).count()
        
        stats = {
            "tenant_count": tenant_count,
            "store_count": store_count,
            "store_master_count": store_master_count
        }
        
        return render_template("dev_tools.html", stats=stats)
    finally:
        s.close()


# ================================================
# お客様情報（来店者詳細）モデル & API & 注文前ガード
# ================================================
# ---- 既存モデルが無ければ定義（あればスキップ） ----
# --- モデル：T_お客様詳細（存在しなければ定義） -----------------------------------
try:
    T_お客様詳細  # type: ignore[name-defined]
except NameError:
    try:
        Base  # type: ignore[name-defined]
    except NameError:
        from sqlalchemy.orm import declarative_base
        Base = declarative_base()  # type: ignore[assignment]
    class T_お客様詳細(Base):  # type: ignore[no-redef]
        __tablename__ = "T_お客様詳細"
        id       = Column(Integer, primary_key=True)
        store_id = Column(Integer, index=True, nullable=True)
        order_id = Column(Integer, index=True, nullable=True)
        table_id = Column(Integer, index=True, nullable=True)
        大人男性 = Column(Integer, default=0)
        大人女性 = Column(Integer, default=0)
        子ども男 = Column(Integer, default=0)   # 男子（小学生以下）
        子ども女 = Column(Integer, default=0)   # 女子（小学生以下）
        作成日時 = Column(DateTime(timezone=True), server_default=func.now())
        更新日時 = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ===== 履歴テーブルモデル（未定義なら定義） =====
try:
    T_お客様詳細履歴  # 既にあるならこのブロックはスキップ
except NameError:
    from sqlalchemy import Column, Integer, Text, DateTime
    from sqlalchemy.sql import func

    class T_お客様詳細履歴(Base):  # type: ignore[misc]
        __tablename__ = "T_お客様詳細履歴"
        id         = Column(Integer, primary_key=True)
        store_id   = Column(Integer, index=True, nullable=True)
        order_id   = Column(Integer, index=True, nullable=True)
        table_id   = Column(Integer, index=True, nullable=True)
        大人男性     = Column(Integer, default=0)
        大人女性     = Column(Integer, default=0)
        子ども男     = Column(Integer, default=0)
        子ども女     = Column(Integer, default=0)
        合計人数      = Column(Integer, default=0)
        version    = Column(Integer, nullable=False)
        変更理由      = Column(Text)
        作成者       = Column(Text)
        created_at = Column(DateTime(timezone=True), server_default=func.now())


# ===== 会計完了時に履歴行を追記する関数 =====
from sqlalchemy import func as sa_func, desc

def append_checkout_customer_detail_history(
    s,
    *,
    order_id: int,
    store_id=None,
    table_id=None,
    reason: str = "会計完了",
    author: str | None = None,
):
    """
    会計が完全に成功したタイミングで呼び出して、
    現在の人数情報を T_お客様詳細履歴 に version を増やして追記する。
    まず order_id で検索し、見つからなければ table_id で最新をフォールバック取得。
    commit は呼び出し元で行う。
    """

    # --- 現在の人数情報を取得（order_id → table_id の順でフォールバック） ---
    model = globals().get("M_顧客詳細") or globals().get("T_お客様詳細")
    a_m = a_f = k_m = k_f = 0
    src_store_id = store_id
    src_table_id = table_id

    cur = None
    if model is not None:
        # ① order_id 優先
        if hasattr(model, "order_id"):
            q1 = s.query(model).filter(getattr(model, "order_id") == order_id)
            q1 = q1.order_by(
                desc(getattr(model, "更新日時")) if hasattr(model, "更新日時") else desc(getattr(model, "id"))
            )
            cur = q1.first()

        # ② 見つからなければ table_id で最新1件
        if cur is None and table_id is not None and hasattr(model, "table_id"):
            q2 = s.query(model).filter(getattr(model, "table_id") == table_id)
            q2 = q2.order_by(
                desc(getattr(model, "更新日時")) if hasattr(model, "更新日時") else desc(getattr(model, "id"))
            )
            cur = q2.first()

        if cur:
            a_m = int(getattr(cur, "大人男性", 0) or 0)
            a_f = int(getattr(cur, "大人女性", 0) or 0)
            k_m = int(getattr(cur, "子ども男", 0) or 0)
            k_f = int(getattr(cur, "子ども女", 0) or 0)
            if src_store_id is None:
                src_store_id = getattr(cur, "store_id", None)
            if src_table_id is None:
                src_table_id = getattr(cur, "table_id", None)

    total = max(0, a_m + a_f + k_m + k_f)

    # --- version 採番（同一 order_id の最大+1。無ければ 1） ---
    max_ver = (
        s.query(sa_func.coalesce(sa_func.max(T_お客様詳細履歴.version), 0))
         .filter(T_お客様詳細履歴.order_id == order_id)
         .scalar()
    )
    next_ver = int(max_ver or 0) + 1

    # --- 追記 INSERT ---
    rec = T_お客様詳細履歴(
        store_id=src_store_id,
        order_id=order_id,
        table_id=src_table_id,
        大人男性=a_m,
        大人女性=a_f,
        子ども男=k_m,
        子ども女=k_f,
        合計人数=total,
        version=next_ver,
        変更理由=reason,
        作成者=author,
    )
    s.add(rec)
    return rec




# --- ヘルパ：非負整数に整形（_int_nonneg） ----------------------------------------
def _int_nonneg(v, default=0):
    try:
        n = int(v)
        return n if n >= 0 else 0
    except Exception:
        return default


# --- ヘルパ：公開トークンから table_id 解決（_resolve_public_table_by_token） ------
def _resolve_public_table_by_token(s, token: str):
    try:
        qt = s.query(QrToken).filter(QrToken.token == token).first()  # type: ignore[name-defined]
        return qt.table_id if qt else None
    except Exception:
        return None


# --- ヘルパ：未クローズ注文の解決（_resolve_open_order） ---------------------------
def _resolve_open_order(s, *, order_id=None, token=None, table_id=None):
    try:
        if order_id:
            oh = s.get(OrderHeader, int(order_id))  # type: ignore[name-defined]
            if oh: return oh
        if token:
            oh = (s.query(OrderHeader)  # type: ignore[name-defined]
                    .filter(OrderHeader.session_token == str(token))
                    .order_by(OrderHeader.id.desc())
                    .first())
            if oh: return oh
        if table_id:
            oh = (s.query(OrderHeader)  # type: ignore[name-defined]
                    .filter(
                        OrderHeader.table_id == int(table_id),
                        OrderHeader.status.in_(["新規","調理中","提供済","会計中"])
                    )
                    .order_by(OrderHeader.id.desc())
                    .first())
            if oh: return oh
        return None
    except Exception:
        return None


# --- ヘルパ：お客様情報の取得 or 作成（_get_or_create_customer_detail） ------------
def _get_or_create_customer_detail(s, *, store_id=None, order_id=None, table_id=None):
    q = s.query(T_お客様詳細)
    if order_id:
        row = q.filter(T_お客様詳細.order_id == order_id).order_by(T_お客様詳細.id.desc()).first()
        if row: return row
    if table_id:
        row = (q.filter(T_お客様詳細.table_id == table_id, T_お客様詳細.order_id == None)
                 .order_by(T_お客様詳細.id.desc()).first())
        if row: return row
    row = T_お客様詳細(store_id=store_id, order_id=order_id, table_id=table_id)
    s.add(row); s.flush()
    return row


# --- ヘルパ：お客様情報の最新取得（_fetch_customer_detail） ------------------------
def _fetch_customer_detail(s, *, order_id=None, table_id=None):
    q = s.query(T_お客様詳細)
    if order_id:
        row = q.filter(T_お客様詳細.order_id == order_id).order_by(T_お客様詳細.id.desc()).first()
        if row: return row
    if table_id:
        return q.filter(T_お客様詳細.table_id == table_id).order_by(T_お客様詳細.id.desc()).first()
    return None


# --- ヘルパ：会計完了時にお客様詳細をリセット（_reset_customer_detail_after_checkout） ----------------
def _reset_customer_detail_after_checkout(s, *, order_id=None, table_id=None) -> dict:
    """
    会計完了後に T_お客様詳細 をリセットする（片テーブル専用）。
    1) order_id 一致を削除
    2) table_id 一致＆order_id IS NULL（孤児）を削除
    3) 上記がどちらも 0 件だった場合、フォールバックで table_id 一致を全削除（慎重運用）
    事前に最大5件を覗きログします。
    戻り：{"model":"T_お客様詳細","by_order":n1,"orphans":n2,"fallback_by_table":n3,"peek": [...], "order_id":..., "table_id":...}
    """
    Model = globals().get("T_お客様詳細")
    if Model is None:
        current_app.logger.warning("[reset_customer_detail] T_お客様詳細 not found")
        return {"model": None, "by_order": 0, "orphans": 0, "fallback_by_table": 0, "peek": [], "order_id": order_id, "table_id": table_id}

    # 事前に覗く（直近5件）
    peek_rows = []
    try:
        q = s.query(Model)
        if table_id is not None:
            q = q.filter(Model.table_id == table_id)
        for r in q.order_by(Model.id.desc()).limit(5).all():
            peek_rows.append({
                "id": getattr(r, "id", None),
                "order_id": getattr(r, "order_id", None),
                "table_id": getattr(r, "table_id", None),
                "store_id": getattr(r, "store_id", None),
            })
    except Exception as e:
        current_app.logger.exception("[reset_customer_detail][peek] failed: %s", e)

    del_by_order = del_orphans = del_fallback = 0

    # 1) 注文に紐づく行を削除
    if order_id is not None:
        del_by_order = s.query(Model).filter(Model.order_id == order_id)\
            .delete(synchronize_session=False)

    # 2) 孤児（table_id 一致 & order_id IS NULL）を削除
    if table_id is not None:
        del_orphans = s.query(Model).filter(
            Model.table_id == table_id,
            Model.order_id == None  # noqa: E711
        ).delete(synchronize_session=False)

    # 3) どちらも 0 なら、フォールバック：table_id 一致を全削除（※最終手段）
    if (del_by_order + del_orphans) == 0 and table_id is not None:
        del_fallback = s.query(Model).filter(Model.table_id == table_id)\
            .delete(synchronize_session=False)

    current_app.logger.info(
        "[reset_customer_detail] model=T_お客様詳細 order_id=%s table_id=%s -> by_order=%s, orphans=%s, fallback=%s, peek=%s",
        order_id, table_id, del_by_order, del_orphans, del_fallback, peek_rows
    )

    return {
        "model": "T_お客様詳細",
        "by_order": del_by_order,
        "orphans": del_orphans,
        "fallback_by_table": del_fallback,
        "peek": peek_rows,
        "order_id": order_id,
        "table_id": table_id,
    }





# ---- 保存 ----
# --- API：お客様情報 保存（POST /api/customer_detail, api_customer_detail_post） ---
@app.post("/api/customer_detail")
def api_customer_detail_post():
    s = SessionLocal()
    try:
        j = request.get_json(silent=True) or {}
        token    = (j.get("token") or "").strip() or None
        order_id = j.get("order_id")
        table_id = j.get("table_id")

        if token and not table_id:
            table_id = _resolve_public_table_by_token(s, token)
            if not table_id:
                return jsonify(ok=False, error="token から table_id を解決できません"), 400
        if not token and not order_id and not table_id:
            return jsonify(ok=False, error="order_id または table_id を指定してください"), 400

        men   = _int_nonneg(j.get("男性", j.get("men", 0)))
        women = _int_nonneg(j.get("女性", j.get("women", 0)))
        boys  = _int_nonneg(j.get("男子", j.get("boys", 0)))
        girls = _int_nonneg(j.get("女子", j.get("girls", 0)))

        oh = _resolve_open_order(s, order_id=order_id, token=token, table_id=table_id)
        if oh:
            order_id = oh.id
            table_id = table_id or getattr(oh, "table_id", None)

        try:
            store_id = current_store_id()  # type: ignore[name-defined]
        except Exception:
            store_id = getattr(oh, "store_id", None) if oh else None

        row = _get_or_create_customer_detail(s, store_id=store_id, order_id=order_id, table_id=table_id)
        row.大人男性 = men; row.大人女性 = women; row.子ども男 = boys; row.子ども女 = girls
        if oh and getattr(row, "order_id", None) != oh.id:
            row.order_id = oh.id
        if getattr(row, "table_id", None) in (None, 0) and table_id is not None:
            row.table_id = table_id
        if getattr(row, "store_id", None) in (None, 0) and store_id is not None:
            row.store_id = store_id

        s.commit()
        return jsonify(ok=True, id=row.id, order_id=row.order_id, table_id=row.table_id, store_id=row.store_id,
                       detail={"大人男性": row.大人男性 or 0, "大人女性": row.大人女性 or 0,
                               "子ども男": row.子ども男 or 0, "子ども女": row.子ども女 or 0})
    except Exception as e:
        s.rollback()
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 500
    finally:
        s.close()


# ---- ステータス ----
# --- API：お客様情報 ステータス確認（GET /api/customer_detail/status） ------------
@app.get("/api/customer_detail/status")
def api_customer_detail_status():
    s = SessionLocal()
    try:
        token    = (request.args.get("token") or "").strip() or None
        order_id = request.args.get("order_id", type=int)
        table_id = request.args.get("table_id", type=int)
        if token and not table_id:
            table_id = _resolve_public_table_by_token(s, token)
        row = _fetch_customer_detail(s, order_id=order_id, table_id=table_id)
        if not row:
            return jsonify(ok=True, exists=False)
        return jsonify(ok=True, exists=True, detail={
            "id": row.id, "store_id": row.store_id, "order_id": row.order_id, "table_id": row.table_id,
            "大人男性": row.大人男性 or 0, "大人女性": row.大人女性 or 0,
            "子ども男": row.子ども男 or 0, "子ども女": row.子ども女 or 0,
        })
    except Exception as e:
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 500
    finally:
        s.close()


# ---- 注文前バリデーション ----
# --- ヘルパ：お客様情報送信済みチェック（ensure_customer_detail_submitted） ------
def ensure_customer_detail_submitted(s, *, order_id=None, token=None, table_id=None):
    if token and not table_id:
        table_id = _resolve_public_table_by_token(s, token)
    row = _fetch_customer_detail(s, order_id=order_id, table_id=table_id)
    if not row:
        raise ValueError("お客様情報（来店者詳細）が未送信です")
    return True


# --- before_request：/api/order ガード（_guard_customer_detail_before_order） ------
@app.before_request
def _guard_customer_detail_before_order():
    try:
        if request.method != "POST":
            return
        # Public
        if request.path == "/api/order":
            j = request.get_json(silent=True) or {}
            token = (j.get("token") or "").strip()
            s = SessionLocal()
            try:
                ensure_customer_detail_submitted(s, token=token)
            except ValueError as ve:
                return jsonify(ok=False, error=str(ve)), 400
            finally:
                s.close()
            return
        # Staff（関数名が staff_api_order の場合を想定。endpoint 解決後にのみ有効）
        if request.endpoint == "staff_api_order":
            j = request.get_json(silent=True) or {}
            order_id = j.get("order_id")
            table_id = j.get("table_id")
            s = SessionLocal()
            try:
                ensure_customer_detail_submitted(s, order_id=order_id, table_id=table_id)
            except ValueError as ve:
                return jsonify(ok=False, error=str(ve)), 400
            finally:
                s.close()
            return
    except Exception:
        # 失敗しても注文処理自体は続行（ログだけにする場合はここでreturnしない）
        return


# ---- 起動時マイグレーション：T_お客様詳細 が無ければ作成・不足列を追加 ----
# --- マイグレーション：T_お客様詳細 を保証（_ensure_customer_detail_table） -------
def _ensure_customer_detail_table():
    s = SessionLocal()
    try:
        bind = s.get_bind()
        insp = sqla_inspect(bind)
        if not insp.has_table("T_お客様詳細"):
            try:
                T_お客様詳細.__table__.create(bind=bind)
                app.logger.debug('[migrate] created table T_お客様詳細')
            except Exception as e:
                app.logger.exception('[migrate] create T_お客様詳細 failed: %s', e)
                raise
        # 不足列の追加（SQLite: ADD COLUMN しかできない）
        try:
            cols = {c['name'] for c in insp.get_columns("T_お客様詳細")}
        except Exception:
            cols = set()
        add_cols = []
        if "大人男性" not in cols:
            add_cols.append(('大人男性', 'INTEGER', '0'))
        if "大人女性" not in cols:
            add_cols.append(('大人女性', 'INTEGER', '0'))
        if "子ども男" not in cols:
            add_cols.append(('子ども男', 'INTEGER', '0'))
        if "子ども女" not in cols:
            add_cols.append(('子ども女', 'INTEGER', '0'))
        for name, typ, default in add_cols:
            try:
                bind.execute(_sa_text(f'ALTER TABLE "T_お客様詳細" ADD COLUMN "{name}" {typ} DEFAULT {default}'))
                app.logger.debug(f'[migrate] add column T_お客様詳細.{name}')
            except Exception as e:
                app.logger.exception('[migrate] add column %s failed: %s', name, e)
        # index は必要に応じて
    finally:
        s.close()


# --- 起動時：T_お客様詳細 の存在保証呼び出し（_ensure_customer_detail_table） ------
try:
    _ensure_customer_detail_table()
except Exception:
    pass



# ============ お客様情報 編集API（取得・更新・削除） ============
# 既存: POST /api/customer_detail は upsert（新規/更新）ですが、
# 明示的な編集用として GET/PUT/DELETE を用意します。

# --- ヘルパ：お客様情報の特定（_find_customer_detail） ----------------------------
def _find_customer_detail(s, *, id=None, order_id=None, token=None, table_id=None):
    if id:
        return s.query(T_お客様詳細).get(int(id))
    if token and not table_id:
        table_id = _resolve_public_table_by_token(s, token)
    row = _fetch_customer_detail(s, order_id=order_id, table_id=table_id)
    return row


# --- API：お客様情報 取得（GET /api/customer_detail/<cid>） ------------------------
@app.get("/api/customer_detail/<int:cid>")
def api_customer_detail_get_by_id(cid):
    s = SessionLocal()
    try:
        row = _find_customer_detail(s, id=cid)
        if not row:
            return jsonify(ok=False, error="not found"), 404
        return jsonify(ok=True, detail={
            "id": row.id, "store_id": row.store_id, "order_id": row.order_id, "table_id": row.table_id,
            "大人男性": row.大人男性 or 0, "大人女性": row.大人女性 or 0,
            "子ども男": row.子ども男 or 0, "子ども女": row.子ども女 or 0,
        })
    except Exception as e:
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 500
    finally:

        s.close()

# --- API：お客様情報 更新（PUT /api/customer_detail/<cid>） ------------------------
@app.put("/api/customer_detail/<int:cid>")
def api_customer_detail_put_by_id(cid):
    s = SessionLocal()
    try:
        j = request.get_json(silent=True) or {}
        row = _find_customer_detail(s, id=cid)
        if not row:
            return jsonify(ok=False, error="not found"), 404

        men   = _int_nonneg(j.get("男性", j.get("men", row.大人男性 or 0)))
        women = _int_nonneg(j.get("女性", j.get("women", row.大人女性 or 0)))
        boys  = _int_nonneg(j.get("男子", j.get("boys",  row.子ども男 or 0)))
        girls = _int_nonneg(j.get("女子", j.get("girls", row.子ども女 or 0)))

        row.大人男性 = men
        row.大人女性 = women
        row.子ども男 = boys
        row.子ども女 = girls

        s.commit()
        return jsonify(ok=True, detail={
            "id": row.id, "store_id": row.store_id, "order_id": row.order_id, "table_id": row.table_id,
            "大人男性": row.大人男性 or 0, "大人女性": row.大人女性 or 0,
            "子ども男": row.子ども男 or 0, "子ども女": row.子ども女 or 0,
        })
    except Exception as e:
        s.rollback()
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 500
    finally:
        s.close()


# --- API：お客様情報 更新（IDなし, PUT /api/customer_detail） ----------------------
@app.put("/api/customer_detail")
def api_customer_detail_put():
    s = SessionLocal()
    try:
        j = request.get_json(silent=True) or {}
        token    = (j.get("token") or "").strip() or None
        order_id = j.get("order_id")
        table_id = j.get("table_id")
        row = _find_customer_detail(s, order_id=order_id, token=token, table_id=table_id)
        if not row:
            return jsonify(ok=False, error="対象のお客様情報が見つかりません"), 404

        men   = _int_nonneg(j.get("男性", j.get("men", row.大人男性 or 0)))
        women = _int_nonneg(j.get("女性", j.get("women", row.大人女性 or 0)))
        boys  = _int_nonneg(j.get("男子", j.get("boys",  row.子ども男 or 0)))
        girls = _int_nonneg(j.get("女子", j.get("girls", row.子ども女 or 0)))

        row.大人男性 = men
        row.大人女性 = women
        row.子ども男 = boys
        row.子ども女 = girls
        s.commit()
        return jsonify(ok=True, detail={
            "id": row.id, "store_id": row.store_id, "order_id": row.order_id, "table_id": row.table_id,
            "大人男性": row.大人男性 or 0, "大人女性": row.大人女性 or 0,
            "子ども男": row.子ども男 or 0, "子ども女": row.子ども女 or 0,
        })
    except Exception as e:
        s.rollback()
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 500
    finally:
        s.close()


# --- API：お客様情報 削除（DELETE /api/customer_detail/<cid>） ---------------------
@app.delete("/api/customer_detail/<int:cid>")
def api_customer_detail_delete(cid):
    s = SessionLocal()
    try:
        row = _find_customer_detail(s, id=cid)
        if not row:
            return jsonify(ok=False, error="not found"), 404
        s.delete(row)
        s.commit()
        return jsonify(ok=True)
    except Exception as e:
        s.rollback()
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 500
    finally:
        s.close()



# ========================================
# テーブル移動取消機能
# ========================================

# ---------------------------------------------------------------------
# テーブル移動取消の条件チェック
# ---------------------------------------------------------------------
def _check_cancel_conditions(s, sid, history):
    """
    テーブル移動の取消が可能かチェックする
    
    Returns:
        tuple: (can_cancel: bool, reasons: list, error_code: str)
    """
    reasons = []
    error_code = None
    
    TOrder = globals().get("T_注文") or globals().get("OrderHeader")
    TItem = globals().get("T_注文明細") or globals().get("OrderItem")
    TPay = globals().get("T_支払") or globals().get("PaymentRecord")
    THistory = globals().get("T_テーブル移動履歴")
    
    # 1. 既に取り消されているか
    if getattr(history, "is_cancelled", 0) == 1:
        reasons.append("既に取り消されています")
        error_code = "ALREADY_CANCELLED"
        return False, reasons, error_code
    
    # 2. 24時間以内か
    moved_at = getattr(history, "moved_at", None)
    if moved_at:
        now = datetime.now(timezone.utc)
        # タイムゾーンを考慮した比較
        if hasattr(moved_at, 'tzinfo') and moved_at.tzinfo is None:
            moved_at = moved_at.replace(tzinfo=timezone.utc)
        time_diff = now - moved_at
        if time_diff > timedelta(hours=24):
            reasons.append("移動から24時間を超えているため取り消せません")
            error_code = "TIME_LIMIT_EXCEEDED"
            return False, reasons, error_code
    
    # 3. 最新の移動か（該当テーブルに関する最新の履歴か）
    from_table_id = getattr(history, "from_table_id", None)
    to_table_id = getattr(history, "to_table_id", None)
    history_id = getattr(history, "id", None)
    
    if THistory and from_table_id:
        # 移動元または移動先に関する最新の履歴を取得
        latest = (s.query(THistory)
                   .filter(
                       (getattr(THistory, "from_table_id") == from_table_id) |
                       (getattr(THistory, "to_table_id") == from_table_id) |
                       (getattr(THistory, "from_table_id") == to_table_id) |
                       (getattr(THistory, "to_table_id") == to_table_id)
                   )
                   .filter(getattr(THistory, "is_cancelled", 0) == 0)
                   .order_by(getattr(THistory, "moved_at").desc())
                   .first())
        
        if latest and getattr(latest, "id", None) != history_id:
            reasons.append("この移動の後に別の移動が行われているため取り消せません")
            error_code = "NOT_LATEST_MOVE"
            return False, reasons, error_code
    
    # 4. 移動元テーブルが現在空席か
    if TOrder and from_table_id:
        active_statuses = ["新規", "調理中", "提供済", "会計中", "open", "pending", "in_progress", "serving", "unpaid"]
        active_order = (s.query(TOrder)
                        .filter(getattr(TOrder, "table_id") == from_table_id)
                        .filter(getattr(TOrder, "status").in_(active_statuses))
                        .first())
        if active_order:
            reasons.append("移動元テーブルに現在注文があるため取り消せません")
            error_code = "SOURCE_TABLE_IN_USE"
            return False, reasons, error_code
    
    # 5. テーブルが存在するか
    from_table = s.get(TableSeat, from_table_id) if from_table_id else None
    to_table = s.get(TableSeat, to_table_id) if to_table_id else None
    
    if not from_table:
        reasons.append("移動元テーブルが存在しません")
        error_code = "TABLE_NOT_FOUND"
        return False, reasons, error_code
    
    if to_table_id and not to_table:
        reasons.append("移動先テーブルが存在しません")
        error_code = "TABLE_NOT_FOUND"
        return False, reasons, error_code
    
    # 6. 支払い完了していないか（移動先の注文）
    mode = getattr(history, "mode", None)
    dest_order_id = getattr(history, "dest_order_id", None)
    new_order_id = getattr(history, "new_order_id", None)
    
    check_order_ids = []
    if mode == "merge" and dest_order_id:
        check_order_ids.append(dest_order_id)
    elif mode == "merge_new" and new_order_id:
        check_order_ids.append(new_order_id)
    elif mode == "swap":
        if dest_order_id:
            check_order_ids.append(dest_order_id)
        order_id = getattr(history, "order_id", None)
        if order_id:
            check_order_ids.append(order_id)
    else:  # move
        order_id = getattr(history, "order_id", None)
        if order_id:
            check_order_ids.append(order_id)
    
    for oid in check_order_ids:
        if TOrder:
            order = s.get(TOrder, oid)
            if order:
                status = getattr(order, "status", None)
                if status in ["会計済", "完了", "paid", "completed"]:
                    reasons.append("支払いが完了しているため取り消せません")
                    error_code = "ALREADY_PAID"
                    return False, reasons, error_code
    
    # 7. 移動後に新しい明細が追加されていないか
    if TItem and moved_at:
        for oid in check_order_ids:
            # 移動日時以降に作成された明細があるかチェック
            new_items = (s.query(TItem)
                         .filter(getattr(TItem, "order_id") == oid))
            
            # created_at列がある場合はそれでチェック
            if hasattr(TItem, "created_at"):
                new_items = new_items.filter(getattr(TItem, "created_at") > moved_at)
            elif hasattr(TItem, "作成日時"):
                new_items = new_items.filter(getattr(TItem, "作成日時") > moved_at)
            
            if new_items.count() > 0:
                reasons.append("移動後に新しい明細が追加されているため取り消せません")
                error_code = "NEW_ITEMS_ADDED"
                return False, reasons, error_code
    
    # すべてのチェックをパス
    return True, reasons, error_code


# ---------------------------------------------------------------------
# 通常移動(move)の取消
# ---------------------------------------------------------------------
def _restore_move(s, sid, history):
    """
    通常移動を取り消す
    
    Returns:
        dict: 復元結果
    """
    TOrder = globals().get("T_注文") or globals().get("OrderHeader")
    TCD = globals().get("T_お客様詳細")
    TQR = globals().get("QrToken")
    
    from_table_id = getattr(history, "from_table_id", None)
    to_table_id = getattr(history, "to_table_id", None)
    order_id = getattr(history, "order_id", None)
    
    result = {
        "mode": "move",
        "from_table_id": from_table_id,
        "to_table_id": to_table_id,
        "order_id": order_id,
    }
    
    # 注文を移動元テーブルに戻す
    if TOrder and order_id:
        order = s.get(TOrder, order_id)
        if order:
            setattr(order, "table_id", from_table_id)
            result["order_restored"] = True
    
    # お客様詳細を移動元テーブルに戻す
    if TCD and order_id:
        s.query(TCD).filter(getattr(TCD, "order_id") == order_id)\
            .update({getattr(TCD, "table_id"): from_table_id}, synchronize_session=False)
        result["customer_detail_restored"] = True
    
    # QRトークンを移動元テーブルに戻す（最新のもの）
    if TQR and to_table_id and from_table_id:
        if hasattr(TQR, "issued_at"):
            sub = (s.query(TQR.table_id, func.max(TQR.issued_at).label("mx"))
                     .filter(TQR.store_id == sid, TQR.table_id == to_table_id)
                     .group_by(TQR.table_id)).subquery()
            latest = (s.query(TQR)
                        .join(sub, and_(TQR.table_id == sub.c.table_id,
                                        TQR.issued_at == sub.c.mx))
                        .filter(TQR.store_id == sid).first())
        else:
            sub = (s.query(TQR.table_id, func.max(TQR.id).label("mx"))
                     .filter(TQR.store_id == sid, TQR.table_id == to_table_id)
                     .group_by(TQR.table_id)).subquery()
            latest = (s.query(TQR)
                        .join(sub, and_(TQR.table_id == sub.c.table_id,
                                        TQR.id == sub.c.mx))
                        .filter(TQR.store_id == sid).first())
        if latest:
            latest.table_id = from_table_id
            result["qr_restored"] = True
    
    # テーブルステータスを更新
    from_table = s.get(TableSeat, from_table_id)
    to_table = s.get(TableSeat, to_table_id)
    
    if from_table and hasattr(from_table, "status"):
        from_table.status = "着席"
    if to_table and hasattr(to_table, "status"):
        to_table.status = "空席"
    
    result["table_status_restored"] = True
    
    return result


# ---------------------------------------------------------------------
# 統合(merge)の取消
# ---------------------------------------------------------------------
def _restore_merge(s, sid, history):
    """
    統合(merge)を取り消す
    
    Returns:
        dict: 復元結果
    """
    TOrder = globals().get("T_注文") or globals().get("OrderHeader")
    TItem = globals().get("T_注文明細") or globals().get("OrderItem")
    TPay = globals().get("T_支払") or globals().get("PaymentRecord")
    TCD = globals().get("T_お客様詳細")
    
    from_table_id = getattr(history, "from_table_id", None)
    to_table_id = getattr(history, "to_table_id", None)
    src_order_id = getattr(history, "order_id", None)
    dest_order_id = getattr(history, "dest_order_id", None)
    
    result = {
        "mode": "merge",
        "from_table_id": from_table_id,
        "to_table_id": to_table_id,
        "src_order_id": src_order_id,
        "dest_order_id": dest_order_id,
    }
    
    # スナップショットから移動元の明細IDリストを取得
    source_snapshot_json = getattr(history, "source_items_snapshot", None)
    if not source_snapshot_json:
        raise ValueError("移動元の明細スナップショットがありません")
    
    source_snapshot = json.loads(source_snapshot_json)
    source_item_ids = [item["id"] for item in source_snapshot.get("items", [])]
    
    # 移動元の明細を元の注文IDに戻す
    if TItem and source_item_ids:
        s.query(TItem).filter(getattr(TItem, "id").in_(source_item_ids))\
            .update({getattr(TItem, "order_id"): src_order_id}, synchronize_session=False)
        result["items_restored"] = len(source_item_ids)
    
    # 移動元の支払いを元の注文IDに戻す（スナップショットから）
    source_payment_ids = [pay["id"] for pay in source_snapshot.get("payments", [])]
    if TPay and source_payment_ids:
        s.query(TPay).filter(getattr(TPay, "id").in_(source_payment_ids))\
            .update({getattr(TPay, "order_id"): src_order_id}, synchronize_session=False)
        result["payments_restored"] = len(source_payment_ids)
    
    # 移動元の注文ヘッダを再アクティブ化
    if TOrder and src_order_id:
        src_order = s.get(TOrder, src_order_id)
        if src_order:
            setattr(src_order, "table_id", from_table_id)
            if hasattr(TOrder, "status"):
                setattr(src_order, "status", getattr(history, "order_status", "新規"))
            
            # 合計を再計算
            _recalc_order_totals_from_items_simple(s, src_order_id, TOrder, TItem)
            result["src_order_reactivated"] = True
    
    # 移動先の注文ヘッダの合計を再計算
    if TOrder and dest_order_id:
        _recalc_order_totals_from_items_simple(s, dest_order_id, TOrder, TItem)
        result["dest_order_recalculated"] = True
    
    # お客様詳細を復元
    if TCD and src_order_id:
        # 既存のお客様詳細を削除
        s.query(TCD).filter(getattr(TCD, "order_id") == src_order_id).delete(synchronize_session=False)
        
        # スナップショットから復元
        cd_data = source_snapshot.get("customer_detail", {})
        if cd_data and cd_data.get("adult_male") is not None:
            new_cd = TCD()
            setattr(new_cd, "order_id", src_order_id)
            setattr(new_cd, "table_id", from_table_id)
            setattr(new_cd, "大人男性", cd_data.get("adult_male", 0))
            setattr(new_cd, "大人女性", cd_data.get("adult_female", 0))
            setattr(new_cd, "子ども男", cd_data.get("child_male", 0))
            setattr(new_cd, "子ども女", cd_data.get("child_female", 0))
            s.add(new_cd)
            result["customer_detail_restored"] = True
    
    # テーブルステータスを更新
    from_table = s.get(TableSeat, from_table_id)
    if from_table and hasattr(from_table, "status"):
        from_table.status = "着席"
    
    return result


# ---------------------------------------------------------------------
# 新伝票統合(merge_new)の取消
# ---------------------------------------------------------------------
def _restore_merge_new(s, sid, history):
    """
    新伝票統合(merge_new)を取り消す
    
    Returns:
        dict: 復元結果
    """
    TOrder = globals().get("T_注文") or globals().get("OrderHeader")
    TItem = globals().get("T_注文明細") or globals().get("OrderItem")
    TPay = globals().get("T_支払") or globals().get("PaymentRecord")
    TCD = globals().get("T_お客様詳細")
    
    from_table_id = getattr(history, "from_table_id", None)
    to_table_id = getattr(history, "to_table_id", None)
    src_order_id = getattr(history, "order_id", None)
    dest_order_id = getattr(history, "dest_order_id", None)
    new_order_id = getattr(history, "new_order_id", None)
    
    result = {
        "mode": "merge_new",
        "from_table_id": from_table_id,
        "to_table_id": to_table_id,
        "src_order_id": src_order_id,
        "dest_order_id": dest_order_id,
        "new_order_id": new_order_id,
    }
    
    # スナップショットを取得
    source_snapshot_json = getattr(history, "source_items_snapshot", None)
    dest_snapshot_json = getattr(history, "dest_items_snapshot", None)
    
    if not source_snapshot_json or not dest_snapshot_json:
        raise ValueError("明細スナップショットがありません")
    
    source_snapshot = json.loads(source_snapshot_json)
    dest_snapshot = json.loads(dest_snapshot_json)
    
    source_item_ids = [item["id"] for item in source_snapshot.get("items", [])]
    dest_item_ids = [item["id"] for item in dest_snapshot.get("items", [])]
    
    # 移動元の明細を元の注文IDに戻す
    if TItem and source_item_ids:
        s.query(TItem).filter(getattr(TItem, "id").in_(source_item_ids))\
            .update({getattr(TItem, "order_id"): src_order_id}, synchronize_session=False)
        result["src_items_restored"] = len(source_item_ids)
    
    # 移動先の明細を元の注文IDに戻す
    if TItem and dest_item_ids:
        s.query(TItem).filter(getattr(TItem, "id").in_(dest_item_ids))\
            .update({getattr(TItem, "order_id"): dest_order_id}, synchronize_session=False)
        result["dest_items_restored"] = len(dest_item_ids)
    
    # 支払いを元の注文IDに戻す
    source_payment_ids = [pay["id"] for pay in source_snapshot.get("payments", [])]
    dest_payment_ids = [pay["id"] for pay in dest_snapshot.get("payments", [])]
    
    if TPay and source_payment_ids:
        s.query(TPay).filter(getattr(TPay, "id").in_(source_payment_ids))\
            .update({getattr(TPay, "order_id"): src_order_id}, synchronize_session=False)
        result["src_payments_restored"] = len(source_payment_ids)
    
    if TPay and dest_payment_ids:
        s.query(TPay).filter(getattr(TPay, "id").in_(dest_payment_ids))\
            .update({getattr(TPay, "order_id"): dest_order_id}, synchronize_session=False)
        result["dest_payments_restored"] = len(dest_payment_ids)
    
    # 新規注文を削除
    if TOrder and new_order_id:
        new_order = s.get(TOrder, new_order_id)
        if new_order:
            s.delete(new_order)
            result["new_order_deleted"] = True
    
    # 移動元・移動先の注文ヘッダを再アクティブ化
    if TOrder and src_order_id:
        src_order = s.get(TOrder, src_order_id)
        if src_order:
            setattr(src_order, "table_id", from_table_id)
            if hasattr(TOrder, "status"):
                setattr(src_order, "status", getattr(history, "order_status", "新規"))
            _recalc_order_totals_from_items_simple(s, src_order_id, TOrder, TItem)
            result["src_order_reactivated"] = True
    
    if TOrder and dest_order_id:
        dest_order = s.get(TOrder, dest_order_id)
        if dest_order:
            setattr(dest_order, "table_id", to_table_id)
            if hasattr(TOrder, "status"):
                # 移動先の元のステータスを復元（スナップショットから）
                dest_status = "新規"
                if dest_snapshot_json:
                    dest_order_data = json.loads(dest_snapshot_json)
                    # ここでは簡易的に新規に設定
                setattr(dest_order, "status", dest_status)
            _recalc_order_totals_from_items_simple(s, dest_order_id, TOrder, TItem)
            result["dest_order_reactivated"] = True
    
    # お客様詳細を復元
    if TCD:
        # 新規注文のお客様詳細を削除
        if new_order_id:
            s.query(TCD).filter(getattr(TCD, "order_id") == new_order_id).delete(synchronize_session=False)
        
        # 移動元のお客様詳細を復元
        s.query(TCD).filter(getattr(TCD, "order_id") == src_order_id).delete(synchronize_session=False)
        cd_data = source_snapshot.get("customer_detail", {})
        if cd_data and cd_data.get("adult_male") is not None:
            new_cd = TCD()
            setattr(new_cd, "order_id", src_order_id)
            setattr(new_cd, "table_id", from_table_id)
            setattr(new_cd, "大人男性", cd_data.get("adult_male", 0))
            setattr(new_cd, "大人女性", cd_data.get("adult_female", 0))
            setattr(new_cd, "子ども男", cd_data.get("child_male", 0))
            setattr(new_cd, "子ども女", cd_data.get("child_female", 0))
            s.add(new_cd)
        
        # 移動先のお客様詳細を復元
        s.query(TCD).filter(getattr(TCD, "order_id") == dest_order_id).delete(synchronize_session=False)
        dest_cd_data = dest_snapshot.get("customer_detail", {})
        if dest_cd_data and dest_cd_data.get("adult_male") is not None:
            new_cd = TCD()
            setattr(new_cd, "order_id", dest_order_id)
            setattr(new_cd, "table_id", to_table_id)
            setattr(new_cd, "大人男性", dest_cd_data.get("adult_male", 0))
            setattr(new_cd, "大人女性", dest_cd_data.get("adult_female", 0))
            setattr(new_cd, "子ども男", dest_cd_data.get("child_male", 0))
            setattr(new_cd, "子ども女", dest_cd_data.get("child_female", 0))
            s.add(new_cd)
        
        result["customer_details_restored"] = True
    
    # テーブルステータスを更新
    from_table = s.get(TableSeat, from_table_id)
    to_table = s.get(TableSeat, to_table_id)
    
    if from_table and hasattr(from_table, "status"):
        from_table.status = "着席"
    if to_table and hasattr(to_table, "status"):
        to_table.status = "着席"
    
    return result


# ---------------------------------------------------------------------
# 交換(swap)の取消
# ---------------------------------------------------------------------
def _restore_swap(s, sid, history):
    """
    交換(swap)を取り消す
    
    Returns:
        dict: 復元結果
    """
    TOrder = globals().get("T_注文") or globals().get("OrderHeader")
    TCD = globals().get("T_お客様詳細")
    TQR = globals().get("QrToken")
    
    from_table_id = getattr(history, "from_table_id", None)
    to_table_id = getattr(history, "to_table_id", None)
    src_order_id = getattr(history, "order_id", None)
    dest_order_id = getattr(history, "dest_order_id", None)
    
    result = {
        "mode": "swap",
        "from_table_id": from_table_id,
        "to_table_id": to_table_id,
        "src_order_id": src_order_id,
        "dest_order_id": dest_order_id,
    }
    
    # 両方の注文のテーブルIDを元に戻す（交換を逆転）
    if TOrder and src_order_id and dest_order_id:
        src_order = s.get(TOrder, src_order_id)
        dest_order = s.get(TOrder, dest_order_id)
        
        if src_order and dest_order:
            # 現在のテーブルIDを取得
            current_src_table = getattr(src_order, "table_id", None)
            current_dest_table = getattr(dest_order, "table_id", None)
            
            # 交換を逆転
            setattr(src_order, "table_id", current_dest_table)
            setattr(dest_order, "table_id", current_src_table)
            result["orders_swapped_back"] = True
    
    # お客様詳細のテーブルIDを元に戻す
    if TCD:
        if src_order_id:
            s.query(TCD).filter(getattr(TCD, "order_id") == src_order_id)\
                .update({getattr(TCD, "table_id"): from_table_id}, synchronize_session=False)
        if dest_order_id:
            s.query(TCD).filter(getattr(TCD, "order_id") == dest_order_id)\
                .update({getattr(TCD, "table_id"): to_table_id}, synchronize_session=False)
        result["customer_details_swapped_back"] = True
    
    # QRトークンを元に戻す
    if TQR:
        # from_table_idのQRを元に戻す
        if hasattr(TQR, "issued_at"):
            sub = (s.query(TQR.table_id, func.max(TQR.issued_at).label("mx"))
                     .filter(TQR.store_id == sid, TQR.table_id == to_table_id)
                     .group_by(TQR.table_id)).subquery()
            latest = (s.query(TQR)
                        .join(sub, and_(TQR.table_id == sub.c.table_id,
                                        TQR.issued_at == sub.c.mx))
                        .filter(TQR.store_id == sid).first())
        else:
            sub = (s.query(TQR.table_id, func.max(TQR.id).label("mx"))
                     .filter(TQR.store_id == sid, TQR.table_id == to_table_id)
                     .group_by(TQR.table_id)).subquery()
            latest = (s.query(TQR)
                        .join(sub, and_(TQR.table_id == sub.c.table_id,
                                        TQR.id == sub.c.mx))
                        .filter(TQR.store_id == sid).first())
        if latest:
            latest.table_id = from_table_id
        
        # to_table_idのQRを元に戻す
        if hasattr(TQR, "issued_at"):
            sub = (s.query(TQR.table_id, func.max(TQR.issued_at).label("mx"))
                     .filter(TQR.store_id == sid, TQR.table_id == from_table_id)
                     .group_by(TQR.table_id)).subquery()
            latest = (s.query(TQR)
                        .join(sub, and_(TQR.table_id == sub.c.table_id,
                                        TQR.issued_at == sub.c.mx))
                        .filter(TQR.store_id == sid).first())
        else:
            sub = (s.query(TQR.table_id, func.max(TQR.id).label("mx"))
                     .filter(TQR.store_id == sid, TQR.table_id == from_table_id)
                     .group_by(TQR.table_id)).subquery()
            latest = (s.query(TQR)
                        .join(sub, and_(TQR.table_id == sub.c.table_id,
                                        TQR.id == sub.c.mx))
                        .filter(TQR.store_id == sid).first())
        if latest:
            latest.table_id = to_table_id
        
        result["qr_tokens_swapped_back"] = True
    
    # テーブルステータスを元に戻す（交換を逆転）
    from_table = s.get(TableSeat, from_table_id)
    to_table = s.get(TableSeat, to_table_id)
    
    if from_table and to_table and hasattr(from_table, "status") and hasattr(to_table, "status"):
        # ステータスを交換
        from_status = from_table.status
        to_status = to_table.status
        from_table.status = to_status
        to_table.status = from_status
        result["table_status_swapped_back"] = True
    
    return result


# ---------------------------------------------------------------------
# 合計再計算のヘルパー関数（簡易版）
# ---------------------------------------------------------------------
def _recalc_order_totals_from_items_simple(s, order_id, TOrder, TItem):
    """
    明細から合計を再計算してヘッダへ反映（簡易版）
    """
    if not (TOrder and TItem and order_id):
        return None
    
    items = s.query(TItem).filter(getattr(TItem, "order_id") == order_id).all()
    
    def _num(x, default=0):
        try:
            return int(x)
        except Exception:
            try:
                return float(x or 0)
            except Exception:
                return default
    
    sub = 0
    tax = 0
    for it in items:
        unit = (_num(getattr(it, "unit_price", None)) or _num(getattr(it, "単価", None)))
        qty  = (_num(getattr(it, "qty", None))         or _num(getattr(it, "数量", None), 1))
        rate = (getattr(it, "tax_rate", None) if hasattr(it, "tax_rate") else getattr(it, "税率", None))
        rate = float(rate or 0)
        sub += unit * qty
        tax += int(unit * rate) * qty
    tot = int(sub + tax)
    
    h = s.get(TOrder, order_id)
    if h:
        for attr in ("subtotal", "小計"):
            if hasattr(TOrder, attr):
                setattr(h, attr, int(sub))
        for attr in ("tax", "税額"):
            if hasattr(TOrder, attr):
                setattr(h, attr, int(tax))
        for attr in ("total", "合計"):
            if hasattr(TOrder, attr):
                setattr(h, attr, tot)
        s.flush()
    
    return {"小計": int(sub), "税額": int(tax), "合計": tot}


# ---------------------------------------------------------------------
# テーブル移動取消のメイン関数
# ---------------------------------------------------------------------
@app.route("/admin/table/move/cancel", methods=["POST"])
@require_staff
def admin_table_move_cancel():
    """
    テーブル移動を取り消す
    """
    s = SessionLocal()
    
    try:
        sid = current_store_id()
        if sid is None:
            return jsonify({"ok": False, "error": "店舗が選択されていません"}), 400
        
        data = request.get_json(silent=True) or request.form
        history_id = int(data.get("history_id") or 0)
        
        if not history_id:
            return jsonify({"ok": False, "error": "履歴IDが指定されていません"}), 400
        
        THistory = globals().get("T_テーブル移動履歴")
        if THistory is None:
            return jsonify({"ok": False, "error": "履歴テーブルが存在しません"}), 500
        
        # 履歴を取得
        history = s.get(THistory, history_id)
        if not history:
            return jsonify({"ok": False, "error": "履歴が見つかりません"}), 404
        
        # 店舗IDチェック
        if hasattr(history, "store_id") and getattr(history, "store_id", None) != sid:
            return jsonify({"ok": False, "error": "他店舗の履歴は取り消せません"}), 403
        
        # 取消可能条件をチェック
        can_cancel, reasons, error_code = _check_cancel_conditions(s, sid, history)
        
        if not can_cancel:
            return jsonify({
                "ok": False,
                "error": reasons[0] if reasons else "取り消せません",
                "error_code": error_code,
                "reasons": reasons
            }), 400
        
        # モードに応じて復元処理を実行
        mode = getattr(history, "mode", None)
        
        if mode == "move" or mode == "deny":
            restore_result = _restore_move(s, sid, history)
        elif mode == "merge":
            restore_result = _restore_merge(s, sid, history)
        elif mode == "merge_new":
            restore_result = _restore_merge_new(s, sid, history)
        elif mode == "swap":
            restore_result = _restore_swap(s, sid, history)
        else:
            return jsonify({"ok": False, "error": f"未対応のモード: {mode}"}), 400
        
        # 履歴に取消情報を記録
        history.is_cancelled = 1
        history.cancelled_at = datetime.now(timezone.utc)
        history.cancelled_by_staff_id = session.get("user_id")
        history.cancelled_by_staff_name = session.get("username")
        
        s.commit()
        mark_floor_changed()
        
        current_app.logger.info("[table_move_cancel] history_id=%s mode=%s restored=%s",
                               history_id, mode, restore_result)
        
        return jsonify({
            "ok": True,
            "history_id": history_id,
            "mode": mode,
            "restored": restore_result
        })
    
    except Exception as e:
        s.rollback()
        current_app.logger.exception("[table_move_cancel] failed")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        s.close()


# ---------------------------------------------------------------------
# テーブル移動取消可能チェックAPI
# ---------------------------------------------------------------------
@app.route("/admin/table/move/cancel_check/<int:history_id>", methods=["GET"])
@require_staff
def admin_table_move_cancel_check(history_id):
    """
    テーブル移動が取り消し可能かチェック
    """
    s = SessionLocal()
    
    try:
        sid = current_store_id()
        if sid is None:
            return jsonify({"ok": False, "error": "店舗が選択されていません"}), 400
        
        THistory = globals().get("T_テーブル移動履歴")
        if THistory is None:
            return jsonify({"ok": False, "error": "履歴テーブルが存在しません"}), 500
        
        history = s.get(THistory, history_id)
        if not history:
            return jsonify({"ok": False, "error": "履歴が見つかりません"}), 404
        
        # 店舗IDチェック
        if hasattr(history, "store_id") and getattr(history, "store_id", None) != sid:
            return jsonify({"ok": False, "error": "他店舗の履歴です"}), 403
        
        # 取消可能条件をチェック
        can_cancel, reasons, error_code = _check_cancel_conditions(s, sid, history)
        
        return jsonify({
            "ok": True,
            "can_cancel": can_cancel,
            "reasons": reasons,
            "error_code": error_code
        })
    
    except Exception as e:
        current_app.logger.exception("[table_move_cancel_check] failed")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        s.close()




# ---------------------------------------------------------------------
# 起動（※ すべてのルート定義の"後ろ"に置く）
# ---------------------------------------------------------------------
# --- デバッグ：メニュー一覧（__menu_test） ---------------------------------------
@app.get("/__menu_test")
def __menu_test():
    s = SessionLocal()
    try:
        from sqlalchemy import or_, func
        sid = session.get("store_id")
        tid = session.get("tenant_id")
        q = s.query(Menu)
        if sid is not None and hasattr(Menu, "store_id"):
            q = q.filter(or_(Menu.store_id == sid, Menu.store_id.is_(None)))
        if tid is not None and hasattr(Menu, "tenant_id"):
            q = q.filter(Menu.tenant_id == tid)
        menus = q.order_by(Menu.id.desc()).all()
        app.logger.info("[__menu_test] sid=%s tid=%s -> %s menus", sid, tid, len(menus))
        return render_template("menu_page.html", menus=menus)
    except Exception as e:
        app.logger.exception("__menu_test failed")
        return jsonify({"error": str(e)}), 500
    finally:
        s.close()


# --- 診断：メニュー件数（__menu_diag） --------------------------------------------
@app.get("/__menu_diag")
def __menu_diag():
    s = SessionLocal()
    try:
        from sqlalchemy import or_, func
        sid = session.get("store_id")
        tid = session.get("tenant_id")
        total = s.query(func.count(Menu.id)).scalar()
        by_store = s.query(func.count(Menu.id)).filter(Menu.store_id == sid).scalar() if sid is not None else None
        by_store_or_null = s.query(func.count(Menu.id)).filter(or_(Menu.store_id == sid, Menu.store_id.is_(None))).scalar() if sid is not None else None
        by_tenant = None
        if hasattr(Menu, "tenant_id") and tid is not None:
            by_tenant = s.query(func.count(Menu.id)).filter(Menu.tenant_id == tid).scalar()
        return jsonify({"sid": sid, "tid": tid, "counts": {"total": total, "by_store": by_store, "by_store_or_null": by_store_or_null, "by_tenant": by_tenant}})
    except Exception as e:
        app.logger.exception("__menu_diag failed")
        return jsonify({"error": str(e)}), 500
    finally:
        s.close()


# --- メイン起動ブロック（__main__） -----------------------------------------------
# ---------------------------------------------------------------------
# 明細印刷（会計前）
# ---------------------------------------------------------------------
@app.route("/bill/<int:order_id>")
def bill_print(order_id):
    """明細印刷画面（会計前にお客様に値段を伝える用）"""
    sid = current_store_id()
    if sid is None:
        return redirect(url_for("admin_login"))
    
    s = SessionLocal()
    try:
        # 注文情報を取得
        order = s.query(OrderHeader).options(
            joinedload(OrderHeader.items).joinedload(OrderItem.menu),
            joinedload(OrderHeader.table)
        ).filter(
            OrderHeader.id == order_id,
            OrderHeader.store_id == sid
        ).first()
        
        if not order:
            abort(404)
        
        # 店舗情報を取得
        store = s.get(Store, sid)
        if not store:
            abort(404)
        
        # テーブル情報を取得
        table = order.table
        
        # 合計金額を再計算（キャンセルを反映）
        import math
        subtotal_excl = 0
        tax_total = 0
        total_incl = 0
        
        CANCEL_WORDS = ("取消", "ｷｬﾝｾﾙ", "キャンセル", "cancel", "void")
        
        for item in order.items:
            qty = int(item.qty or 0)
            if qty == 0:
                continue
            
            # 「正数量かつ取消ラベル」は合計から除外
            st = str(item.status or "").lower()
            if qty > 0 and any(w in st for w in CANCEL_WORDS):
                continue
            
            unit_excl = int(item.unit_price or 0)
            rate = float(item.tax_rate or 0.10)
            
            # 時価商品の場合、実際価格を使用
            menu = item.menu
            is_market_price = getattr(menu, "is_market_price", 0) if menu else 0
            actual_price = getattr(item, "actual_price", None)
            
            if is_market_price and actual_price is not None:
                unit_excl = int(actual_price)
            
            unit_tax = math.floor(unit_excl * rate)
            unit_incl = unit_excl + unit_tax
            
            subtotal_excl += unit_excl * qty
            tax_total += unit_tax * qty
            total_incl += unit_incl * qty
        
        return render_template(
            "bill_print.html",
            store=store,
            order=order,
            table=table,
            subtotal=int(subtotal_excl),
            tax=int(tax_total),
            total=int(total_incl)
        )
    finally:
        s.close()


# ---------------------------------------------------------------------
# レシート印刷
# ---------------------------------------------------------------------
@app.route("/receipt/<int:order_id>")
def receipt_print(order_id):
    """レシート印刷画面"""
    sid = current_store_id()
    if sid is None:
        return redirect(url_for("admin_login"))
    
    s = SessionLocal()
    try:
        # 注文情報を取得
        order = s.query(OrderHeader).options(
            joinedload(OrderHeader.items).joinedload(OrderItem.menu),
            joinedload(OrderHeader.table)
        ).filter(
            OrderHeader.id == order_id,
            OrderHeader.store_id == sid
        ).first()
        
        if not order:
            abort(404)
        
        # 店舗情報を取得
        store = s.get(Store, sid)
        if not store:
            abort(404)
        
        # テーブル情報を取得
        table = order.table
        
        # 合計金額を再計算（キャンセルを反映）
        import math
        subtotal_excl = 0
        tax_total = 0
        total_incl = 0
        
        CANCEL_WORDS = ("取消", "ｷｬﾝｾﾙ", "キャンセル", "cancel", "void")
        
        for item in order.items:
            qty = int(item.qty or 0)
            if qty == 0:
                continue
            
            # 「正数量かつ取消ラベル」は合計から除外
            st = str(item.status or "").lower()
            if qty > 0 and any(w in st for w in CANCEL_WORDS):
                continue
            
            unit_excl = int(item.unit_price or 0)
            rate = float(item.tax_rate or 0.10)
            
            # 時価商品の場合、実際価格を使用
            menu = item.menu
            is_market_price = getattr(menu, "is_market_price", 0) if menu else 0
            actual_price = getattr(item, "actual_price", None)
            
            if is_market_price and actual_price is not None:
                unit_excl = int(actual_price)
            
            unit_tax = math.floor(unit_excl * rate)
            unit_incl = unit_excl + unit_tax
            
            subtotal_excl += unit_excl * qty
            tax_total += unit_tax * qty
            total_incl += unit_incl * qty
        
        return render_template(
            "receipt_print.html",
            store=store,
            order=order,
            table=table,
            subtotal=int(subtotal_excl),
            tax=int(tax_total),
            total=int(total_incl)
        )
    finally:
        s.close()


# ---------------------------------------------------------------------
# 領収書印刷
# ---------------------------------------------------------------------
@app.route("/invoice/<int:order_id>")
def invoice_print(order_id):
    """領収書印刷画面"""
    sid = current_store_id()
    if sid is None:
        return redirect(url_for("admin_login"))
    
    s = SessionLocal()
    try:
        # 注文情報を取得
        order = s.query(OrderHeader).options(
            joinedload(OrderHeader.items).joinedload(OrderItem.menu),
            joinedload(OrderHeader.table)
        ).filter(
            OrderHeader.id == order_id,
            OrderHeader.store_id == sid
        ).first()
        
        if not order:
            abort(404)
        
        # 店舗情報を取得
        store = s.get(Store, sid)
        if not store:
            abort(404)
        
        # 領収書番号を生成（店舗ID + 注文IDを組み合わせ）
        invoice_number = f"{sid:04d}-{order_id:06d}"
        
        # 発行日（今日の日付）
        from datetime import datetime
        issue_date = datetime.now().strftime("%Y年%m月%d日")
        
        # 宛名（URLパラメータから取得）
        recipient = request.args.get("recipient", "")
        
        # 合計金額を再計算（キャンセルを反映）
        import math
        subtotal_excl = 0
        tax_total = 0
        total_incl = 0
        
        CANCEL_WORDS = ("取消", "ｷｬﾝｾﾙ", "キャンセル", "cancel", "void")
        
        for item in order.items:
            qty = int(item.qty or 0)
            if qty == 0:
                continue
            
            # 「正数量かつ取消ラベル」は合計から除外
            st = str(item.status or "").lower()
            if qty > 0 and any(w in st for w in CANCEL_WORDS):
                continue
            
            unit_excl = int(item.unit_price or 0)
            rate = float(item.tax_rate or 0.10)
            
            # 時価商品の場合、実際価格を使用
            menu = item.menu
            is_market_price = getattr(menu, "is_market_price", 0) if menu else 0
            actual_price = getattr(item, "actual_price", None)
            
            if is_market_price and actual_price is not None:
                unit_excl = int(actual_price)
            
            unit_tax = math.floor(unit_excl * rate)
            unit_incl = unit_excl + unit_tax
            
            subtotal_excl += unit_excl * qty
            tax_total += unit_tax * qty
            total_incl += unit_incl * qty
        
        return render_template(
            "invoice_print.html",
            store=store,
            order=order,
            invoice_number=invoice_number,
            issue_date=issue_date,
            recipient=recipient,
            subtotal=int(subtotal_excl),
            tax=int(tax_total),
            total=int(total_incl)
        )
    finally:
        s.close()


if __name__ == "__main__":
    # 既存のDBに不足しているカラムを追加する処理
    print("Migrating schema to add new columns...")

    # --- マイグレーション：M_メニューに税込価格列を追加（migrate_menu_price_incl） ---
    def migrate_menu_price_incl():
        eng = _shared_engine_or_none()
        if eng is None:
            return
        with eng.begin() as conn:
            # テーブルが存在するか確認
            try:
                conn.execute(text('SELECT 1 FROM "M_メニュー" LIMIT 1'))
            except Exception:
                return  # テーブルがなければ何もしない

            # '税込価格' 列が存在するか確認
            cols = [r[1] for r in conn.execute(text("PRAGMA table_info('M_メニュー')")).fetchall()]
            if "税込価格" not in cols:
                conn.execute(text('ALTER TABLE "M_メニュー" ADD COLUMN "税込価格" INTEGER'))
                print("Added column '税込価格' to M_メニュー table.")

    migrate_menu_price_incl()

    # --- マイグレーション：時価機能用のカラムを追加 ---
    def migrate_market_price():
        eng = _shared_engine_or_none()
        if eng is None:
            return
        with eng.begin() as conn:
            # PostgreSQL用のマイグレーション
            try:
                # m_メニューテーブルに「時価」カラムを追加
                conn.execute(text('ALTER TABLE "m_メニュー" ADD COLUMN IF NOT EXISTS "時価" INTEGER NOT NULL DEFAULT 0'))
                print("Added column '時価' to m_メニュー table.")
            except Exception as e:
                print(f"[MIGRATE] Failed to add '時価' column: {e}")
            
            try:
                # t_注文明細テーブルに「実際価格」カラムを追加
                conn.execute(text('ALTER TABLE "t_注文明細" ADD COLUMN IF NOT EXISTS "実際価格" INTEGER'))
                print("Added column '実際価格' to t_注文明細 table.")
            except Exception as e:
                print(f"[MIGRATE] Failed to add '実際価格' column: {e}")
    
    migrate_market_price()
    
    # ★ 時価機能用のマイグレーションを実行
    try:
        run_migrations()
    except Exception as e:
        print(f"[MIGRATION] Migration failed: {e}")

    # ★★ 新規追加：進捗テーブルのマイグレーション＆初期シード
    try:
        migrate_progress_table()
    except Exception as e:
        # 失敗しても起動は続ける（ログは出す）
        print("[MIGRATE] progress table migration failed:", e)

    # 必要テーブル検証 / 自動作成
    verify_schema_or_create()
    # 既存DBの軽量マイグレーション
    migrate_schema_if_needed()
    # （任意）自動列追加を許可したい場合のみ
    # ensure_tenant_columns()

    # ★ ここを修正：LANから到達可能にする
    import os
    host = "0.0.0.0"                           # ← 重要：外部端末（スマホ）からアクセス可
    port = int(os.getenv("PORT", "5000"))      # 任意のポートにしたい場合は環境変数で上書き
    app.run(host=host, port=port, debug=True, threaded=True)