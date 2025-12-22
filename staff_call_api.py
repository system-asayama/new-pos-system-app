# =========================================================
# 店員呼び出しAPI
# =========================================================
# このコードをapp.pyの適切な場所（15112行目付近）に挿入してください

# --- [API] 店員呼び出し通知（パブリック：QR側） ---
@app.route("/api/staff_call", methods=["POST"])
def api_staff_call():
    """
    お客様が店員を呼び出すAPI
    POST JSON:
      { "token": "<qr token>", "table_no": "<テーブル番号>" }
    
    レスポンス:
      { "ok": true }
    """
    try:
        data = request.get_json(force=True) or {}
        token = data.get("token", "")
        table_no = data.get("table_no", "不明")
        
        # トークン検証（既存のQRトークン検証ロジックを使用）
        s = SessionLocal()
        try:
            # QRトークンからテーブル情報を取得
            qr = s.query(QRToken).filter(QRToken.token == token).first()
            if qr and qr.table_id:
                table = s.get(TableSeat, qr.table_id)
                if table:
                    table_no = getattr(table, "テーブル番号", table_no)
        except Exception as e:
            app.logger.warning(f"[api_staff_call] token validation warning: {e}")
        finally:
            s.close()
        
        # ログに記録
        app.logger.info(f"[STAFF_CALL] テーブル {table_no} から店員呼び出し")
        
        return jsonify({"ok": True, "table_no": table_no})
    
    except Exception as e:
        app.logger.error(f"[api_staff_call] error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500
