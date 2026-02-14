#!/bin/bash

# Electron版printer-serverのビルドスクリプト

echo "==================================="
echo "プリンターサーバー ビルドスクリプト"
echo "==================================="

# 依存パッケージのインストール
echo ""
echo "[1/3] 依存パッケージをインストール中..."
npm install

# ビルド
echo ""
echo "[2/3] Windows用EXEをビルド中..."
npm run build:win

# 完了
echo ""
echo "[3/3] ビルド完了！"
echo ""
echo "生成されたファイル:"
ls -lh dist/*.exe 2>/dev/null || echo "  ※ EXEファイルが見つかりません"

echo ""
echo "==================================="
echo "ビルド完了"
echo "==================================="
