@echo off
chcp 65001 >nul
echo ========================================
echo   GON POS System - 印刷サーバー
echo ========================================
echo.
echo プリンター: EPSON TM-T90 ReceiptJ4
echo ポート: 3001
echo.
echo サーバーを停止するには Ctrl+C を押してください。
echo.
echo ========================================
echo.

call npm start
