@echo off
echo ========================================
echo GON POS System - 印刷サーバー インストール
echo ========================================
echo.
echo Node.jsがインストールされているか確認しています...
node --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo エラー: Node.jsがインストールされていません。
    echo.
    echo 以下のURLからNode.jsをダウンロードしてインストールしてください:
    echo https://nodejs.org/
    echo.
    pause
    exit /b 1
)
echo.
echo Node.jsが見つかりました。
node --version
echo.
echo 依存パッケージをインストールしています...
echo.
npm install
echo.
echo ========================================
echo インストールが完了しました！
echo ========================================
echo.
echo 次のステップ:
echo 1. プリンターをUSB接続してください
echo 2. start.bat をダブルクリックして印刷サーバーを起動してください
echo.
pause
