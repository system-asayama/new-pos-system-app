@echo off
chcp 65001 >nul
echo ========================================
echo   印刷サーバー 自動更新スクリプト
echo ========================================
echo.
echo 最新版をダウンロードしています...
echo.

REM 一時ディレクトリを作成
set TEMP_DIR=%TEMP%\gon-pos-update
if exist "%TEMP_DIR%" rmdir /s /q "%TEMP_DIR%"
mkdir "%TEMP_DIR%"

REM GitHubから最新版をダウンロード
powershell -Command "Invoke-WebRequest -Uri 'https://github.com/system-asayama/gon-pos-system/archive/refs/heads/main.zip' -OutFile '%TEMP_DIR%\latest.zip'"

if not exist "%TEMP_DIR%\latest.zip" (
    echo.
    echo [エラー] ダウンロードに失敗しました。
    echo インターネット接続を確認してください。
    echo.
    pause
    exit /b 1
)

echo ダウンロード完了！
echo.
echo ファイルを展開しています...

REM ZIPファイルを展開
powershell -Command "Expand-Archive -Path '%TEMP_DIR%\latest.zip' -DestinationPath '%TEMP_DIR%' -Force"

REM 現在のディレクトリのファイルを更新（node_modulesとpackage-lock.jsonは保持）
echo.
echo ファイルを更新しています...

REM server.jsとpackage.jsonを更新
copy /y "%TEMP_DIR%\gon-pos-system-main\printer-server\server.js" "%~dp0server.js" >nul
copy /y "%TEMP_DIR%\gon-pos-system-main\printer-server\package.json" "%~dp0package.json" >nul

REM 一時ファイルを削除
rmdir /s /q "%TEMP_DIR%"

echo.
echo 依存パッケージをインストールしています...
echo.

call npm install

if errorlevel 1 (
    echo.
    echo [エラー] パッケージのインストールに失敗しました。
    echo.
    pause
    exit /b 1
)

echo.
echo ========================================
echo   更新が完了しました！
echo ========================================
echo.
echo start.bat をダブルクリックして
echo 印刷サーバーを起動してください。
echo.
pause
