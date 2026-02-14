# GON POS System - 印刷サーバー

EPSON TM-T90IIレシートプリンタをUSB接続で使用するためのローカル印刷サーバーです。

## 必要なもの

- Windows PC（店舗に設置）
- EPSON TM-T90II レシートプリンタ
- USBケーブル（Type-B）
- Node.js（バージョン 14以上）

## セットアップ手順

### 1. Node.jsのインストール

1. [Node.js公式サイト](https://nodejs.org/)にアクセス
2. **LTS版（推奨版）**をダウンロード
3. インストーラーを実行してインストール
4. コマンドプロンプトを開いて以下を実行して確認：
   ```
   node --version
   npm --version
   ```

### 2. プリンタードライバーのインストール

1. EPSON TM-T90II Software & Documents Discをパソコンに挿入
2. **EPSON Advanced Printer Driver**をインストール
3. プリンタをUSB接続
4. Windowsの「設定」→「デバイス」→「プリンターとスキャナー」で確認

### 3. 印刷サーバーのセットアップ

**簡単セットアップ（推奨）**

1. このフォルダ（`printer-server`）をデスクトップにコピー
2. `update.bat` をダブルクリック
   - 依存パッケージが自動でインストールされます

**手動セットアップ**

1. このフォルダ（`printer-server`）をWindowsパソコンにコピー
2. コマンドプロンプトを開いて、フォルダに移動：
   ```
   cd C:\path\to\printer-server
   ```
3. 依存パッケージをインストール：
   ```
   npm install
   ```

### 4. 印刷サーバーの起動

**簡単起動（推奨）**

`start.bat` をダブルクリック

**手動起動**

```
npm start
```

以下のメッセージが表示されれば成功です：
```
印刷サーバーがポート 3001 で起動しました
✓ プリンターが接続されています
```

### 5. 動作確認

ブラウザで以下のURLを開いて、プリンター接続を確認：
```
http://localhost:3001/health
```

以下のレスポンスが返れば成功：
```json
{"status":"ok","message":"プリンターが接続されています"}
```

## 更新方法

コードが更新された場合：

1. `update.bat` をダブルクリック
   - 最新版がGitHubから自動でダウンロードされます
2. `start.bat` をダブルクリック
   - 印刷サーバーを起動します

## 使用方法

### 自動印刷モード（推奨）

**ブラウザを開かなくても、新規注文が入ると自動的に印刷されます。**

#### 設定手順

1. `config.json.sample` を `config.json` にコピー
2. `config.json` を編集：
   ```json
   {
     "printerName": "EPSON TM-T90 ReceiptJ4",
     "autoPolling": {
       "enabled": true,
       "herokuUrl": "https://your-app.herokuapp.com",
       "storeId": 1,
       "apiKey": "your-secret-key-here",
       "interval": 10000
     }
   }
   ```
3. 各項目を設定：
   - `printerName`: Windowsに登録されているプリンター名
   - `enabled`: `true` で自動印刷を有効化
   - `herokuUrl`: POSシステムのURL（例: https://gon-pos-system.herokuapp.com）
   - `storeId`: 店舗ID（管理画面で確認）
   - `apiKey`: APIキー（Herokuの環境変数 `PRINTER_SERVER_API_KEY` と同じ値）
   - `interval`: チェック間隔（ミリ秒、10000 = 10秒）

4. Herokuの環境変数を設定：
   ```bash
   heroku config:set PRINTER_SERVER_API_KEY=your-secret-key-here
   ```

5. `start.bat` をダブルクリックして起動

起動後、以下のメッセージが表示されれば成功：
```
[POLLING] 自動ポーリングを開始します（間隔: 10000ms）
```

新規注文が入ると、自動的に印刷されます。

### 手動印刷モード

POSシステムの管理画面で「印刷」ボタンをクリックすると印刷されます。

#### レシート印刷

会計画面で「レシート印刷」ボタンをクリック

#### 注文伝票印刷

注文確定画面で「伝票印刷」ボタンをクリック

## トラブルシューティング

### プリンターが見つからない

1. USBケーブルが正しく接続されているか確認
2. プリンターの電源が入っているか確認
3. Windowsの「デバイスマネージャー」でプリンターが認識されているか確認
4. プリンタードライバーが正しくインストールされているか確認

### 印刷サーバーが起動しない

1. Node.jsが正しくインストールされているか確認
2. `npm install`が正常に完了したか確認
3. ポート3001が他のプログラムで使用されていないか確認

### 印刷が実行されない

1. ヘルスチェックURL（http://localhost:3001/health）でプリンター接続を確認
2. プリンターのエラーLEDが点灯していないか確認
3. ロール紙が正しくセットされているか確認

## 自動起動設定（オプション）

Windowsの起動時に印刷サーバーを自動起動するには：

1. `startup.bat`ファイルを作成：
   ```batch
   @echo off
   cd C:\path\to\printer-server
   npm start
   ```
2. スタートアップフォルダに配置：
   - `Win + R`を押して`shell:startup`を実行
   - `startup.bat`をコピー

## API仕様

### ヘルスチェック

```
GET http://localhost:3001/health
```

### サーバー情報（自動検出用）

```
GET http://localhost:3001/info
```

レスポンス例：
```json
{
  "type": "printer_server",
  "name": "Node.js Print Server",
  "version": "2.0.0",
  "platform": "win32",
  "printer_name": "EPSON TM-T90 ReceiptJ4",
  "printer_connected": true,
  "ip_addresses": ["192.168.1.66"],
  "port": 3001,
  "auto_polling": {
    "enabled": true,
    "interval": 10000,
    "last_processed_order_id": 123
  }
}
```

### 設定の取得

```
GET http://localhost:3001/config
```

### 設定の更新

```
POST http://localhost:3001/config
Content-Type: application/json

{
  "printerName": "EPSON TM-T90 ReceiptJ4",
  "autoPolling": {
    "enabled": true,
    "herokuUrl": "https://your-app.herokuapp.com",
    "storeId": 1,
    "apiKey": "your-secret-key-here",
    "interval": 10000
  }
}
```

### レシート印刷

```
POST http://localhost:3001/print/receipt
Content-Type: application/json

{
  "orderNumber": "525",
  "tableName": "0101",
  "items": [
    {"name": "ホルモン", "quantity": 1, "price": 700}
  ],
  "subtotal": 700,
  "tax": 70,
  "total": 770,
  "timestamp": "2025-12-25 15:48"
}
```

### 注文伝票印刷

```
POST http://localhost:3001/print/order
Content-Type: application/json

{
  "orderNumber": "525",
  "tableName": "0101",
  "items": [
    {"name": "ホルモン", "quantity": 1, "memo": "よく焼いて"}
  ],
  "timestamp": "2025-12-25 15:48"
}
```

## サポート

問題が発生した場合は、以下の情報を添えてお問い合わせください：

- Windowsのバージョン
- Node.jsのバージョン
- エラーメッセージ
- プリンターのモデル番号
