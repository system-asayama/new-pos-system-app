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

## 使用方法

### レシート印刷

POSシステムの会計画面で「レシート印刷」ボタンをクリックすると、自動的に印刷されます。

### 注文伝票印刷

POSシステムの注文確定画面で「伝票印刷」ボタンをクリックすると、キッチン用の注文伝票が印刷されます。

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
