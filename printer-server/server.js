const express = require('express');
const cors = require('cors');
const { exec } = require('child_process');
const os = require('os');
const iconv = require('iconv-lite');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = 3001;

// 設定ファイルの読み込み
const CONFIG_FILE = path.join(__dirname, 'config.json');
let config = {
  printerName: process.env.PRINTER_NAME || 'EPSON TM-T90 ReceiptJ4',
  autoPolling: {
    enabled: false,
    herokuUrl: '',
    storeId: null,
    apiKey: '',
    interval: 10000 // 10秒ごと
  }
};

// 設定ファイルが存在すれば読み込む
if (fs.existsSync(CONFIG_FILE)) {
  try {
    const fileConfig = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
    config = { ...config, ...fileConfig };
    console.log('[CONFIG] 設定ファイルを読み込みました');
  } catch (e) {
    console.error('[CONFIG] 設定ファイルの読み込みに失敗しました:', e);
  }
}

// プリンター名の設定
const PRINTER_NAME = config.printerName;

// ミドルウェア
app.use(cors());
app.use(express.json());

// 最後に処理した注文ID（重複印刷防止）
let lastProcessedOrderId = 0;

// プリンター接続確認（Windows）
function checkPrinter() {
  return new Promise((resolve) => {
    if (os.platform() !== 'win32') {
      resolve(false);
      return;
    }
    
    exec('powershell -Command "Get-Printer | Select-Object -ExpandProperty Name"', (error, stdout) => {
      if (error) {
        resolve(false);
        return;
      }
      const printers = stdout.split('\n').map(p => p.trim()).filter(p => p);
      resolve(printers.includes(PRINTER_NAME));
    });
  });
}

// テキストを印刷（copyコマンドで共有プリンターへ）
function printText(text) {
  return new Promise((resolve, reject) => {
    if (os.platform() !== 'win32') {
      reject(new Error('Windowsのみサポートしています'));
      return;
    }
    
    const tempFile = path.join(os.tmpdir(), `print_${Date.now()}.txt`);
    
    // ESC/POSコマンドを追加
    const ESC = 0x1B;
    const GS = 0x1D;
    
    // 初期化コマンド
    const initCmd = Buffer.from([ESC, 0x40]); // ESC @ - プリンター初期化
    
    // 国際文字セットを日本に設定
    const intlCharsetCmd = Buffer.from([ESC, 0x52, 0x08]); // ESC R 8 - Japan
    
    // 漢字コード体系をShift-JISに設定
    const kanjiCodeCmd = Buffer.from([0x1C, 0x43, 0x01]); // FS C 1 - Shift-JIS
    
    // 漢字モードON
    const kanjiModeCmd = Buffer.from([0x1C, 0x26]); // FS & - 漢字モードON
    
    // テキストをShift-JISでエンコード
    const textBuffer = iconv.encode(text, 'shift_jis');
    
    // 紙送りとカットコマンド
    const feedCmd = Buffer.from([0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A]); // 8行紙送り
    const cutCmd = Buffer.from([GS, 0x56, 0x00]); // GS V 0 - フルカット
    
    // 全てを結合
    const fullBuffer = Buffer.concat([initCmd, intlCharsetCmd, kanjiCodeCmd, kanjiModeCmd, textBuffer, feedCmd, cutCmd]);
    
    // ファイルに書き込み
    fs.writeFileSync(tempFile, fullBuffer);
    
    // copyコマンドで共有プリンターへRaw印刷
    const command = `cmd /c copy /b "${tempFile}" "\\\\%COMPUTERNAME%\\${PRINTER_NAME}"`;
    
    console.log(`[DEBUG] 印刷コマンド: ${command}`);
    
    exec(command, (error, stdout, stderr) => {
      // 一時ファイルを削除
      try {
        fs.unlinkSync(tempFile);
      } catch (e) {
        console.error('一時ファイル削除エラー:', e);
      }
      
      if (error) {
        console.error('印刷エラー:', error);
        console.error('stderr:', stderr);
        reject(error);
        return;
      }
      
      console.log('印刷成功');
      resolve();
    });
  });
}

// 新規注文をポーリング
async function pollNewOrders() {
  if (!config.autoPolling.enabled) {
    return;
  }
  
  const { herokuUrl, storeId, apiKey } = config.autoPolling;
  
  if (!herokuUrl || !storeId || !apiKey) {
    console.error('[POLLING] 設定が不完全です。config.jsonを確認してください。');
    return;
  }
  
  try {
    const url = `${herokuUrl}/api/printer-server/new-orders?store_id=${storeId}&api_key=${encodeURIComponent(apiKey)}&since_id=${lastProcessedOrderId}`;
    const response = await axios.get(url, { timeout: 5000 });
    
    if (!response.data.ok) {
      console.error('[POLLING] APIエラー:', response.data.error);
      return;
    }
    
    const orders = response.data.orders || [];
    
    if (orders.length > 0) {
      console.log(`[POLLING] 新規注文 ${orders.length} 件を検出`);
      
      for (const order of orders) {
        try {
          // 印刷データを取得
          const printDataUrl = `${herokuUrl}/api/print_data/${order.id}`;
          const printDataResponse = await axios.get(printDataUrl, { timeout: 5000 });
          
          if (printDataResponse.data.text) {
            console.log(`[POLLING] 注文 #${order.id} を印刷中...`);
            await printText(printDataResponse.data.text);
            console.log(`[POLLING] 注文 #${order.id} の印刷完了`);
            
            // 最後に処理したIDを更新
            if (order.id > lastProcessedOrderId) {
              lastProcessedOrderId = order.id;
            }
          }
        } catch (printError) {
          console.error(`[POLLING] 注文 #${order.id} の印刷に失敗:`, printError.message);
        }
      }
    }
  } catch (error) {
    if (error.code === 'ECONNREFUSED') {
      console.error('[POLLING] Herokuに接続できません。URLを確認してください。');
    } else if (error.code === 'ETIMEDOUT') {
      console.error('[POLLING] タイムアウト。ネットワーク接続を確認してください。');
    } else {
      console.error('[POLLING] エラー:', error.message);
    }
  }
}

// 自動ポーリングの開始
let pollingInterval = null;
function startAutoPolling() {
  if (config.autoPolling.enabled && !pollingInterval) {
    console.log(`[POLLING] 自動ポーリングを開始します（間隔: ${config.autoPolling.interval}ms）`);
    pollingInterval = setInterval(pollNewOrders, config.autoPolling.interval);
    // 即座に1回実行
    pollNewOrders();
  }
}

function stopAutoPolling() {
  if (pollingInterval) {
    console.log('[POLLING] 自動ポーリングを停止します');
    clearInterval(pollingInterval);
    pollingInterval = null;
  }
}

// ヘルスチェック
app.get('/health', async (req, res) => {
  const isConnected = await checkPrinter();
  if (isConnected) {
    res.json({ status: 'ok', message: 'プリンターが接続されています' });
  } else {
    res.status(500).json({ status: 'error', message: 'プリンターが見つかりません' });
  }
});

// サーバー情報（自動検出用）
app.get('/info', async (req, res) => {
  const isConnected = await checkPrinter();
  const networkInterfaces = os.networkInterfaces();
  const ipAddresses = [];
  
  // ローカルIPアドレスを取得
  for (const name of Object.keys(networkInterfaces)) {
    for (const net of networkInterfaces[name]) {
      // IPv4かつ内部アドレスでないもの
      if (net.family === 'IPv4' && !net.internal) {
        ipAddresses.push(net.address);
      }
    }
  }
  
  res.json({
    type: 'printer_server',
    name: 'Node.js Print Server',
    version: '2.0.0',
    platform: os.platform(),
    printer_name: PRINTER_NAME,
    printer_connected: isConnected,
    ip_addresses: ipAddresses,
    port: PORT,
    auto_polling: {
      enabled: config.autoPolling.enabled,
      interval: config.autoPolling.interval,
      last_processed_order_id: lastProcessedOrderId
    }
  });
});

// 設定の取得
app.get('/config', (req, res) => {
  res.json(config);
});

// 設定の更新
app.post('/config', (req, res) => {
  try {
    const newConfig = req.body;
    config = { ...config, ...newConfig };
    
    // ファイルに保存
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2), 'utf8');
    
    // ポーリングの再起動
    stopAutoPolling();
    if (config.autoPolling.enabled) {
      startAutoPolling();
    }
    
    res.json({ success: true, message: '設定を更新しました', config });
  } catch (error) {
    res.status(500).json({ error: '設定の保存に失敗しました', details: error.message });
  }
});

// レシート印刷
app.post('/print/receipt', async (req, res) => {
  const { orderNumber, tableName, items, subtotal, tax, total, timestamp } = req.body;

  const isConnected = await checkPrinter();
  if (!isConnected) {
    return res.status(500).json({ error: 'プリンターが見つかりません' });
  }

  try {
    let text = '';
    
    // ヘッダー
    text += '        GON POS System\r\n';
    text += '       お会計レシート\r\n';
    text += '--------------------------------\r\n';
    text += `注文番号: ${orderNumber}\r\n`;
    text += `テーブル: ${tableName}\r\n`;
    text += `日時: ${timestamp}\r\n`;
    text += '--------------------------------\r\n';
    text += '商品明細\r\n';
    text += '--------------------------------\r\n';
    
    // 商品リスト
    items.forEach(item => {
      const name = item.name.padEnd(20, ' ');
      const qty = `x${item.quantity}`.padStart(4, ' ');
      const price = `¥${item.price.toLocaleString()}`.padStart(8, ' ');
      text += `${name}${qty}${price}\r\n`;
    });
    
    // フッター
    text += '--------------------------------\r\n';
    text += `              小計: ¥${subtotal.toLocaleString()}\r\n`;
    text += `            消費税: ¥${tax.toLocaleString()}\r\n`;
    text += `              合計: ¥${total.toLocaleString()}\r\n`;
    text += '--------------------------------\r\n';
    text += '      ありがとうございました\r\n';
    text += '\r\n\r\n\r\n';

    // 印刷実行
    await printText(text);
    res.json({ success: true, message: 'レシートを印刷しました' });
  } catch (error) {
    console.error('印刷エラー:', error);
    res.status(500).json({ error: '印刷に失敗しました', details: error.message });
  }
});

// 汎用印刷エンドポイント（ブラウザから直接呼び出し用）
app.post('/print', async (req, res) => {
  const { type, data, text } = req.body;
  console.log('[DEBUG] /print リクエスト受信');
  console.log('[DEBUG] type:', type);
  console.log('[DEBUG] text:', text ? 'あり' : 'なし');
  if (text) {
    console.log('[DEBUG] textの内容（最初の500文字）:');
    console.log(text.substring(0, 500));
  }
  console.log('[DEBUG] data:', JSON.stringify(data, null, 2));

  // textフィールドがある場合は直接印刷
  if (text) {
    const isConnected = await checkPrinter();
    if (!isConnected) {
      return res.status(500).json({ error: 'プリンターが見つかりません' });
    }

    try {
      await printText(text);
      res.json({ success: true, message: '印刷が完了しました' });
    } catch (error) {
      console.error('印刷エラー:', error);
      res.status(500).json({ error: '印刷に失敗しました', details: error.message });
    }
    return;
  }

  // typeに応じて適切な印刷処理を実行
  if (type === 'order') {
    // 注文伝票印刷
    const { orderNumber, tableName, items, timestamp } = data;
    
    const isConnected = await checkPrinter();
    if (!isConnected) {
      return res.status(500).json({ error: 'プリンターが見つかりません' });
    }

    try {
      let text = '';
      
      // ヘッダー（大きめ）
      text += '\r\n';
      text += '        *** 注文伝票 ***\r\n';
      text += '--------------------------------\r\n';
      text += `注文番号: ${orderNumber}\r\n`;
      text += `テーブル: ${tableName}\r\n`;
      text += `時刻: ${timestamp}\r\n`;
      text += '--------------------------------\r\n\r\n';
      
      // 商品リスト（レシートと同じフォーマット）
      text += '商品名                  数量    金額\r\n';
      text += '--------------------------------\r\n';
      
      let totalAmount = 0;
      items.forEach(item => {
        const price = item.price || 0;
        const quantity = item.quantity || 1;
        const subtotal = price * quantity;
        totalAmount += subtotal;
        
        // 1行表示: 商品名(26文字) 数量(4桁) 金額(右寄せ)
        const name = item.name.substring(0, 26).padEnd(26, ' ');
        const qty = String(quantity).padStart(4, ' ');
        const amount = `￥${subtotal.toLocaleString()}`.padStart(11, ' ');
        text += `${name}${qty} ${amount}\r\n`;
        
        // メモがある場合は次の行に表示
        if (item.memo) {
          text += `  ※ ${item.memo}\r\n`;
        }
      });
      
      text += `\r\n\r\n`;
      text += `================================\r\n`;
      text += `\r\n`;
      text += `      ■■ 合計金額 ■■\r\n`;
      text += `\r\n`;
      text += `\r\n`;
      text += `        ￥ ${totalAmount.toLocaleString()}\r\n`;
      text += `\r\n`;
      text += `\r\n`;
      text += `================================\r\n`;
      text += '\r\n\r\n\r\n';

      // 印刷実行
      await printText(text);
      res.json({ success: true, message: '注文伝票を印刷しました' });
    } catch (error) {
      console.error('印刷エラー:', error);
      res.status(500).json({ error: '印刷に失敗しました', details: error.message });
    }
  } else if (type === 'receipt') {
    // レシート印刷
    const { orderNumber, tableName, items, subtotal, tax, total, timestamp } = data;
    
    const isConnected = await checkPrinter();
    if (!isConnected) {
      return res.status(500).json({ error: 'プリンターが見つかりません' });
    }

    try {
      let text = '';
      
      // ヘッダー
      text += '        GON POS System\r\n';
      text += '       お会計レシート\r\n';
      text += '--------------------------------\r\n';
      text += `注文番号: ${orderNumber}\r\n`;
      text += `テーブル: ${tableName}\r\n`;
      text += `日時: ${timestamp}\r\n`;
      text += '--------------------------------\r\n';
      text += '商品明細\r\n';
      text += '--------------------------------\r\n';
      
      // 商品リスト
      items.forEach(item => {
        const name = item.name.padEnd(20, ' ');
        const qty = `x${item.quantity}`.padStart(4, ' ');
        const price = `¥${item.price.toLocaleString()}`.padStart(8, ' ');
        text += `${name}${qty}${price}\r\n`;
      });
      
      // フッター
      text += '--------------------------------\r\n';
      text += `              小計: ¥${subtotal.toLocaleString()}\r\n`;
      text += `            消費税: ¥${tax.toLocaleString()}\r\n`;
      text += `              合計: ¥${total.toLocaleString()}\r\n`;
      text += '--------------------------------\r\n';
      text += '      ありがとうございました\r\n';
      text += '\r\n\r\n\r\n';

      // 印刷実行
      await printText(text);
      res.json({ success: true, message: 'レシートを印刷しました' });
    } catch (error) {
      console.error('印刷エラー:', error);
      res.status(500).json({ error: '印刷に失敗しました', details: error.message });
    }
  } else {
    res.status(400).json({ error: '不明な印刷タイプです', type });
  }
});

// 注文伝票印刷（キッチン用）
app.post('/print/order', async (req, res) => {
  const { orderNumber, tableName, items, timestamp } = req.body;

  const isConnected = await checkPrinter();
  if (!isConnected) {
    return res.status(500).json({ error: 'プリンターが見つかりません' });
  }

  try {
    let text = '';
    
    // ヘッダー（大きめ）
    text += '\r\n';
    text += '        *** 注文伝票 ***\r\n';
    text += '--------------------------------\r\n';
    text += `注文番号: ${orderNumber}\r\n`;
    text += `テーブル: ${tableName}\r\n`;
    text += `時刻: ${timestamp}\r\n`;
    text += '--------------------------------\r\n\r\n';
    
    // 商品リスト（レシートと同じフォーマット）
    text += '商品名                  数量    金額\r\n';
    text += '--------------------------------\r\n';
    
    let totalAmount = 0;
    items.forEach(item => {
      const price = item.price || 0;
      const quantity = item.quantity || 1;
      const subtotal = price * quantity;
      totalAmount += subtotal;
      
      // 1行表示: 商品名(26文字) 数量(4桁) 金額(右寄せ)
      const name = item.name.substring(0, 26).padEnd(26, ' ');
      const qty = String(quantity).padStart(4, ' ');
      const amount = `￥${subtotal.toLocaleString()}`.padStart(11, ' ');
      text += `${name}${qty} ${amount}\r\n`;
      
      // メモがある場合は次の行に表示
      if (item.memo) {
        text += `  ※ ${item.memo}\r\n`;
      }
    });
    
    text += `\r\n\r\n`;
    text += `================================\r\n`;
    text += `\r\n`;
    text += `      ■■ 合計金額 ■■\r\n`;
    text += `\r\n`;
    text += `\r\n`;
    text += `        ￥ ${totalAmount.toLocaleString()}\r\n`;
    text += `\r\n`;
    text += `\r\n`;
    text += `================================\r\n`;
    text += '\r\n\r\n\r\n';

    // 印刷実行
    await printText(text);
    res.json({ success: true, message: '注文伝票を印刷しました' });
  } catch (error) {
    console.error('印刷エラー:', error);
    res.status(500).json({ error: '印刷に失敗しました', details: error.message });
  }
});

// サーバー起動
app.listen(PORT, async () => {
  console.log(`印刷サーバーがポート ${PORT} で起動しました`);
  console.log(`ヘルスチェック: http://localhost:${PORT}/health`);
  console.log(`プリンター名: ${PRINTER_NAME}`);
  console.log(`プラットフォーム: ${os.platform()}`);
  
  // プリンター接続確認
  const isConnected = await checkPrinter();
  if (isConnected) {
    console.log('✓ プリンターが接続されています');
  } else {
    console.log('✗ プリンターが見つかりません。プリンター名を確認してください。');
    
    // 利用可能なプリンター一覧を表示
    if (os.platform() === 'win32') {
      exec('powershell -Command "Get-Printer | Select-Object -ExpandProperty Name"', (error, stdout) => {
        if (!error) {
          console.log('利用可能なプリンター:');
          const printers = stdout.split('\n').map(p => p.trim()).filter(p => p);
          printers.forEach(p => console.log(`  - ${p}`));
        }
      });
    }
  }
  
  // 自動ポーリングの開始
  if (config.autoPolling.enabled) {
    startAutoPolling();
  } else {
    console.log('[INFO] 自動ポーリングは無効です。有効にするには config.json を編集してください。');
  }
});
