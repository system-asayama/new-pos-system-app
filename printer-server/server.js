const express = require('express');
const cors = require('cors');
const { exec } = require('child_process');
const os = require('os');
const iconv = require('iconv-lite');

const app = express();
const PORT = 3001;

// プリンター名の設定（環境変数または固定値）
const PRINTER_NAME = process.env.PRINTER_NAME || 'EPSON TM-T90 ReceiptJ4';

// ミドルウェア
app.use(cors());
app.use(express.json());

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

// テキストファイルを作成して印刷（Windows、Shift-JIS）
function printText(text) {
  return new Promise((resolve, reject) => {
    if (os.platform() !== 'win32') {
      reject(new Error('Windowsのみサポートしています'));
      return;
    }
    
    const fs = require('fs');
    const path = require('path');
    const tempFile = path.join(os.tmpdir(), `print_${Date.now()}.txt`);
    
    // Shift-JIS（CP932）でテキストファイルを作成
    const shiftJisBuffer = iconv.encode(text, 'shift_jis');
    fs.writeFileSync(tempFile, shiftJisBuffer);
    
    // PowerShellで印刷（デフォルトエンコーディングで読み込み）
    const escapedFile = tempFile.replace(/\\/g, '\\\\');
    const escapedPrinter = PRINTER_NAME.replace(/'/g, "''");
    const command = `powershell -Command "Get-Content '${escapedFile}' -Encoding Default | Out-Printer -Name '${escapedPrinter}'"`;
    
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
      
      resolve();
    });
  });
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
    
    // 商品リスト
    items.forEach(item => {
      text += `【${item.name}】\r\n`;
      text += `  数量: ${item.quantity}\r\n`;
      
      if (item.memo) {
        text += `  メモ: ${item.memo}\r\n`;
      }
      text += '\r\n';
    });
    
    text += '--------------------------------\r\n';
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
});
