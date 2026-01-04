const express = require('express');
const cors = require('cors');
const printer = require('printer');

const app = express();
const PORT = 3001;

// プリンター名の設定（環境変数または固定値）
const PRINTER_NAME = process.env.PRINTER_NAME || 'EPSON TM-T90 ReceiptJ4';

// ミドルウェア
app.use(cors());
app.use(express.json());

// プリンター接続確認
function checkPrinter() {
  try {
    const printers = printer.getPrinters();
    const targetPrinter = printers.find(p => p.name === PRINTER_NAME);
    return targetPrinter ? true : false;
  } catch (error) {
    console.error('プリンター確認エラー:', error.message);
    return false;
  }
}

// ESC/POSコマンドを生成
function createESCPOSCommand(text) {
  const ESC = '\x1B';
  const GS = '\x1D';
  
  let command = '';
  
  // 初期化
  command += ESC + '@';
  
  // テキスト追加
  command += text;
  
  // カット
  command += GS + 'V' + '\x41' + '\x03';
  
  return Buffer.from(command, 'binary');
}

// ヘルスチェック
app.get('/health', (req, res) => {
  if (checkPrinter()) {
    res.json({ status: 'ok', message: 'プリンターが接続されています' });
  } else {
    res.status(500).json({ status: 'error', message: 'プリンターが見つかりません' });
  }
});

// レシート印刷
app.post('/print/receipt', async (req, res) => {
  const { orderNumber, tableName, items, subtotal, tax, total, timestamp } = req.body;

  if (!checkPrinter()) {
    return res.status(500).json({ error: 'プリンターが見つかりません' });
  }

  try {
    let text = '';
    
    // ヘッダー
    text += '        GON POS System\n';
    text += '       お会計レシート\n';
    text += '--------------------------------\n';
    text += `注文番号: ${orderNumber}\n`;
    text += `テーブル: ${tableName}\n`;
    text += `日時: ${timestamp}\n`;
    text += '--------------------------------\n';
    text += '商品明細\n';
    text += '--------------------------------\n';
    
    // 商品リスト
    items.forEach(item => {
      const name = item.name.padEnd(20, ' ');
      const qty = `x${item.quantity}`.padStart(4, ' ');
      const price = `¥${item.price.toLocaleString()}`.padStart(8, ' ');
      text += `${name}${qty}${price}\n`;
    });
    
    // フッター
    text += '--------------------------------\n';
    text += `              小計: ¥${subtotal.toLocaleString()}\n`;
    text += `            消費税: ¥${tax.toLocaleString()}\n`;
    text += `              合計: ¥${total.toLocaleString()}\n`;
    text += '--------------------------------\n';
    text += '      ありがとうございました\n';
    text += '\n\n\n';

    // 印刷実行
    printer.printDirect({
      data: text,
      printer: PRINTER_NAME,
      type: 'RAW',
      success: function(jobID) {
        console.log(`印刷ジョブ送信成功: ${jobID}`);
        res.json({ success: true, message: 'レシートを印刷しました' });
      },
      error: function(err) {
        console.error('印刷エラー:', err);
        res.status(500).json({ error: '印刷に失敗しました', details: err });
      }
    });
  } catch (error) {
    console.error('印刷エラー:', error);
    res.status(500).json({ error: '印刷に失敗しました', details: error.message });
  }
});

// 注文伝票印刷（キッチン用）
app.post('/print/order', async (req, res) => {
  const { orderNumber, tableName, items, timestamp } = req.body;

  if (!checkPrinter()) {
    return res.status(500).json({ error: 'プリンターが見つかりません' });
  }

  try {
    let text = '';
    
    // ヘッダー（大きめ）
    text += '\n';
    text += '        *** 注文伝票 ***\n';
    text += '--------------------------------\n';
    text += `注文番号: ${orderNumber}\n`;
    text += `テーブル: ${tableName}\n`;
    text += `時刻: ${timestamp}\n`;
    text += '--------------------------------\n\n';
    
    // 商品リスト
    items.forEach(item => {
      text += `【${item.name}】\n`;
      text += `  数量: ${item.quantity}\n`;
      
      if (item.memo) {
        text += `  メモ: ${item.memo}\n`;
      }
      text += '\n';
    });
    
    text += '--------------------------------\n';
    text += '\n\n\n';

    // 印刷実行
    printer.printDirect({
      data: text,
      printer: PRINTER_NAME,
      type: 'RAW',
      success: function(jobID) {
        console.log(`印刷ジョブ送信成功: ${jobID}`);
        res.json({ success: true, message: '注文伝票を印刷しました' });
      },
      error: function(err) {
        console.error('印刷エラー:', err);
        res.status(500).json({ error: '印刷に失敗しました', details: err });
      }
    });
  } catch (error) {
    console.error('印刷エラー:', error);
    res.status(500).json({ error: '印刷に失敗しました', details: error.message });
  }
});

// サーバー起動
app.listen(PORT, () => {
  console.log(`印刷サーバーがポート ${PORT} で起動しました`);
  console.log(`ヘルスチェック: http://localhost:${PORT}/health`);
  console.log(`プリンター名: ${PRINTER_NAME}`);
  
  // プリンター接続確認
  if (checkPrinter()) {
    console.log('✓ プリンターが接続されています');
  } else {
    console.log('✗ プリンターが見つかりません。プリンター名を確認してください。');
    console.log('利用可能なプリンター:');
    try {
      const printers = printer.getPrinters();
      printers.forEach(p => console.log(`  - ${p.name}`));
    } catch (error) {
      console.error('プリンター一覧取得エラー:', error.message);
    }
  }
});
