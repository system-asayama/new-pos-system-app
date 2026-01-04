const express = require('express');
const cors = require('cors');
const escpos = require('escpos');
escpos.USB = require('escpos-usb');

const app = express();
const PORT = 3001;

// ミドルウェア
app.use(cors());
app.use(express.json());

// プリンターデバイスの取得
function getPrinter() {
  try {
    const device = new escpos.USB();
    const printer = new escpos.Printer(device);
    return { device, printer };
  } catch (error) {
    console.error('プリンターが見つかりません:', error.message);
    return null;
  }
}

// ヘルスチェック
app.get('/health', (req, res) => {
  const printerInfo = getPrinter();
  if (printerInfo) {
    res.json({ status: 'ok', message: 'プリンターが接続されています' });
  } else {
    res.status(500).json({ status: 'error', message: 'プリンターが見つかりません' });
  }
});

// レシート印刷
app.post('/print/receipt', async (req, res) => {
  const { orderNumber, tableName, items, subtotal, tax, total, timestamp } = req.body;

  const printerInfo = getPrinter();
  if (!printerInfo) {
    return res.status(500).json({ error: 'プリンターが見つかりません' });
  }

  const { device, printer } = printerInfo;

  try {
    await new Promise((resolve, reject) => {
      device.open((err) => {
        if (err) {
          reject(err);
          return;
        }

        printer
          .font('a')
          .align('ct')
          .style('bu')
          .size(1, 1)
          .text('GON POS System')
          .text('お会計レシート')
          .text('--------------------------------')
          .align('lt')
          .style('normal')
          .text(`注文番号: ${orderNumber}`)
          .text(`テーブル: ${tableName}`)
          .text(`日時: ${timestamp}`)
          .text('--------------------------------')
          .text('商品明細')
          .text('--------------------------------');

        // 商品リスト
        items.forEach(item => {
          const name = item.name.padEnd(20, ' ');
          const qty = `x${item.quantity}`.padStart(4, ' ');
          const price = `¥${item.price.toLocaleString()}`.padStart(8, ' ');
          printer.text(`${name}${qty}${price}`);
        });

        printer
          .text('--------------------------------')
          .align('rt')
          .text(`小計: ¥${subtotal.toLocaleString()}`)
          .text(`消費税: ¥${tax.toLocaleString()}`)
          .size(1, 1)
          .style('b')
          .text(`合計: ¥${total.toLocaleString()}`)
          .style('normal')
          .size(0, 0)
          .text('--------------------------------')
          .align('ct')
          .text('ありがとうございました')
          .text('')
          .cut()
          .close(() => {
            resolve();
          });
      });
    });

    res.json({ success: true, message: 'レシートを印刷しました' });
  } catch (error) {
    console.error('印刷エラー:', error);
    res.status(500).json({ error: '印刷に失敗しました', details: error.message });
  }
});

// 注文伝票印刷（キッチン用）
app.post('/print/order', async (req, res) => {
  const { orderNumber, tableName, items, timestamp } = req.body;

  const printerInfo = getPrinter();
  if (!printerInfo) {
    return res.status(500).json({ error: 'プリンターが見つかりません' });
  }

  const { device, printer } = printerInfo;

  try {
    await new Promise((resolve, reject) => {
      device.open((err) => {
        if (err) {
          reject(err);
          return;
        }

        printer
          .font('a')
          .align('ct')
          .style('bu')
          .size(2, 2)
          .text('注文伝票')
          .size(1, 1)
          .text('--------------------------------')
          .align('lt')
          .style('b')
          .text(`注文番号: ${orderNumber}`)
          .text(`テーブル: ${tableName}`)
          .style('normal')
          .text(`時刻: ${timestamp}`)
          .text('--------------------------------')
          .size(1, 1);

        // 商品リスト
        items.forEach(item => {
          printer
            .style('b')
            .text(`${item.name}`)
            .style('normal')
            .text(`  数量: ${item.quantity}`)
            .text('');
          
          if (item.memo) {
            printer.text(`  メモ: ${item.memo}`).text('');
          }
        });

        printer
          .text('--------------------------------')
          .text('')
          .cut()
          .close(() => {
            resolve();
          });
      });
    });

    res.json({ success: true, message: '注文伝票を印刷しました' });
  } catch (error) {
    console.error('印刷エラー:', error);
    res.status(500).json({ error: '印刷に失敗しました', details: error.message });
  }
});

// サーバー起動
app.listen(PORT, () => {
  console.log(`印刷サーバーがポート ${PORT} で起動しました`);
  console.log(`ヘルスチェック: http://localhost:${PORT}/health`);
  
  // プリンター接続確認
  const printerInfo = getPrinter();
  if (printerInfo) {
    console.log('✓ プリンターが接続されています');
  } else {
    console.log('✗ プリンターが見つかりません。USB接続を確認してください。');
  }
});
