const { app, BrowserWindow, ipcMain, Tray, Menu, nativeImage } = require('electron');
const path = require('path');
const fs = require('fs');
const axios = require('axios');
const net = require('net');
const os = require('os');
const escpos = require('escpos');
escpos.Network = require('escpos-network');

let mainWindow = null;
let tray = null;
let pollingInterval = null;
let config = null;
let lastOrderId = 0;

// 設定ファイルのパス
const configPath = path.join(app.getPath('userData'), 'config.json');

// 設定を読み込む
function loadConfig() {
    try {
        if (fs.existsSync(configPath)) {
            const data = fs.readFileSync(configPath, 'utf8');
            config = JSON.parse(data);
            return config;
        }
    } catch (error) {
        console.error('設定ファイルの読み込みエラー:', error);
    }
    return null;
}

// 設定を保存する
function saveConfig(newConfig) {
    try {
        fs.writeFileSync(configPath, JSON.stringify(newConfig, null, 2), 'utf8');
        config = newConfig;
        return true;
    } catch (error) {
        console.error('設定ファイルの保存エラー:', error);
        return false;
    }
}

// メインウィンドウを作成
function createWindow() {
    mainWindow = new BrowserWindow({
        width: 600,
        height: 700,
        webPreferences: {
            nodeIntegration: true,
            contextIsolation: false
        },
        icon: path.join(__dirname, 'assets', 'icon.png')
    });

    mainWindow.loadFile('index.html');

    mainWindow.on('close', (event) => {
        if (!app.isQuitting) {
            event.preventDefault();
            mainWindow.hide();
        }
        return false;
    });
}

// システムトレイを作成
function createTray() {
    const iconPath = path.join(__dirname, 'assets', 'icon.png');
    const trayIcon = nativeImage.createFromPath(iconPath);
    tray = new Tray(trayIcon.resize({ width: 16, height: 16 }));

    const contextMenu = Menu.buildFromTemplate([
        {
            label: '設定を開く',
            click: () => {
                mainWindow.show();
            }
        },
        {
            label: 'ポーリング状態',
            enabled: false
        },
        {
            type: 'separator'
        },
        {
            label: '終了',
            click: () => {
                app.isQuitting = true;
                app.quit();
            }
        }
    ]);

    tray.setToolTip('プリンターサーバー');
    tray.setContextMenu(contextMenu);

    tray.on('click', () => {
        mainWindow.show();
    });
}

// 新規注文をチェック
async function checkNewOrders() {
    if (!config || !config.enabled) {
        return;
    }

    try {
        const response = await axios.get(`${config.herokuUrl}/api/printer-server/new-orders`, {
            params: {
                store_id: config.storeId,
                last_id: lastOrderId
            },
            headers: {
                'X-API-Key': config.apiKey
            },
            timeout: 10000
        });

        if (response.data && response.data.orders && response.data.orders.length > 0) {
            for (const order of response.data.orders) {
                await printOrder(order);
                if (order.id > lastOrderId) {
                    lastOrderId = order.id;
                }
            }
            
            // ウィンドウに通知
            if (mainWindow) {
                mainWindow.webContents.send('new-orders', response.data.orders.length);
            }
        }
    } catch (error) {
        console.error('注文チェックエラー:', error.message);
        if (mainWindow) {
            mainWindow.webContents.send('polling-error', error.message);
        }
    }
}

// 注文を印刷
async function printOrder(order) {
    return new Promise((resolve, reject) => {
        try {
            // プリンタIPアドレスを取得
            const printerIp = config.printerIp || '192.168.1.213';
            const printerPort = config.printerPort || 9100;

            const device = new escpos.Network(printerIp, printerPort);
            const printer = new escpos.Printer(device);

            device.open((error) => {
                if (error) {
                    console.error('プリンタ接続エラー:', error);
                    reject(error);
                    return;
                }

                printer
                    .font('a')
                    .align('ct')
                    .style('bu')
                    .size(2, 2)
                    .text(order.table_name || `テーブル ${order.table_no}`)
                    .size(1, 1)
                    .style('normal')
                    .text('--------------------------------')
                    .align('lt');

                // 注文明細
                if (order.items && order.items.length > 0) {
                    for (const item of order.items) {
                        const qty = item.qty || item.数量 || 1;
                        const name = item.menu_name || item.商品名 || '';
                        printer.text(`${name} x${qty}`);
                    }
                }

                printer
                    .text('--------------------------------')
                    .align('ct')
                    .text(`注文番号: ${order.id}`)
                    .text(`${new Date().toLocaleString('ja-JP')}`)
                    .feed(3)
                    .cut()
                    .close(() => {
                        console.log(`注文 #${order.id} を印刷しました`);
                        resolve();
                    });
            });
        } catch (error) {
            console.error('印刷エラー:', error);
            reject(error);
        }
    });
}

// ポーリングを開始
function startPolling() {
    if (pollingInterval) {
        clearInterval(pollingInterval);
    }

    if (config && config.enabled) {
        const interval = config.interval || 10000;
        pollingInterval = setInterval(checkNewOrders, interval);
        console.log(`ポーリング開始: ${interval}ms間隔`);
        
        // 即座に1回チェック
        checkNewOrders();
    }
}

// ポーリングを停止
function stopPolling() {
    if (pollingInterval) {
        clearInterval(pollingInterval);
        pollingInterval = null;
        console.log('ポーリング停止');
    }
}

// IPCハンドラー
ipcMain.handle('load-config', () => {
    return loadConfig();
});

ipcMain.handle('save-config', (event, newConfig) => {
    const result = saveConfig(newConfig);
    if (result) {
        stopPolling();
        startPolling();
    }
    return result;
});

ipcMain.handle('test-connection', async (event, testConfig) => {
    try {
        const response = await axios.get(`${testConfig.herokuUrl}/api/printer-server/new-orders`, {
            params: {
                store_id: testConfig.storeId,
                last_id: 0
            },
            headers: {
                'X-API-Key': testConfig.apiKey
            },
            timeout: 10000
        });
        return { success: true, message: '接続成功！' };
    } catch (error) {
        return { success: false, message: error.message };
    }
});

// プリンタを自動検出
ipcMain.handle('scan-printers', async () => {
    return new Promise((resolve) => {
        const printers = [];
        const networkInterfaces = os.networkInterfaces();
        
        // ローカルIPアドレスを取得
        let localIp = null;
        for (const name of Object.keys(networkInterfaces)) {
            for (const iface of networkInterfaces[name]) {
                if (iface.family === 'IPv4' && !iface.internal) {
                    localIp = iface.address;
                    break;
                }
            }
            if (localIp) break;
        }
        
        if (!localIp) {
            resolve([]);
            return;
        }
        
        // ネットワークの範囲を計算（例: 192.168.1.x）
        const ipParts = localIp.split('.');
        const baseIp = `${ipParts[0]}.${ipParts[1]}.${ipParts[2]}`;
        
        let completed = 0;
        const total = 254; // 1-254までスキャン
        
        // タイムアウト設定
        const timeout = setTimeout(() => {
            resolve(printers);
        }, 30000); // 30秒でタイムアウト
        
        // 各IPアドレスをスキャン
        for (let i = 1; i <= 254; i++) {
            const ip = `${baseIp}.${i}`;
            
            // ポート9100に接続を試みる
            const socket = new net.Socket();
            socket.setTimeout(1000); // 1秒タイムアウト
            
            socket.on('connect', () => {
                printers.push({
                    ip: ip,
                    port: 9100,
                    status: 'online'
                });
                socket.destroy();
            });
            
            socket.on('timeout', () => {
                socket.destroy();
            });
            
            socket.on('error', () => {
                // 接続失敗（プリンタではない）
            });
            
            socket.on('close', () => {
                completed++;
                if (completed === total) {
                    clearTimeout(timeout);
                    resolve(printers);
                }
            });
            
            socket.connect(9100, ip);
        }
    });
});

// アプリ起動時
app.whenReady().then(() => {
    createWindow();
    createTray();
    
    // 設定を読み込んでポーリング開始
    loadConfig();
    if (config && config.enabled) {
        startPolling();
    }
});

// すべてのウィンドウが閉じられたとき
app.on('window-all-closed', () => {
    // macOS以外ではアプリを終了しない（トレイに常駐）
    if (process.platform !== 'darwin') {
        // 何もしない（トレイに常駐）
    }
});

app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
        createWindow();
    }
});

// アプリ終了時
app.on('before-quit', () => {
    stopPolling();
});

// 自動起動設定
app.setLoginItemSettings({
    openAtLogin: true,
    openAsHidden: true
});
