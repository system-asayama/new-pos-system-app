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
            
            // 旧形式から新形式への移行
            if (config.printerIp && !config.printers) {
                config.printers = [{
                    id: 1,
                    name: 'メインプリンタ',
                    ip: config.printerIp,
                    port: config.printerPort || 9100,
                    enabled: true
                }];
                delete config.printerIp;
                delete config.printerPort;
                saveConfig(config);
            }
            
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
        width: 700,
        height: 800,
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

// 自動起動を設定
function setupAutoLaunch() {
    app.setLoginItemSettings({
        openAtLogin: true,
        path: app.getPath('exe')
    });
}

// プリンタに印刷
async function printToDevice(printerConfig, orderData) {
    return new Promise((resolve, reject) => {
        try {
            const device = new escpos.Network(printerConfig.ip, printerConfig.port);
            const printer = new escpos.Printer(device);

            device.open((error) => {
                if (error) {
                    reject(new Error(`プリンタ接続エラー (${printerConfig.name}): ${error.message}`));
                    return;
                }

                try {
                    printer
                        .font('a')
                        .align('ct')
                        .style('bu')
                        .size(2, 2)
                        .text(orderData.title || '注文伝票')
                        .size(1, 1)
                        .style('normal')
                        .text('------------------------')
                        .align('lt')
                        .text(`注文番号: ${orderData.orderNumber || ''}`)
                        .text(`テーブル: ${orderData.table || ''}`)
                        .text(`日時: ${orderData.datetime || ''}`)
                        .text('------------------------');

                    if (orderData.items && orderData.items.length > 0) {
                        orderData.items.forEach(item => {
                            printer.text(`${item.name} x${item.quantity}`);
                            if (item.notes) {
                                printer.text(`  備考: ${item.notes}`);
                            }
                        });
                    }

                    printer
                        .text('------------------------')
                        .text('')
                        .cut()
                        .close(() => {
                            console.log(`印刷完了: ${printerConfig.name}`);
                            resolve();
                        });
                } catch (printError) {
                    reject(new Error(`印刷エラー (${printerConfig.name}): ${printError.message}`));
                }
            });
        } catch (error) {
            reject(new Error(`プリンタ初期化エラー (${printerConfig.name}): ${error.message}`));
        }
    });
}

// 新規注文をチェックしてプリント
async function checkAndPrint() {
    if (!config || !config.enabled) return;
    if (!config.printers || config.printers.length === 0) return;

    try {
        const response = await axios.get(
            `${config.herokuUrl}/api/printer-server/new-orders`,
            {
                params: {
                    store_id: config.storeId,
                    last_order_id: lastOrderId
                },
                headers: {
                    'X-API-Key': config.apiKey
                },
                timeout: 10000
            }
        );

        if (response.data && response.data.orders && response.data.orders.length > 0) {
            const orders = response.data.orders;
            console.log(`新規注文: ${orders.length}件`);

            // 有効なプリンタのみフィルタリング
            const enabledPrinters = config.printers.filter(p => p.enabled);

            if (enabledPrinters.length === 0) {
                console.log('有効なプリンタがありません');
                return;
            }

            // 各注文を全ての有効なプリンタに印刷
            for (const order of orders) {
                const printPromises = enabledPrinters.map(printer => 
                    printToDevice(printer, order).catch(error => {
                        console.error(`印刷失敗 (${printer.name}):`, error.message);
                        mainWindow.webContents.send('polling-error', `${printer.name}: ${error.message}`);
                        return null; // エラーでも続行
                    })
                );

                await Promise.all(printPromises);
                lastOrderId = Math.max(lastOrderId, order.id || 0);
            }

            mainWindow.webContents.send('new-orders', orders.length);
        }
    } catch (error) {
        console.error('ポーリングエラー:', error.message);
        mainWindow.webContents.send('polling-error', error.message);
    }
}

// ポーリングを開始
function startPolling() {
    if (pollingInterval) {
        clearInterval(pollingInterval);
    }

    if (config && config.enabled) {
        const interval = config.interval || 10000;
        console.log(`ポーリング開始: ${interval}ms間隔`);
        pollingInterval = setInterval(checkAndPrint, interval);
        checkAndPrint(); // 即座に1回実行
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

// プリンタをスキャン
async function scanPrinters() {
    return new Promise((resolve) => {
        const printers = [];
        const networkInterfaces = os.networkInterfaces();
        let baseIp = null;

        // ローカルIPアドレスを取得
        for (const name of Object.keys(networkInterfaces)) {
            for (const net of networkInterfaces[name]) {
                if (net.family === 'IPv4' && !net.internal) {
                    const parts = net.address.split('.');
                    if (parts[0] === '192' && parts[1] === '168') {
                        baseIp = `${parts[0]}.${parts[1]}.${parts[2]}`;
                        break;
                    }
                }
            }
            if (baseIp) break;
        }

        if (!baseIp) {
            resolve([]);
            return;
        }

        console.log(`ネットワークスキャン開始: ${baseIp}.x`);

        const promises = [];
        for (let i = 1; i <= 254; i++) {
            const ip = `${baseIp}.${i}`;
            const promise = new Promise((resolveCheck) => {
                const socket = new net.Socket();
                const timeout = setTimeout(() => {
                    socket.destroy();
                    resolveCheck();
                }, 200);

                socket.on('connect', () => {
                    clearTimeout(timeout);
                    console.log(`プリンタ検出: ${ip}:9100`);
                    printers.push({ ip, port: 9100 });
                    socket.destroy();
                    resolveCheck();
                });

                socket.on('error', () => {
                    clearTimeout(timeout);
                    resolveCheck();
                });

                socket.connect(9100, ip);
            });

            promises.push(promise);
        }

        Promise.all(promises).then(() => {
            console.log(`スキャン完了: ${printers.length}台検出`);
            resolve(printers);
        });
    });
}

// 接続テスト
async function testConnection(testConfig) {
    try {
        const response = await axios.get(
            `${testConfig.herokuUrl}/api/printer-server/new-orders`,
            {
                params: {
                    store_id: testConfig.storeId,
                    last_order_id: 0
                },
                headers: {
                    'X-API-Key': testConfig.apiKey
                },
                timeout: 10000
            }
        );

        return {
            success: true,
            message: '接続成功！APIキーが正しく設定されています。'
        };
    } catch (error) {
        if (error.response && error.response.status === 401) {
            return {
                success: false,
                message: 'APIキーが正しくありません'
            };
        } else if (error.code === 'ECONNABORTED') {
            return {
                success: false,
                message: '接続タイムアウト'
            };
        } else {
            return {
                success: false,
                message: error.message
            };
        }
    }
}

// アプリ起動時
app.whenReady().then(() => {
    createWindow();
    createTray();
    setupAutoLaunch();

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
        // Do nothing - keep running in tray
    }
});

// アプリ終了時
app.on('before-quit', () => {
    stopPolling();
});

// IPC通信ハンドラー
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

ipcMain.handle('scan-printers', async () => {
    return await scanPrinters();
});

ipcMain.handle('test-connection', async (event, testConfig) => {
    return await testConnection(testConfig);
});

ipcMain.handle('add-printer', (event, printer) => {
    if (!config.printers) {
        config.printers = [];
    }
    const newId = config.printers.length > 0 
        ? Math.max(...config.printers.map(p => p.id)) + 1 
        : 1;
    printer.id = newId;
    config.printers.push(printer);
    return saveConfig(config);
});

ipcMain.handle('update-printer', (event, printer) => {
    const index = config.printers.findIndex(p => p.id === printer.id);
    if (index !== -1) {
        config.printers[index] = printer;
        return saveConfig(config);
    }
    return false;
});

ipcMain.handle('delete-printer', (event, printerId) => {
    config.printers = config.printers.filter(p => p.id !== printerId);
    return saveConfig(config);
});
