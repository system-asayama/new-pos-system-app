const { ipcRenderer } = require('electron');

let printCount = 0;
let config = null;

// ページ読み込み時に設定を読み込む
window.addEventListener('DOMContentLoaded', async () => {
    config = await ipcRenderer.invoke('load-config');
    if (config) {
        document.getElementById('enabled').checked = config.enabled || false;
        document.getElementById('herokuUrl').value = config.herokuUrl || '';
        document.getElementById('storeId').value = config.storeId || 1;
        document.getElementById('apiKey').value = config.apiKey || '';
        document.getElementById('interval').value = config.interval || 10000;

        updatePollingStatus(config.enabled);
        updatePrinterList();
        updatePrinterCount();
    }
});

// タブ切り替え
function switchTab(tabName) {
    // すべてのタブとコンテンツを非アクティブに
    document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));

    // 選択されたタブとコンテンツをアクティブに
    event.target.classList.add('active');
    document.getElementById(`${tabName}-tab`).classList.add('active');
}

// フォーム送信時
document.getElementById('configForm').addEventListener('submit', async (e) => {
    e.preventDefault();

    config.enabled = document.getElementById('enabled').checked;
    config.herokuUrl = document.getElementById('herokuUrl').value.trim();
    config.storeId = parseInt(document.getElementById('storeId').value);
    config.apiKey = document.getElementById('apiKey').value.trim();
    config.interval = parseInt(document.getElementById('interval').value);

    const result = await ipcRenderer.invoke('save-config', config);
    
    if (result) {
        showStatus('設定を保存しました！', 'success');
        updatePollingStatus(config.enabled);
    } else {
        showStatus('設定の保存に失敗しました', 'error');
    }
});

// 接続テスト
async function testConnection() {
    const testConfig = {
        herokuUrl: document.getElementById('herokuUrl').value.trim(),
        storeId: parseInt(document.getElementById('storeId').value),
        apiKey: document.getElementById('apiKey').value.trim()
    };

    if (!testConfig.herokuUrl || !testConfig.apiKey) {
        showStatus('HerokuアプリURLとAPIキーを入力してください', 'error');
        return;
    }

    showStatus('接続テスト中...', 'info');

    const result = await ipcRenderer.invoke('test-connection', testConfig);
    
    if (result.success) {
        showStatus('✅ ' + result.message, 'success');
    } else {
        showStatus('❌ 接続失敗: ' + result.message, 'error');
    }
}

// プリンタを検索
async function scanPrinters() {
    const scanResults = document.getElementById('scanResults');
    scanResults.style.display = 'block';
    scanResults.innerHTML = `
        <div class="scanning">
            <div class="spinner"></div>
            <p>ローカルネットワークをスキャン中...</p>
            <p style="font-size: 12px; margin-top: 10px;">最大30秒かかる場合があります</p>
        </div>
    `;
    
    showStatus('プリンタを検索中...', 'info');
    
    try {
        const printers = await ipcRenderer.invoke('scan-printers');
        
        if (printers.length === 0) {
            scanResults.innerHTML = `
                <div class="scanning">
                    <p>❌ プリンタが見つかりませんでした</p>
                    <p style="font-size: 12px; margin-top: 10px;">プリンタの電源とネットワーク接続を確認してください</p>
                </div>
            `;
            showStatus('プリンタが見つかりませんでした', 'error');
        } else {
            let html = '<div class="scan-results">';
            html += `<p style="margin-bottom: 10px; font-weight: bold;">${printers.length}台のプリンタを検出しました：</p>`;
            
            for (const printer of printers) {
                html += `
                    <div class="scan-item" onclick="selectScannedPrinter('${printer.ip}', ${printer.port})">
                        <div style="font-weight: bold;">📡 ${printer.ip}</div>
                        <div style="font-size: 12px; opacity: 0.8;">ポート: ${printer.port}</div>
                    </div>
                `;
            }
            
            html += '</div>';
            scanResults.innerHTML = html;
            showStatus(`${printers.length}台のプリンタを検出しました`, 'success');
        }
    } catch (error) {
        scanResults.innerHTML = `
            <div class="scanning">
                <p>❌ エラーが発生しました</p>
                <p style="font-size: 12px; margin-top: 10px;">${error.message}</p>
            </div>
        `;
        showStatus('検索中にエラーが発生しました: ' + error.message, 'error');
    }
}

// スキャンしたプリンタを選択
function selectScannedPrinter(ip, port) {
    document.getElementById('newPrinterIp').value = ip;
    document.getElementById('newPrinterPort').value = port;
    document.getElementById('scanResults').style.display = 'none';
    showStatus(`プリンタ ${ip}:${port} を選択しました`, 'success');
}

// プリンタを追加
async function addPrinter() {
    const name = document.getElementById('newPrinterName').value.trim();
    const ip = document.getElementById('newPrinterIp').value.trim();
    const port = parseInt(document.getElementById('newPrinterPort').value);

    if (!name || !ip) {
        showStatus('プリンタ名とIPアドレスを入力してください', 'error');
        return;
    }

    const printer = {
        name: name,
        ip: ip,
        port: port,
        enabled: true
    };

    const result = await ipcRenderer.invoke('add-printer', printer);
    
    if (result) {
        showStatus('プリンタを追加しました！', 'success');
        document.getElementById('newPrinterName').value = '';
        document.getElementById('newPrinterIp').value = '';
        document.getElementById('newPrinterPort').value = 9100;
        
        // 設定を再読み込み
        config = await ipcRenderer.invoke('load-config');
        updatePrinterList();
        updatePrinterCount();
    } else {
        showStatus('プリンタの追加に失敗しました', 'error');
    }
}

// プリンタリストを更新
function updatePrinterList() {
    const printerList = document.getElementById('printerList');
    
    if (!config || !config.printers || config.printers.length === 0) {
        printerList.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">🖨️</div>
                <p>プリンタが登録されていません</p>
                <p style="font-size: 13px; margin-top: 10px; color: #999;">上のフォームから新しいプリンタを追加してください</p>
            </div>
        `;
        return;
    }

    let html = '';
    for (const printer of config.printers) {
        html += `
            <div class="printer-item ${printer.enabled ? '' : 'disabled'}">
                <div class="printer-info">
                    <div class="printer-name">${printer.name}</div>
                    <div class="printer-details">📡 ${printer.ip}:${printer.port}</div>
                </div>
                <div class="printer-actions">
                    <label class="toggle-switch">
                        <input type="checkbox" ${printer.enabled ? 'checked' : ''} onchange="togglePrinter(${printer.id})">
                        <span class="slider"></span>
                    </label>
                    <button class="btn-danger" onclick="deletePrinter(${printer.id})">🗑️ 削除</button>
                </div>
            </div>
        `;
    }
    
    printerList.innerHTML = html;
}

// プリンタの有効/無効を切り替え
async function togglePrinter(printerId) {
    const printer = config.printers.find(p => p.id === printerId);
    if (printer) {
        printer.enabled = !printer.enabled;
        const result = await ipcRenderer.invoke('update-printer', printer);
        
        if (result) {
            showStatus(`${printer.name}を${printer.enabled ? '有効' : '無効'}にしました`, 'success');
            updatePrinterList();
        } else {
            showStatus('プリンタの更新に失敗しました', 'error');
        }
    }
}

// プリンタを削除
async function deletePrinter(printerId) {
    const printer = config.printers.find(p => p.id === printerId);
    if (!printer) return;

    if (!confirm(`${printer.name}を削除してもよろしいですか？`)) {
        return;
    }

    const result = await ipcRenderer.invoke('delete-printer', printerId);
    
    if (result) {
        showStatus('プリンタを削除しました', 'success');
        config = await ipcRenderer.invoke('load-config');
        updatePrinterList();
        updatePrinterCount();
    } else {
        showStatus('プリンタの削除に失敗しました', 'error');
    }
}

// プリンタ数を更新
function updatePrinterCount() {
    const count = config && config.printers ? config.printers.length : 0;
    document.getElementById('printerCount').textContent = count;
}

// ステータスメッセージを表示
function showStatus(message, type) {
    const statusDiv = document.getElementById('status');
    statusDiv.textContent = message;
    statusDiv.className = 'status ' + type;
    statusDiv.style.display = 'block';

    if (type === 'success' || type === 'error') {
        setTimeout(() => {
            statusDiv.style.display = 'none';
        }, 5000);
    }
}

// ポーリング状態を更新
function updatePollingStatus(enabled) {
    const statusElement = document.getElementById('pollingStatus');
    if (enabled) {
        statusElement.textContent = '稼働中';
        statusElement.style.color = '#28a745';
    } else {
        statusElement.textContent = '停止中';
        statusElement.style.color = '#dc3545';
    }
}

// 新規注文通知を受信
ipcRenderer.on('new-orders', (event, count) => {
    printCount += count;
    document.getElementById('printCount').textContent = printCount;
    showStatus(`${count}件の注文を印刷しました`, 'success');
});

// ポーリングエラー通知を受信
ipcRenderer.on('polling-error', (event, message) => {
    showStatus('エラー: ' + message, 'error');
});
