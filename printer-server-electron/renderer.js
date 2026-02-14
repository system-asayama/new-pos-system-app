const { ipcRenderer } = require('electron');

let printCount = 0;

// ページ読み込み時に設定を読み込む
window.addEventListener('DOMContentLoaded', async () => {
    const config = await ipcRenderer.invoke('load-config');
    if (config) {
        document.getElementById('enabled').checked = config.enabled || false;
        document.getElementById('herokuUrl').value = config.herokuUrl || '';
        document.getElementById('storeId').value = config.storeId || 1;
        document.getElementById('apiKey').value = config.apiKey || '';
        document.getElementById('printerIp').value = config.printerIp || '192.168.1.213';
        document.getElementById('printerPort').value = config.printerPort || 9100;
        document.getElementById('interval').value = config.interval || 10000;

        updatePollingStatus(config.enabled);
    }
});

// フォーム送信時
document.getElementById('configForm').addEventListener('submit', async (e) => {
    e.preventDefault();

    const config = {
        enabled: document.getElementById('enabled').checked,
        herokuUrl: document.getElementById('herokuUrl').value.trim(),
        storeId: parseInt(document.getElementById('storeId').value),
        apiKey: document.getElementById('apiKey').value.trim(),
        printerIp: document.getElementById('printerIp').value.trim(),
        printerPort: parseInt(document.getElementById('printerPort').value),
        interval: parseInt(document.getElementById('interval').value)
    };

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
    const config = {
        herokuUrl: document.getElementById('herokuUrl').value.trim(),
        storeId: parseInt(document.getElementById('storeId').value),
        apiKey: document.getElementById('apiKey').value.trim()
    };

    if (!config.herokuUrl || !config.apiKey) {
        showStatus('HerokuアプリURLとAPIキーを入力してください', 'error');
        return;
    }

    showStatus('接続テスト中...', 'info');

    const result = await ipcRenderer.invoke('test-connection', config);
    
    if (result.success) {
        showStatus('✅ ' + result.message, 'success');
    } else {
        showStatus('❌ 接続失敗: ' + result.message, 'error');
    }
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
