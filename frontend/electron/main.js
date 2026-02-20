const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const fs = require('fs');
const http = require('http');
const chokidar = require('chokidar');
const { createMenu } = require("./menu");

let mainWindow;
const VITE_DEV_SERVER_URL = 'http://localhost:5173';
const DOCKER_OUT_PATH = path.join(__dirname, '../../airflow/plugins/out/output');
const BACKEND_SETTINGS_URL = 'http://localhost:8000/etl/settings/';
function getDownloadPath() {
    return new Promise((resolve) => {
        http.get(BACKEND_SETTINGS_URL, (res) => {
            const { statusCode } = res;
            let data = '';

            if (statusCode !== 200) {
                res.resume();
                return resolve(null);
            }

            res.setEncoding('utf8');
            res.on('data', (chunk) => { data += chunk; });
            res.on('end', () => {
                try {
                    if (!data) {
                        console.warn("⚠️ A backend válasza üres volt.");
                        return resolve(null);
                    }
                    const settings = JSON.parse(data);
                    resolve(settings.download_path || null);
                } catch (e) {
                    resolve(null);
                }
            });
        }).on("error", (err) => {
            resolve(null);
        });
    });
}

chokidar.watch(DOCKER_OUT_PATH, {
    ignoreInitial: true, 
    persistent: true,
    usePolling: true,   
    interval: 500,       
    binaryInterval: 1000
}).on('add', async (filePath) => {
    const fileName = path.basename(filePath);
    try {
        const targetFolder = await getDownloadPath();
        if (targetFolder) {
            if (!fs.existsSync(targetFolder)) {
                console.log(`📁 Célmappa nem létezik, létrehozás: ${targetFolder}`);
                fs.mkdirSync(targetFolder, { recursive: true });
            }

            const destPath = path.join(targetFolder, fileName);
            fs.copyFileSync(filePath, destPath);
        } else {
        }
    } catch (err) {

    }
});

// --- ABLAKKEZELÉS ---
async function waitForViteServer(retries = 20) {
    return new Promise((resolve) => {
        const checkServer = () => {
            http.get(VITE_DEV_SERVER_URL, () => {
                resolve();
            }).on("error", () => {
                if (retries > 0) {
                    setTimeout(() => checkServer(), 500);
                } else {
                    resolve();
                }
            });
        };
        checkServer();
    });
}

async function createWindow() {
    mainWindow = new BrowserWindow({
        width: 1600,
        height: 800,
        webPreferences: {
            nodeIntegration: false,
            contextIsolation: true,
            preload: path.join(__dirname, 'preload.js')
        }
    });

    await waitForViteServer();
    mainWindow.loadURL(VITE_DEV_SERVER_URL);
    createMenu(mainWindow);

    mainWindow.on('closed', () => {
        mainWindow = null;
    });
}

app.whenReady().then(() => {
   createWindow();
});

app.on("window-all-closed", () => {
    if (process.platform !== "darwin") {
        app.quit();
    }
});

// Tallózó dialógus
ipcMain.handle('dialog:openDirectory', async () => {
    const { canceled, filePaths } = await dialog.showOpenDialog({
        properties: ['openDirectory']
    });
    return canceled ? null : filePaths[0];
});