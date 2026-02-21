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

// --- FÁJLFIGYELŐ (CHOKIDAR) MÓDOSÍTVA A ROOKIEDATAFACTORY MAPPÁHOZ ---
chokidar.watch(DOCKER_OUT_PATH, {
    ignoreInitial: true, 
    persistent: true,
    usePolling: true,   
    interval: 500,       
    binaryInterval: 1000
}).on('add', async (filePath) => {
    const fileName = path.basename(filePath);
    try {
        const basePath = await getDownloadPath();
        if (basePath) {
            // A célmappa most: Kiválasztott_útvonal / RookieDataFactory / Results
            const resultsFolder = path.join(basePath, 'RookieDataFactory', 'Results');
            
            if (!fs.existsSync(resultsFolder)) {
                fs.mkdirSync(resultsFolder, { recursive: true });
            }

            const destPath = path.join(resultsFolder, fileName);
            fs.copyFileSync(filePath, destPath);
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

// --- IPC HANDLEREK A FRONTEND KOMMUNIKÁCIÓHOZ ---

ipcMain.handle('dialog:openDirectory', async () => {
    const { canceled, filePaths } = await dialog.showOpenDialog({
        properties: ['openDirectory']
    });
    return canceled ? null : filePaths[0];
});

// 1. Mappák automatikus létrehozása (a Settings.vue hívja majd)
ipcMain.handle('create-directories', async (event, basePath) => {
    if (!basePath) return { success: false };
    try {
        // Fő mappa: RookieDataFactory
        const mainPath = path.join(basePath, 'RookieDataFactory');
        // Almappák:
        const resultsPath = path.join(mainPath, 'Results');
        const logsPath = path.join(mainPath, 'Logs');
        
        // Mappák generálása
        if (!fs.existsSync(mainPath)) fs.mkdirSync(mainPath, { recursive: true });
        if (!fs.existsSync(resultsPath)) fs.mkdirSync(resultsPath, { recursive: true });
        if (!fs.existsSync(logsPath)) fs.mkdirSync(logsPath, { recursive: true });
        
        return { success: true, mainPath, resultsPath, logsPath };
    } catch (err) {
        console.error('Hiba a mappák létrehozásakor:', err);
        return { success: false, error: err.message };
    }
});

ipcMain.handle('save-file-to-folder', async (event, { fileName, fileContent, basePath, subFolder }) => {
    try {
        const targetDir = path.join(basePath, 'RookieDataFactory', subFolder);
        if (!fs.existsSync(targetDir)) fs.mkdirSync(targetDir, { recursive: true });
        const filePath = path.join(targetDir, fileName);
        fs.writeFileSync(filePath, fileContent);
        return { success: true, filePath };
    } catch (err) {
        console.error("Hiba a mentéskor:", err);
        return { success: false, error: err.message };
    }
});