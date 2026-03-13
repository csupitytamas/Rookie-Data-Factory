const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const { exec } = require('child_process');
const path = require('path');
const fs = require('fs');
const http = require('http');
const chokidar = require('chokidar');
const { createMenu } = require("./menu");

let mainWindow;

// --- ÚTVONAL KEZELÉS ---
const isDev = !app.isPackaged;
const PROJECT_ROOT = isDev ? path.join(__dirname, '../../') : path.dirname(process.execPath);
const DOCKER_OUT_PATH = path.join(PROJECT_ROOT, 'output');
const DEBUG_LOG_PATH = path.join(PROJECT_ROOT, 'electron_debug.log');

// Saját log függvény: kiírja a konzolra ÉS egy fájlba is az .exe mellé
function logToFile(msg) {
    const time = new Date().toISOString();
    const logMsg = `[${time}] ${msg}\n`;
    fs.appendFileSync(DEBUG_LOG_PATH, logMsg);
    console.log(msg);
}

// Kezdő log
logToFile("--- Alkalmazás indul ---");
logToFile(`Mód: ${isDev ? "Fejlesztői" : "Produkciós (.exe)"}`);
logToFile(`Projekt gyökér: ${PROJECT_ROOT}`);

if (!fs.existsSync(DOCKER_OUT_PATH)) {
    fs.mkdirSync(DOCKER_OUT_PATH, { recursive: true });
    logToFile("Output mappa létrehozva.");
}

/**
 * Lekéri a mentési útvonalat. Kezeli a 307-es átirányítást is.
 */
function getDownloadPath() {
    return new Promise((resolve) => {
        const url = 'http://localhost:8000/etl/settings/';
        http.get(url, (res) => {
            // Ha a Backend átirányít (307), kövessük a Location fejlécet
            if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
                logToFile(`Átirányítás észlele (307) -> ${res.headers.location}`);
                http.get(res.headers.location, (res2) => {
                    let data = '';
                    res2.on('data', d => data += d);
                    res2.on('end', () => {
                        try { resolve(JSON.parse(data).download_path); } catch(e) { resolve(null); }
                    });
                });
                return;
            }

            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                try {
                    const settings = JSON.parse(data);
                    resolve(settings.download_path || null);
                } catch (e) { resolve(null); }
            });
        }).on("error", (err) => {
            logToFile(`Hiba a Settings lekérésekor: ${err.message}`);
            resolve(null);
        });
    });
}

// --- FÁJLFIGYELŐ (CHOKIDAR) ---
chokidar.watch(DOCKER_OUT_PATH, {
    ignoreInitial: true,
    persistent: true,
    usePolling: true,
    interval: 1000,
    awaitWriteFinish: { stabilityThreshold: 2000, pollInterval: 500 }
}).on('add', async (filePath) => {
    logToFile(`ÚJ FÁJL A DOCKERBEN: ${filePath}`);
    const fileName = path.basename(filePath);

    try {
        const userBasePath = await getDownloadPath();
        logToFile(`Cél útvonal a Settings alapján: ${userBasePath}`);

        if (userBasePath) {
            // Mappaszerkezet felépítése (RookieDataFactory/Results)
            const resultsFolder = path.join(userBasePath, 'RookieDataFactory', 'Results');

            if (!fs.existsSync(resultsFolder)) {
                fs.mkdirSync(resultsFolder, { recursive: true });
                logToFile(`Létrehozva: ${resultsFolder}`);
            }

            const destPath = path.join(resultsFolder, fileName);

            // TÉNYLEGES MÁSOLÁS
            fs.copyFileSync(filePath, destPath);
            logToFile(`SIKER: Fájl átmásolva ide: ${destPath}`);
        } else {
            logToFile("FIGYELEM: Nincs mentési útvonal beállítva a Settings-ben!");
        }
    } catch (err) {
        logToFile(`HIBA a másolás során: ${err.message}`);
    }
});

// --- ABLAK ÉS DOCKER ---
async function createWindow() {
    mainWindow = new BrowserWindow({
        width: 1600, height: 800, show: false,
        webPreferences: { nodeIntegration: false, contextIsolation: true, preload: path.join(__dirname, 'preload.js') }
    });

    createMenu(mainWindow);
    mainWindow.loadFile(path.join(__dirname, 'loading.html'));
    mainWindow.once('ready-to-show', () => mainWindow.show());

    logToFile("Docker indítása...");
    exec('docker compose up -d', { cwd: PROJECT_ROOT }, (err) => {
        if (err) logToFile(`Docker Error: ${err.message}`);
    });

    // Várakozás a Vite szerverre
    const checkInterval = setInterval(() => {
        http.get('http://localhost:5173', () => {
            logToFile("Szerver kész, betöltés...");
            clearInterval(checkInterval);
            mainWindow.loadURL('http://localhost:5173');
        }).on("error", () => {});
    }, 2000);
}

app.whenReady().then(createWindow);

app.on("window-all-closed", () => {
    logToFile("Alkalmazás leállítása, konténerek lekapcsolása...");
    exec('docker compose down', { cwd: PROJECT_ROOT }, () => {
        if (process.platform !== "darwin") app.quit();
    });
});

// --- IPC HANDLEREK ---
ipcMain.handle('dialog:openDirectory', async () => {
    const { canceled, filePaths } = await dialog.showOpenDialog({ properties: ['openDirectory'] });
    return canceled ? null : filePaths[0];
});

ipcMain.handle('create-directories', async (event, basePath) => {
    if (!basePath) return { success: false };
    try {
        const mainPath = path.join(basePath, 'RookieDataFactory');
        const resultsPath = path.join(mainPath, 'Results');
        const logsPath = path.join(mainPath, 'Logs');
        if (!fs.existsSync(mainPath)) fs.mkdirSync(mainPath, { recursive: true });
        if (!fs.existsSync(resultsPath)) fs.mkdirSync(resultsPath, { recursive: true });
        if (!fs.existsSync(logsPath)) fs.mkdirSync(logsPath, { recursive: true });
        return { success: true, mainPath, resultsPath, logsPath };
    } catch (err) { return { success: false, error: err.message }; }
});

ipcMain.handle('save-file-to-folder', async (event, { fileName, fileContent, basePath, subFolder }) => {
    try {
        const targetDir = path.join(basePath, 'RookieDataFactory', subFolder);
        if (!fs.existsSync(targetDir)) fs.mkdirSync(targetDir, { recursive: true });
        const filePath = path.join(targetDir, fileName);
        fs.writeFileSync(filePath, fileContent);
        return { success: true, filePath };
    } catch (err) { return { success: false, error: err.message }; }
});