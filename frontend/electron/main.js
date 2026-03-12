const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const { exec } = require('child_process');
const path = require('path');
const fs = require('fs');
const http = require('http');
const chokidar = require('chokidar');
const { createMenu } = require("./menu");

let mainWindow;

// --- ÚTVONAL KEZELÉS JAVÍTÁSA ---
// Meghatározzuk, hogy az alkalmazás be van-e csomagolva (.exe)
const isDev = !app.isPackaged;

// Ha fejlesztünk (npm start), a gyökér 2 szinttel feljebb van.
// Ha az .exe fut, a gyökér az .exe melletti mappa.
const PROJECT_ROOT = isDev 
    ? path.join(__dirname, '../../') 
    : path.dirname(process.execPath);

const VITE_DEV_SERVER_URL = 'http://localhost:5173';
const DOCKER_OUT_PATH = path.join(PROJECT_ROOT, 'airflow/plugins/out/output');
const BACKEND_SETTINGS_URL = 'http://localhost:8000/etl/settings/';

/**
 * Lekéri a backendtől a felhasználó által beállított letöltési útvonalat.
 */
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
                    if (!data) return resolve(null);
                    const settings = JSON.parse(data);
                    resolve(settings.download_path || null);
                } catch (e) {
                    resolve(null);
                }
            });
        }).on("error", () => {
            resolve(null);
        });
    });
}

// --- FÁJLFIGYELŐ (CHOKIDAR) ---
// Figyeli a Docker által generált kimeneti fájlokat és másolja őket a user mappájába
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
            const resultsFolder = path.join(basePath, 'RookieDataFactory', 'Results');
            if (!fs.existsSync(resultsFolder)) {
                fs.mkdirSync(resultsFolder, { recursive: true });
            }
            const destPath = path.join(resultsFolder, fileName);
            fs.copyFileSync(filePath, destPath);
        }
    } catch (err) {
        console.error("Hiba a fájl másolásakor:", err);
    }
});

// --- ABLAK ÉS DOCKER KEZELÉS ---

/**
 * Megvárja, amíg a Dockerben futó Frontend szerver elérhetővé válik.
 */
async function waitForViteServer(retries = 120) {
    return new Promise((resolve, reject) => {
        const checkServer = () => {
            http.get(VITE_DEV_SERVER_URL, () => {
                resolve(); // Siker! A konténerek futnak.
            }).on("error", () => {
                if (retries > 0) {
                    retries--;
                    setTimeout(() => checkServer(), 1000);
                } else {
                    reject(new Error("Időtúllépés"));
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
        show: false, // Ne mutassuk rögtön, amíg a Docker buildel
        webPreferences: {
            nodeIntegration: false,
            contextIsolation: true,
            preload: path.join(__dirname, 'preload.js')
        }
    });

    createMenu(mainWindow);

    // 1. Mutatunk egy helyi betöltő oldalt (ezt lentebb létrehozzuk)
    // Ez jelzi a usernek, hogy "nyugi, dolgozom a háttérben"
    mainWindow.loadFile(path.join(__dirname, 'loading.html'));
    mainWindow.once('ready-to-show', () => {
        mainWindow.show();
    });

    // 2. Docker indítása --build flag-gel!
    // Ez elintézi az NPM install-t és a Build-et a konténeren belül
    console.log(`Docker indítása (build-del): ${PROJECT_ROOT}`);

    // A parancs: up -d --build
    exec('docker-compose up -d --build', { cwd: PROJECT_ROOT }, (error) => {
        if (error) {
            console.error(`Docker hiba: ${error}`);
            if (!isDev) {
                dialog.showErrorBox('Docker Hiba', 'Ellenőrizd, hogy a Docker Desktop fut-e!');
            }
        }
    });

    // 3. Várakozás a szerverre
    try {
        // Mivel az első build lassú, növeljük a timeout-ot (pl. 5 perc = 300 retries)
        await waitForViteServer(300);

        // Ha kész a build és fut a konténer, átváltunk a valódi URL-re
        mainWindow.loadURL(VITE_DEV_SERVER_URL);
    } catch (err) {
        dialog.showErrorBox('Indítási hiba', 'A háttérfolyamatok túl lassan indultak el.');
    }

    mainWindow.on('closed', () => {
        mainWindow = null;
    });
}

app.whenReady().then(() => {
   createWindow();
});

// Kilépéskor leállítjuk a konténereket is
app.on("window-all-closed", () => {
    console.log("Docker konténerek leállítása...");
    exec('docker-compose down', { cwd: PROJECT_ROOT }, () => {
        if (process.platform !== "darwin") {
            app.quit();
        }
    });
});

// --- IPC HANDLEREK A FRONTEND KOMMUNIKÁCIÓHOZ ---

ipcMain.handle('dialog:openDirectory', async () => {
    const { canceled, filePaths } = await dialog.showOpenDialog({
        properties: ['openDirectory']
    });
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
    } catch (err) {
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
        return { success: false, error: err.message };
    }
});