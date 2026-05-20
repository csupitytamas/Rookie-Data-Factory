/**
    A main.js az Electron fő folyamata, amely a konténerek indításáért,
    az ablakkezelésért és a fájlrendszer-műveletekért felelős.
*/

// Szükséges modulok importálása az Electron, a fájlrendszer és a Docker kezeléséhez.
const { app, BrowserWindow, ipcMain, dialog, Tray, Menu } = require('electron');
const { exec, execSync } = require('child_process');
const path = require('path');
const fs = require('fs');
const http = require('http');
const chokidar = require('chokidar');
const { createMenu } = require("./menu");

// Alkalmazás állapotát tároló globális változók.
let mainWindow;
let tray = null;
let isQuitting = false;

// Ellenőrizzük, hogy csak egy példány fut-e az alkalmazásból; ha már fut, az ablakot az előtérbe hozzuk.
const gotTheLock = app.requestSingleInstanceLock();
if (!gotTheLock) {
    app.quit();
} else {
    app.on('second-instance', (event, commandLine, workingDirectory) => {
        if (mainWindow) {
            if (mainWindow.isMinimized()) mainWindow.restore();
            mainWindow.show();
            mainWindow.focus();
        }
    });
}


// Fejlesztői (dev) vagy éles (packaged) környezet meghatározása.
const isDev = !app.isPackaged;
const RESOURCES_PATH = isDev 
    ? path.join(__dirname, '../../') 
    : process.resourcesPath;

// A projekt gyökérkönyvtárának és az output mappa útvonalának beállítása.
const PROJECT_ROOT = isDev ? RESOURCES_PATH : RESOURCES_PATH;
const DOCKER_OUT_PATH = path.join(PROJECT_ROOT, 'output');

// Biztosítjuk, hogy a Docker által írt output könyvtár létezzen a gazdagépen.
if (!fs.existsSync(DOCKER_OUT_PATH)) {
    fs.mkdirSync(DOCKER_OUT_PATH, { recursive: true });
}

// Lekéri a backendtől a felhasználó által beállított helyi letöltési útvonalat.
function getDownloadPath() {
    return new Promise((resolve) => {
        const url = 'http://localhost:8000/etl/settings/';

        // HTTP GET kérés a beállítások lekéréséhez.
        http.get(url, (res) => {
            if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
                console.log(`Redirect detected (307) -> ${res.headers.location}`);
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

            // A JSON válasz feldolgozása és a letöltési útvonal kinyerése.
                try {
                    const settings = JSON.parse(data);
                    resolve(settings.download_path || null);
                } catch (e) { resolve(null); }
            });
        }).on("error", (err) => {
            console.log(`Error fetching settings: ${err.message}`);
            resolve(null);
        });
    });
}

// Chokidar watcher indítása az output mappa figyelésére; az új fájlokat automatikusan átmozgatja a felhasználó Results mappájába.
chokidar.watch(DOCKER_OUT_PATH, {
    ignoreInitial: true,
    persistent: true,
    usePolling: true,
    interval: 1000,
    awaitWriteFinish: { stabilityThreshold: 2000, pollInterval: 500 }
}).on('add', async (filePath) => {
    const fileName = path.basename(filePath);

    // Amikor egy pipeline-futtatás során új fájl keletkezik, meghatározzuk a célkönyvtárat.
    try {
        const userBasePath = await getDownloadPath();
        console.log(`Path: ${userBasePath}`);

        // Ha van beállított útvonal, létrehozzuk a Results mappát és átmásoljuk a fájlt a Docker konténerből.
        if (userBasePath) {
            const resultsFolder = path.join(userBasePath, 'RookieDataFactory', 'Results');
            if (!fs.existsSync(resultsFolder)) {
                fs.mkdirSync(resultsFolder, { recursive: true });
            }

            // Fájl másolása a végső helyére.
            const destPath = path.join(resultsFolder, fileName);
            fs.copyFileSync(filePath, destPath);
        } else {
            console.log("No save path has been set!");
        }
    } catch (err) {
        console.log(`Error: ${err.message}`);
    }
});

// Tálca ikon létrehozása a háttérben való futás biztosításához.
function createTray() {
    const iconPath = path.join(__dirname, 'factory.png');
    tray = new Tray(iconPath);

    // Kontextus menü összeállítása a tálca ikonhoz.
    const contextMenu = Menu.buildFromTemplate([
        { 
            label: 'Open',
            click: () => {
                if (mainWindow) {
                    mainWindow.show();
                    mainWindow.focus();
                }
            } 
        },
        { type: 'separator' },
        { 
            label: 'Exit',
            click: () => {
                isQuitting = true;
                app.quit();
            } 
        }
    ]);

    // Tálca ikon tulajdonságainak és eseménykezelőinek beállítása.
    tray.setToolTip('Rookie Data Factory');
    tray.setContextMenu(contextMenu);
    tray.on('double-click', () => {
        if (mainWindow) {
            mainWindow.show();
            mainWindow.focus();
        }
    });
}

// Az alkalmazás főablakának létrehozása és inicializálása.
async function createWindow() {
    mainWindow = new BrowserWindow({
        width: 1600, height: 800, show: false,
        icon: path.join(__dirname, 'factory.png'),
        webPreferences: { nodeIntegration: false, contextIsolation: true, preload: path.join(__dirname, 'preload.js') }
    });

    // Menü és tálca ikon létrehozása.
    createMenu(mainWindow);
    createTray();

    // A betöltő képernyő (loading.html) elérési útjának meghatározása a környezettől függően.
    const loadingPath = isDev 
        ? path.join(__dirname, 'loading.html') 
        : path.join(RESOURCES_PATH, 'loading.html');

    // Betöltő képernyő megjelenítése, amíg a Docker stack feláll.
    mainWindow.loadFile(loadingPath);
    mainWindow.once('ready-to-show', () => mainWindow.show());

    // Bezárás esemény elkapása: az ablakot csak elrejtjük a tálcára, nem zárjuk be az alkalmazást.
    mainWindow.on('close', (event) => {
        if (!isQuitting) {
            event.preventDefault();
            mainWindow.hide();
            return false;
        }
    });

    // Docker konténerek indítása a megfelelő docker-compose fájl használatával.
    console.log("Starting Docker...");
    const composeFile = isDev && fs.existsSync(path.join(PROJECT_ROOT, 'docker-compose.dev.yml')) 
        ? 'docker-compose.dev.yml' 
        : 'docker-compose.yml';
    const composeCmd = `docker compose -f ${composeFile} up -d`;
    console.log(`Docker command: ${composeCmd}`);

    // Docker parancs végrehajtása aszinkron módon.
    exec(composeCmd, { cwd: PROJECT_ROOT }, (err) => {
        if (err) console.log(`Docker Error: ${err.message}`);
    });

    // Periodikus ellenőrzés indítása: megvárjuk, amíg a frontend és a backend szerverek is elérhetővé válnak.
    const checkInterval = setInterval(() => {
        const checkFrontend = new Promise(resolve => {
            http.get('http://localhost:5173', () => resolve(true)).on("error", () => resolve(false));
        });
        const checkBackend = new Promise(resolve => {
            http.get('http://localhost:8000/', () => resolve(true)).on("error", () => resolve(false));
        });

        // Ha minden szolgáltatás kész, betöltjük a tényleges Vue alkalmazást.
        Promise.all([checkFrontend, checkBackend]).then(([feReady, beReady]) => {
            if (feReady && beReady) {
                console.log("Servers are ready, loading...");
                clearInterval(checkInterval);
                mainWindow.loadURL('http://localhost:5173');
            } else {
                if (!feReady) console.log("Waiting for Frontend (5173)...");
                if (!beReady) console.log("Waiting for Backend (8000)...");
            }
        });
    }, 2000);
}

app.whenReady().then(createWindow);

// macOS specifikus viselkedés kezelése az ablakok bezárásakor.
app.on("window-all-closed", () => {
    if (process.platform === 'darwin') {
    }
});

// Kilépés előtti takarítás: a Docker konténerek leállítása (docker compose down).
app.on("before-quit", (event) => {
    console.log("Stopping application, shutting down containers...");

    // Szinkron módon leállítjuk a stack-et a biztonságos kilépés érdekében.
    try {
        execSync('docker compose down', { cwd: PROJECT_ROOT });
        console.log("Docker containers stopped.");
    } catch (err) {
        console.log(`Error during Docker shutdown: ${err.message}`);
    }
});

// IPC handler: Könyvtárválasztó párbeszédablak megnyitása a felhasználó számára.
ipcMain.handle('dialog:openDirectory', async () => {
    const { canceled, filePaths } = await dialog.showOpenDialog({ properties: ['openDirectory'] });
    return canceled ? null : filePaths[0];
});

// IPC handler: Létrehozza a szükséges könyvtárszerkezetet a választott célhelyen a desktopon.
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

// IPC handler: Fájl mentése a megadott mappába és tartalommal.
ipcMain.handle('save-file-to-folder', async (event, { fileName, fileContent, basePath, subFolder }) => {
    try {
        const targetDir = path.join(basePath, 'RookieDataFactory', subFolder);
        if (!fs.existsSync(targetDir)) fs.mkdirSync(targetDir, { recursive: true });
        const filePath = path.join(targetDir, fileName);
        fs.writeFileSync(filePath, fileContent);
        return { success: true, filePath };
    } catch (err) { return { success: false, error: err.message }; }
});