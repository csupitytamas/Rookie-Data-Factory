const { app, BrowserWindow, ipcMain } = require('electron');
const path = require('path');
const http = require('http'); //  HTTP kérések küldéséhez
const { createMenu } = require("./menu");

let mainWindow;
const VITE_DEV_SERVER_URL = 'http://localhost:5173';


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


ipcMain.on("navigate", (_, route) => {
    if (mainWindow) {
        mainWindow.webContents.send("navigate", route);
    }
});
