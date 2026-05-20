/**
Ez a fájl az alkalmazás felső menüsorát definiálja és kezeli a navigációs eseményeket.
*/
const { Menu, ipcMain } = require("electron");

// Létrehozza az alkalmazás egyedi menüsorát, és beállítja a navigációs útvonalakat a frontend számára.
function createMenu(mainWindow) {
    const template = [
        {
            label: "Home",
            click: () => {
                if (mainWindow) {
                    mainWindow.webContents.send("navigate", "/");
                }
            }
        },
        {
            label: "History",
            click: () => {
                if (mainWindow) {
                    mainWindow.webContents.send("navigate", "/history");
                }
            }
        },
        {
            label: "Projects",
            submenu: [
                {
                    label: "Create workflow",
                    click: () => {
                        if (mainWindow) {
                            mainWindow.webContents.send("navigate", "/create-etl");
                        }
                    }
                },
                {
                    label: "Active jobs",
                    click: () => {
                        if (mainWindow) {
                            mainWindow.webContents.send("navigate", "/active-pipelines");
                        }
                    }
                }
            ]
        },
        {
              label: "Settings",
               click: () => {
                    if (mainWindow) {
                      mainWindow.webContents.send("navigate", "/settings");
                    }
                    }
                }    
    
    ];

    // Felépíti a menüt a sablon alapján, és globálisan beállítja az alkalmazáshoz.
    const menu = Menu.buildFromTemplate(template);
    Menu.setApplicationMenu(menu);
}

// Exportálja a menükészítő függvényt az Electron fő folyamata számára.
module.exports = { createMenu };
