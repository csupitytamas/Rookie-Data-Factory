const { Menu, ipcMain } = require("electron");

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
                    label: "Create new",
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
                },
                {
                    label: "Help",
                    click: () => {
                        if (mainWindow) {
                            mainWindow.webContents.send("navigate", "/help");
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

    const menu = Menu.buildFromTemplate(template);
    Menu.setApplicationMenu(menu);
}

module.exports = { createMenu };
