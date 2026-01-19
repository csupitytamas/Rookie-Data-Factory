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
                    label: "Create new ETL pipeline",
                    click: () => {
                        if (mainWindow) {
                            mainWindow.webContents.send("navigate", "/create-etl");
                        }
                    }
                },
                {
                    label: "Active ETL pipelines",
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
              submenu: [
                {
                  label: "User settings",
                  click: () => {
                    if (mainWindow) {
                      mainWindow.webContents.send("navigate", "/settings");
                    }
                  }
                },
                {
                  label: "Logout",
                  click: () => {
                    if (mainWindow) {
                      mainWindow.webContents.send("navigate", "/logout");
                    }
                  }
                }
              ]
            }
    ];

    const menu = Menu.buildFromTemplate(template);
    Menu.setApplicationMenu(menu);
}

module.exports = { createMenu };
