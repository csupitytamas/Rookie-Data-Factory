/**
    Ez a fájl biztosít biztonságos hidat az Electron fő folyamata és a Vue megjelenítő folyamat között.
*/
const { contextBridge, ipcRenderer } = require('electron');

// Az Electron API biztonságos exportálása a renderer folyamat számára a contextBridge segítségével.
contextBridge.exposeInMainWorld('electron', {
    ipcRenderer: {
        on: (channel, func) => ipcRenderer.on(channel, func),
        send: (channel, ...args) => ipcRenderer.send(channel, ...args),
    },

    // Segédfüggvények a mappaválasztáshoz, könyvtárszerkezet létrehozásához és fájlmentéshez az IPC-n keresztül.
    selectFolder: () => ipcRenderer.invoke('dialog:openDirectory'),
    createDirectories: (basePath) => ipcRenderer.invoke('create-directories', basePath),
    saveFileToFolder: (data) => ipcRenderer.invoke('save-file-to-folder', data)
});