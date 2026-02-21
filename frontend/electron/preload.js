const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electron', {
    ipcRenderer: {
        on: (channel, func) => ipcRenderer.on(channel, func),
        send: (channel, ...args) => ipcRenderer.send(channel, ...args),
    },
    selectFolder: () => ipcRenderer.invoke('dialog:openDirectory'),
    createDirectories: (basePath) => ipcRenderer.invoke('create-directories', basePath),
    saveFileToFolder: (data) => ipcRenderer.invoke('save-file-to-folder', data)
});