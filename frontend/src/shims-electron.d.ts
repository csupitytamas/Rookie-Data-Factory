/**
 * A fájl kiterjeszti a globális Window interfészt az Electron-specifikus típusokkal.
 * Biztosítja a TypeScript típusbiztonságot a preload.js-ben exponált 'electron' objektumhoz.
 */

export {}

// A globális Window objektum kiegészítése az Electron IPC kommunikációhoz szükséges metódusokkal.
declare global {
    interface Window {
        electron?: {
            ipcRenderer?: {
                on: (channel: string, callback: (event: any, ...args: any[]) => void) => void;
                send: (channel: string, ...args: any[]) => void;
            }
        }
    }
}
