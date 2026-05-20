/**
 * Ez a fájl globális típusdefiníciókat tartalmaz, amelyek kiterjesztik a Window objektumot
 * az Electron által biztosított API-kkal és segédfüggvényekkel.
 */


export {};

// A globális Window interfész kiegészítése az Electron-specifikus metódusokkal.
declare global {
    interface Window {
        electron?: {
            ipcRenderer?: {
                on(channel: string, listener: (event: any, ...args: any[]) => void): void;
            };
            selectFolder: () => Promise<string | null>;
        };
    }
}