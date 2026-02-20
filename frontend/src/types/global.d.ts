export {};

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