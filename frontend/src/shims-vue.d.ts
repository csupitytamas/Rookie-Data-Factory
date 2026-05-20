/**
 * A fájl segít a TypeScriptnek felismerni és kezelni a .vue kiterjesztésű fájlokat modulként.
 * Definiálja a Vue komponensek alapértelmezett típusát, lehetővé téve azok importálását TS fájlokba.
 */

declare module "*.vue" {
    import type { DefineComponent } from "vue";
    const component: DefineComponent<{}, {}, any>;
    export default component;
}
