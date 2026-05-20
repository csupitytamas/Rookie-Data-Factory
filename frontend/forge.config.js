/**
Ez a fájl az Electron Forge konfigurációját tartalmazza, amely az alkalmazás csomagolásáért és a 
telepítőfájlok  létrehozásáért felelős. Meghatározza a beépítendő erőforrásokat
(pl. Docker konfigurációk, környezeti változók), a célplatformokat és a biztonsági beállításokat.
*/

const { FusesPlugin } = require('@electron-forge/plugin-fuses');
const { FuseV1Options, FuseVersion } = require('@electron/fuses');

module.exports = {
  packagerConfig: {
    asar: true,
    icon: './electron/factory',
    extraResource: [
      "../docker-compose.yml",
      "../.env",
      "../airflow",
      "../output",
      "./electron/loading.html",
      "./electron/factory.png"
    ],
  },
  rebuildConfig: {},
  makers: [
    {
      name: '@electron-forge/maker-squirrel',
      config: {
        name: "RookieDataFactory",
        setupIcon: './electron/factory.ico',
        setupExe: "Rookie Data Factory Setup.exe",
        description: "Rookie Data Factory ETL Tool"
      },
    },
    {
      name: '@electron-forge/maker-zip',
      platforms: ['darwin'],
    },
    {
      name: '@electron-forge/maker-deb',
      config: {},
    },
    {
      name: '@electron-forge/maker-rpm',
      config: {},
    },
  ],
  plugins: [
    {
      name: '@electron-forge/plugin-auto-unpack-natives',
      config: {},
    },
    new FusesPlugin({
      version: FuseVersion.V1,
      [FuseV1Options.RunAsNode]: false,
      [FuseV1Options.EnableCookieEncryption]: true,
      [FuseV1Options.EnableNodeOptionsEnvironmentVariable]: false,
      [FuseV1Options.EnableNodeCliInspectArguments]: false,
      [FuseV1Options.EnableEmbeddedAsarIntegrityValidation]: true,
      [FuseV1Options.OnlyLoadAppFromAsar]: true,
    }),
  ],
};