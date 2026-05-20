/**
    Ez a fájl a Vue alkalmazás belépési pontja, itt történik az inicializálása.
*/

import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import { createPinia } from 'pinia'
import './components/styles/common.css'

// A Vue alkalmazás példányának létrehozása a gyökérkomponenssel.
const app = createApp(App)

// A Pinia állapotkezelő inicializálása.
const pinia = createPinia()

// Az állapotkezelő és a router regisztrálása, majd az alkalmazás csatolása a DOM-hoz.
app.use(pinia)
app.use(router)
app.mount('#app')

// Figyeljük az Electron felől érkező navigációs eseményeket és irányítjuk a Vue Routert.
window.electron?.ipcRenderer?.on("navigate", (_, route) => {
  router.push(route)
})
