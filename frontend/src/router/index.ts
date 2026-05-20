/**
 * A fájl tartalmazza a Vue Router konfigurációját, az útvonalakat és a navigációs guard-okat definiál.
*/

import { createRouter, createWebHashHistory } from "vue-router";
import axios from "axios";
import CreateETLPipeline from "../components/CreateETLPipeline.vue";
import History from "../components/Histroy.vue";
import ActivePipelines from "../components/ActivePipelines.vue";
import Settings from "../components/Settings.vue";
import Dashboard from "../components/Dashboard.vue";
import EditETLConfig from "../components/EditETLConfig.vue";

// Az alkalmazás elérhető útvonalainak definiálása.
const routes = [
    { path: "/", component: Dashboard },
    { path: "/create-etl", component: CreateETLPipeline },
    { path: "/edit-config", component: EditETLConfig},
    { path: "/history", component: History },
    { path: "/active-pipelines", component: ActivePipelines },
    { path: "/settings", component: Settings }
];

// A router példány létrehozása hash alapú előzménykezeléssel.
const router = createRouter({
    history: createWebHashHistory(),
    routes,
});

// Globális navigációs guard: ellenőrzi az alapvető rendszerbeállításokat minden útvonalváltás előtt.
router.beforeEach(async (to, from, next) => {

    // A beállítások oldal elérésekor nincs szükség külön ellenőrzésre.
    if (to.path === '/settings') {
        return next();
    }

    // Ellenőrizzük a backendnél, hogy be van-e állítva a mentési útvonal.
    try {
        const response = await axios.get('http://localhost:8000/etl/settings/');
        const downloadPath = response.data?.download_path;

        // Ha nincs beállított útvonal, a felhasználót a beállítások oldalra kényszerítjük.
        if (!downloadPath) {
            console.log("No download path found, redirecting to settings...");
            return next('/settings');
        }
    } catch (err) {
        console.error("Failed to check settings in router:", err);
        return next('/settings');
    }
    next();
});

export default router;
