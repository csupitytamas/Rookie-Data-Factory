<!-- A fájl a futtatási előzmények és azok logjai megjelenítéséért felelős komponens. -->
<template>
  <div class="history-page">
    <div class="header">
      <h2>Execution History</h2>
      <button @click="refreshHistory" class="btn-refresh btn-primary">Refresh</button>
    </div>

    <div v-if="loading" class="loading-state">
      <div class="spinner"></div>
      <p>Loading history...</p>
    </div>

    <div v-else-if="error" class="error-state">
      {{ error }}
    </div>

    <div v-else class="table-container">
      <table class="history-table">
        <thead>
          <tr>
            <th>Date</th>
            <th>Name</th>
            <th>Source</th>
            <th>Status</th>
            <th>Logs</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in historyItems" :key="item.id">
            <td class="col-date">{{ formatDate(item.run_at) }}</td>
            <td class="col-name">
              <strong>{{ item.pipeline_name }}</strong>
            </td>
            <td>{{ item.source }}</td>
            <td>
              <span :class="['status-badge', item.status]">
                {{ item.status }}
              </span>
            </td>
            <td>
               <button @click="viewLogs(item.id)" class="btn-logs">
                 Report 🧾
               </button>
            </td>
          </tr>
        </tbody>
      </table>

      <div v-if="historyItems.length === 0" class="empty-state">
        No execution history found.
      </div>
    </div>
    
    <div class="footer">
      <router-link to="/" class="btn-back">← Back to Dashboard</router-link>
    </div>

    <div v-if="showLogModal" class="modal-overlay" @click.self="closeLogs">
      <div class="modal-content">
        <div class="modal-header">
          <h3>Execution Logs</h3>
          <button @click="closeLogs" class="btn-close-x">✕</button>
        </div>
        
        <div class="modal-body">
            <div v-if="logLoading" class="log-loading">
               <div class="spinner small"></div> Fetching logs...
            </div>
            <pre v-else class="log-viewer">{{ currentLogs }}</pre>
        </div>
        
        <div class="modal-footer">
          <button 
            @click="downloadLogs" 
            class="btn-download btn-success" 
            :class="{ 'success-mode': isSuccess }"
            :disabled="!currentLogs || logLoading || isSaving"
          >
            {{ isSaving ? 'Saving...' : (isSuccess ? 'Success!' : 'Download') }} 
          </button>
          
          <button @click="closeLogs" class="btn-close btn-secondary">Close</button>
        </div>
      </div>
    </div>

  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { getPipelineHistory, getPipelineLogs } from '@/api/pipeline';
import axios from 'axios';

const historyItems = ref([]);
const loading = ref(true);
const error = ref(null);
const userTimezone = ref('UTC');
const showLogModal = ref(false);
const logLoading = ref(false);
const currentLogs = ref("");
const currentPipelineId = ref(null); 
const isSaving = ref(false); 
const isSuccess = ref(false); 

// Dátum formázó segédfüggvény, amely a megadott időzónát használja a megjelenítéshez.
const formatDate = (dateString) => {
  if (!dateString) return '-';

  // Megpróbáljuk feldolgozni a dátumsztringet; ha nincs benne időzóna információ, UTC-nek tekintjük.
  try {
    let date;
    if (typeof dateString === 'string' && !dateString.includes('Z') && !dateString.includes('+')) {
      date = new Date(dateString + 'Z');
    } else {
      date = new Date(dateString);
    }

    // Az időpont formázása a felhasználó által beállított (vagy alapértelmezett) időzóna szerint.
    return date.toLocaleString('en-US', {
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit',
      timeZone: userTimezone.value
    });
  } catch (e) {
    return dateString;
  }
};

// Frissíti a pipeline futtatási előzményeket a backendről.
const refreshHistory = async () => {
  loading.value = true;
  error.value = null;

  // Lekérdezzük a beállításokat (időzóna) és a teljes futtatási történetet.
  try {
    const settingsRes = await axios.get('http://localhost:8000/etl/settings/');
    userTimezone.value = settingsRes.data?.timezone || 'Europe/Budapest';
    const response = await getPipelineHistory();
    historyItems.value = response.data;
  } catch (err) {
    error.value = "Failed to load history data.";
  } finally {
    loading.value = false;
  }
};

// Megnyitja a log nézetet egy adott pipeline futtatáshoz és lekéri a részletes naplózást.
const viewLogs = async (pipelineId) => {
  showLogModal.value = true;
  logLoading.value = true;
  currentLogs.value = "";
  currentPipelineId.value = pipelineId; 
  isSuccess.value = false;

  // Naplófájlok tartalmának lekérése az API-n keresztül.
  try {
    const response = await getPipelineLogs(pipelineId);
    currentLogs.value = response.data.logs;
  } catch (err) {
    currentLogs.value = "Failed to fetch logs.";
  } finally {
    logLoading.value = false;
  }
};

// A megjelenített logok letöltése és mentése helyi fájlba az Electron bridge segítségével.
const downloadLogs = async () => {
  if (!currentLogs.value) return;
  isSaving.value = true;

  // Lekérjük a mentési útvonalat a beállításokból a célkönyvtár meghatározásához.
  try {
    const settingsRes = await axios.get('http://localhost:8000/etl/settings/');
    const basePath = settingsRes.data?.download_path;

    // Fájlnév generálása a pipeline neve és az aktuális időpont alapján.
    if (!basePath) return;
    const dateStr = new Date().toISOString().replace(/:/g, '-').slice(0, 19); 
    const pipeline = historyItems.value.find(item => item.id === currentPipelineId.value);
    const safeName = pipeline?.pipeline_name.replace(/[^a-zA-Z0-9]/g, '_') || 'logs';
    const fileName = `${safeName}_${dateStr}.txt`;

    // Mentés elindítása az Electron fő folyamatán keresztül a 'Logs' mappába.
    if (window.electron?.saveFileToFolder) {
      await window.electron.saveFileToFolder({ fileName, fileContent: currentLogs.value, basePath, subFolder: 'Logs' });
      isSuccess.value = true;
      setTimeout(() => { isSuccess.value = false; }, 2000);
    }
  } catch (e) {
    console.error(e);
  } finally {
    isSaving.value = false;
  }
};

// A log nézet modális ablakának bezárása.
const closeLogs = () => { showLogModal.value = false; };
onMounted(refreshHistory);
</script>

<style src="./styles/History.css"></style>
