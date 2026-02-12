<template>
  <div class="history-page">
    <div class="header">
      <h2>Pipeline Execution History</h2>
      <button @click="refreshHistory" class="btn-refresh">🔄 Refresh</button>
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
            <th>Date & Time</th>
            <th>Pipeline Name</th>
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
                 📜 Logs
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
               <div class="spinner small"></div> Fetching logs from Airflow...
            </div>
            <pre v-else class="log-viewer">{{ currentLogs }}</pre>
        </div>
        
        <div class="modal-footer">
          <button 
            @click="downloadLogs" 
            class="btn-download" 
            :disabled="!currentLogs || logLoading"
          >
            ⬇️ Download .txt
          </button>
          
          <button @click="closeLogs" class="btn-close">Close</button>
        </div>
      </div>
    </div>

  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { getPipelineHistory, getPipelineLogs } from '@/api/pipeline';

const historyItems = ref([]);
const loading = ref(true);
const error = ref(null);

const showLogModal = ref(false);
const logLoading = ref(false);
const currentLogs = ref("");
// Eltároljuk a kiválasztott pipeline ID-t a fájlnévhez
const currentPipelineId = ref(null); 

const formatDate = (dateString) => {
  if (!dateString) return '-';
  return new Date(dateString).toLocaleString('hu-HU', {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit'
  });
};

const refreshHistory = async () => {
  loading.value = true;
  error.value = null;
  try {
    const response = await getPipelineHistory();
    historyItems.value = response.data;
  } catch (err) {
    console.error("Failed to load history:", err);
    error.value = "Failed to load history data. Please ensure the backend is running.";
  } finally {
    loading.value = false;
  }
};

const viewLogs = async (pipelineId) => {
  showLogModal.value = true;
  logLoading.value = true;
  currentLogs.value = "";
  currentPipelineId.value = pipelineId; // ID mentése a fájlnévhez
  
  try {
    const response = await getPipelineLogs(pipelineId);
    currentLogs.value = response.data.logs;
  } catch (err) {
    console.error("Log fetch error:", err);
    currentLogs.value = "Failed to fetch logs. " + (err.response?.data?.detail || err.message);
  } finally {
    logLoading.value = false;
  }
};

// --- ÚJ FÜGGVÉNY: LETÖLTÉS ---
const downloadLogs = () => {
  if (!currentLogs.value) return;

  // Fájlnév generálása: pipeline_ID_datum.txt
  const dateStr = new Date().toISOString().slice(0, 10);
  const fileName = `pipeline_${currentPipelineId.value}_logs_${dateStr}.txt`;

  // Blob készítése a szövegből
  const blob = new Blob([currentLogs.value], { type: 'text/plain' });
  
  // Láthatatlan letöltő link készítése és kattintása
  const link = document.createElement('a');
  link.href = URL.createObjectURL(blob);
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(link.href);
};

const closeLogs = () => {
  showLogModal.value = false;
};

onMounted(() => {
  refreshHistory();
});
</script>

<style scoped>
/* A stílusok nagyrészt változatlanok, csak az új gombhoz adunk hozzá */

.history-page {
  max-width: 1200px;
  margin: 0 auto;
  padding: 20px;
  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.table-container {
  background: white;
  border-radius: 8px;
  box-shadow: 0 4px 6px rgba(0,0,0,0.1);
  overflow: hidden;
}

.history-table {
  width: 100%;
  border-collapse: collapse;
}

.history-table th, .history-table td {
  padding: 12px 15px;
  text-align: left;
  border-bottom: 1px solid #eee;
}

.history-table th {
  background-color: #f8f9fa;
  font-weight: 600;
  color: #333;
}

.history-table tr:hover {
  background-color: #f1f1f1;
}

.status-badge {
  padding: 4px 8px;
  border-radius: 12px;
  font-size: 0.85em;
  font-weight: bold;
  text-transform: uppercase;
}
.status-badge.success { background-color: #d4edda; color: #155724; }
.status-badge.failed { background-color: #f8d7da; color: #721c24; }
.status-badge.running { background-color: #cce5ff; color: #004085; }
.status-badge.queued { background-color: #e2e3e5; color: #383d41; }

.btn-refresh {
  background-color: #007bff;
  color: white;
  border: none;
  padding: 8px 16px;
  border-radius: 4px;
  cursor: pointer;
}

.btn-back {
  display: inline-block;
  margin-top: 20px;
  text-decoration: none;
  color: #6c757d;
  font-weight: bold;
}

.loading-state, .error-state, .empty-state {
  text-align: center;
  padding: 40px;
  background: #fff;
  border-radius: 8px;
}
.error-state { color: red; }

.btn-logs {
  background-color: #6c757d;
  color: white;
  border: none;
  padding: 6px 12px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.9em;
  transition: background 0.2s;
}
.btn-logs:hover {
  background-color: #5a6268;
}

/* Modal Stílusok */
.modal-overlay {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background-color: rgba(0, 0, 0, 0.6);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
  backdrop-filter: blur(2px);
}

.modal-content {
  background: white;
  width: 90%;
  max-width: 1000px;
  height: 80vh;
  border-radius: 8px;
  display: flex;
  flex-direction: column;
  box-shadow: 0 10px 25px rgba(0,0,0,0.2);
}

.modal-header {
  padding: 15px 20px;
  border-bottom: 1px solid #eee;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.modal-body {
  flex: 1;
  overflow: hidden;
  padding: 0;
  display: flex;
  flex-direction: column;
}

.btn-close-x {
  background: none;
  border: none;
  font-size: 1.5rem;
  cursor: pointer;
  color: #666;
}

.log-loading {
  padding: 40px;
  text-align: center;
  font-style: italic;
  color: #666;
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 10px;
}

.log-viewer {
  flex: 1;
  background-color: #1e1e1e;
  color: #d4d4d4;
  padding: 20px;
  margin: 0;
  overflow-y: auto;
  font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
  font-size: 0.85rem;
  line-height: 1.5;
  white-space: pre-wrap;
}

.modal-footer {
  padding: 15px 20px;
  display: flex;
  justify-content: flex-end; /* Jobbra igazítva */
  gap: 10px; /* Távolság a gombok között */
  background-color: #f8f9fa;
  border-top: 1px solid #eee;
}

.btn-close {
  background-color: #6c757d;
  color: white;
  border: none;
  padding: 8px 20px;
  border-radius: 4px;
  cursor: pointer;
}
.btn-close:hover { background-color: #5a6268; }

/* ÚJ STÍLUS A LETÖLTÉS GOMBHOZ */
.btn-download {
  background-color: #28a745; /* Zöld */
  color: white;
  border: none;
  padding: 8px 20px;
  border-radius: 4px;
  cursor: pointer;
  font-weight: 500;
}
.btn-download:hover { background-color: #218838; }
.btn-download:disabled { 
  background-color: #94d3a2; 
  cursor: not-allowed; 
}

.spinner.small {
  width: 20px;
  height: 20px;
  border-width: 2px;
}
</style>