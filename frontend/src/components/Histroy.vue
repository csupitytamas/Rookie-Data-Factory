<template>
  <div class="history-container">
    <h2>Process History</h2>
    
    <div v-if="loading" class="loading">Loading history...</div>

    <ul v-else class="etl-list">
      <li v-if="historyItems.length === 0">No history found.</li>
      
      <li v-for="item in historyItems" :key="item.id" class="etl-item">
        <div class="item-info">
          <strong>{{ item.pipeline_name }}</strong> 
          <span :class="['status-badge', item.status]">{{ item.status }}</span>
          <br>
          <small>{{ formatDate(item.changed_at) }}</small>
          <div v-if="item.message" class="error-msg">{{ item.message }}</div>
        </div>

        <button class="log-btn" @click="openLogModal(item)">
          📄 View Log
        </button>
      </li>
    </ul>

    <div v-if="showModal" class="modal-overlay" @click.self="closeModal">
      <div class="modal-content">
        <div class="modal-header">
          <h3>Log Details: {{ selectedItem?.pipeline_name }}</h3>
          <button class="close-btn" @click="closeModal">×</button>
        </div> 
        
        <div class="modal-body">
          <div v-if="loadingLog">Fetching logs from Airflow...</div>
          <pre v-else class="log-viewer">{{ logContent }}</pre>
        </div>
      </div>
    </div>

  </div>
</template>

<script>
import { getPipelineHistory, getPipelineLog } from '@/api/pipeline';

export default {
  name: "History",
  data() {
    return {
      historyItems: [],
      loading: false,
      
      // Modal state
      showModal: false,
      selectedItem: null,
      logContent: "",
      loadingLog: false
    };
  },
  mounted() {
    this.fetchHistory();
  },
  methods: {
    async fetchHistory() {
      this.loading = true;
      try {
        // Itt hívd meg a valódi végpontodat ami visszaadja a status_history táblát
        // Ha még nincs kész a backend endpoint, akkor ezt cseréld le mock adatra ideiglenesen
        const response = await getPipelineHistory(); 
        this.historyItems = response.data;
      } catch (error) {
        console.error("Failed to load history:", error);
      } finally {
        this.loading = false;
      }
    },

    async openLogModal(item) {
      this.selectedItem = item;
      this.showModal = true;
      this.loadingLog = true;
      this.logContent = "";

      try {
        // Fontos: A 'run_id'-t a history itemből kell kiszedni
        // Ha a history táblában nincs run_id, akkor a backendnek kell azt is mentenie!
        const runId = item.run_id || "latest"; // Fallback, ha nincs ID
        
        const response = await getPipelineLog(item.etlconfig_id, runId);
        this.logContent = response.data.logs || "No logs returned.";
      } catch (error) {
        this.logContent = `Error loading logs: ${error.message}`;
      } finally {
        this.loadingLog = false;
      }
    },

    closeModal() {
      this.showModal = false;
      this.logContent = "";
      this.selectedItem = null;
    },

    formatDate(dateString) {
      if (!dateString) return "";
      return new Date(dateString).toLocaleString();
    }
  }
};
</script>

<style scoped>
.history-container {
  width: 80%;
  max-width: 1000px;
  margin: 30px auto;
  padding: 20px;
  border: 1px solid #e0e0e0;
  background-color: #fff;
  border-radius: 8px;
  box-shadow: 0 2px 10px rgba(0,0,0,0.05);
}

.etl-list {
  list-style-type: none;
  padding: 0;
}

.etl-item {
  padding: 15px;
  border-bottom: 1px solid #eee;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.item-info {
  flex: 1;
}

.status-badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 12px;
  font-size: 12px;
  font-weight: bold;
  margin-left: 10px;
  text-transform: uppercase;
}

.status-badge.success { background: #d4edda; color: #155724; }
.status-badge.failed { background: #f8d7da; color: #721c24; }
.status-badge.running { background: #cce5ff; color: #004085; }

.error-msg {
  color: #dc3545;
  font-size: 0.9em;
  margin-top: 4px;
}

.log-btn {
  background-color: #6c757d;
  color: white;
  border: none;
  padding: 8px 12px;
  border-radius: 4px;
  cursor: pointer;
}
.log-btn:hover { background-color: #5a6268; }

/* MODAL STYLES */
.modal-overlay {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
}

.modal-content {
  background: white;
  width: 80%;
  height: 80%;
  border-radius: 8px;
  display: flex;
  flex-direction: column;
  box-shadow: 0 5px 15px rgba(0,0,0,0.3);
}

.modal-header {
  padding: 15px;
  border-bottom: 1px solid #eee;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.modal-body {
  flex: 1;
  padding: 0;
  overflow: hidden;
  background: #1e1e1e; /* Sötét háttér a lognak */
}

.log-viewer {
  width: 100%;
  height: 100%;
  margin: 0;
  padding: 15px;
  overflow: auto;
  color: #00ff00; /* Hacker zöld betűk, vagy legyen fehér #fff */
  font-family: 'Courier New', Courier, monospace;
  font-size: 13px;
  white-space: pre-wrap; /* Hosszú sorok tördelése */
}

.close-btn {
  background: none;
  border: none;
  font-size: 24px;
  cursor: pointer;
}
</style>