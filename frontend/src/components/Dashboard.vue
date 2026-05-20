<!-- A fájl a dashboard felületéért felelős, ahol a pipeline-ok állapota és előnézete látható. -->
<template>
  <div class="dashboard">
    <div class="dashboard-header">
      <h2>OVERVIEW DASHBOARD</h2>
      <div class="system-status" v-if="airflowStatus && showAirflowStatus">
        <span class="status-label">Airflow:</span>
        <div :class="['status-indicator', airflowStatus.status]">
          <span class="status-dot"></span>
          <span class="status-text">{{ airflowStatusText }}</span>
        </div>
      </div>
    </div>
    
    <div class="pipeline-grid">
      <div 
        v-for="pipeline in pipelines" 
        :key="pipeline.id"
        :class="['pipeline-tile', { 'is-expanded': expandedId === pipeline.id }]"
        @click="handleTileClick(pipeline)"
      >
        <!-- Close button (Always in top-right when expanded) -->
        <button v-if="expandedId === pipeline.id" class="close-tile-btn" @click.stop="expandedId = null" title="Collapse">✕</button>

        <!-- MINI VIEW (Header part) -->
        <div class="pipeline-info-tile mini">
          <div class="tile-header">
            <h3>{{ pipeline.name }}</h3>
            <span v-if="expandedId !== pipeline.id" :class="['status-dot', getStatusClass(pipeline.status)]"></span>
          </div>
          <div v-if="expandedId !== pipeline.id" class="mini-meta">
            <p>Status: <strong :class="getStatusClass(pipeline.status)">{{ pipeline.status }}</strong></p>
            <p>Last run: <strong>{{ formatDate(pipeline.lastRun) }}</strong></p>
          </div>
        </div>

        <!-- EXPANDED CONTENT -->
        <div v-if="expandedId === pipeline.id" class="expanded-content transition-fade">
          <div class="info-grid secondary-info">
            <p>Last update: <strong>{{ formatDate(pipeline.lastRun) }}</strong></p>
            <p>Next update: <strong>{{ formatDate(pipeline.nextRun) }}</strong></p>
            <p>Status: <strong :class="getStatusClass(pipeline.status)">{{ pipeline.status }}</strong></p>
            <p>Source: <strong>{{ pipeline.alias }}</strong></p>
          </div>

          <!-- POWER BI LINK -->
          <div class="powerbi-link-container">
            <button 
              class="copy-powerbi-btn" 
              @click.stop="copyPowerBiLink(pipeline.id)"
              title="Copy this URL to use as a Web Data Source in Power BI Desktop"
            >
              🔗 PowerBI source link
            </button>
            <span v-if="copiedId === pipeline.id" class="copy-success-msg">Copied!</span>
          </div>

          <div class="preview-section">
            <div v-if="pipeline.sampleData && pipeline.sampleData.length > 0">
              <div class="table-preview-container">
                <table>
                  <thead>
                    <tr>
                      <th v-for="key in tableKeys(pipeline.sampleData[0])" :key="key">{{ key }}</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="(row, index) in visibleRows(pipeline.sampleData)" :key="index">
                      <td v-for="key in tableKeys(row)" :key="key" :data-label="key">
                        <span v-if="isNull(row[key])" class="null-value">NULL</span>
                        <span v-else>{{ row[key] }}</span>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <div class="table-actions">
                <button class="ellipsis-btn" @click.stop="openFullDataModal(pipeline)" title="Show ALL data in modal">
                  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
                    <polyline points="15 3 21 3 21 9"></polyline>
                    <polyline points="9 21 3 21 3 15"></polyline>
                    <line x1="21" y1="3" x2="14" y2="9"></line>
                    <line x1="3" y1="21" x2="10" y2="15"></line>
                  </svg>
                </button>
              </div>
            </div>
            <div v-else class="no-data-msg">
              <em>No sample data available for this pipeline.</em>
            </div>
          </div>

          <!-- AIRFLOW VISUALIZATION -->
          <AirflowGrid :pipelineId="pipeline.id" />
        </div>
      </div>
    </div>

    <!-- FULL DATA MODAL -->
    <div v-if="showModal && modalPipeline" class="modal-overlay" @click.self="closeModal">
      <div class="expanded-pipeline-card">
        <button class="close-btn-top" @click="closeModal">✕</button>
        <h2>Full Dataset: {{ modalPipeline.name }}</h2>

        <div class="full-data-section">
          <div v-if="modalData && modalData.length > 0" class="table-container">
            <table>
              <thead>
                <tr>
                  <th v-for="key in tableKeys(modalData[0])" :key="key">{{ key }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, idx) in modalData" :key="idx">
                  <td v-for="key in tableKeys(row)" :key="key">
                    <span v-if="row[key] === null" class="null-value">NULL</span>
                    <span v-else>{{ row[key] }}</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-else class="modal-loading">
            <em v-if="modalData === null">Loading full dataset...</em>
            <em v-else>No data available.</em>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { getDashboardPipelines, getPipelineData } from "@/api/dashboard";
import axios from "axios";
import AirflowGrid from "./AirflowGrid.vue";

// A maximálisan megjeleníthető sorok száma az előnézeti táblázatban.
const MAX_PREVIEW_ROWS = 11;

export default {
  name: "DashboardView",

  // Beágyazott komponensek regisztrálása.
  components: {
    AirflowGrid
  },

  // A komponens reaktív állapota: pipeline adatok, nyitott panelek, modal állapot, időzítők és státuszok.
  data() {
    return {
      pipelines: [],
      expandedId: null,
      showModal: false,
      modalPipeline: null,
      modalData: [], 
      refreshIntervalId: null,
      userTimezone: 'Europe/Budapest',
      copiedId: null,
      airflowStatus: null,
      showAirflowStatus: true
    };
  },

  // Figyeljük az Airflow státusz szövegének változását a UI frissítéséhez.
  watch: {
    airflowStatusText(newVal) {

      // Ha az Airflow készen áll (Ready), egy rövid késleltetés után elrejtjük a státuszjelzőt.
      if (newVal === 'Ready') {
        setTimeout(() => {
          this.showAirflowStatus = false;
        }, 2000);
      } else {
        this.showAirflowStatus = true;
      }
    }
  },

  // Számított tulajdonság az Airflow státusz emberi olvasásra alkalmas formájának előállításához.
  computed: {
    airflowStatusText() {
      if (!this.airflowStatus) return 'Loading...';
      if (this.airflowStatus.status === 'healthy') return 'Ready';
      return 'Loading...';
    }
  },

  // A komponens csatolásakor lekérjük a beállításokat, a pipeline-okat és az Airflow státuszt.
  async mounted() {
    await this.fetchSettings(); 
    await this.fetchPipelines();
    await this.fetchAirflowHealth();

    // Elindítunk egy 5 másodperces időzítőt az adatok és a státusz folyamatos frissítéséhez.
    this.refreshIntervalId = setInterval(() => {
      this.fetchPipelines();
      this.fetchAirflowHealth();
    }, 5000);
  },

  // A komponens megsemmisítésekor leállítjuk az időzítőt, hogy ne fusson feleslegesen a háttérben.
  unmounted() {
    if (this.refreshIntervalId) {
      clearInterval(this.refreshIntervalId);
    }
  },

  // Segédfüggvények a UI működéséhez és az adatlekérésekhez.
  methods: {
    isNull(val) {
      if (val === null || val === undefined) return true;
      if (typeof val === 'number' && isNaN(val)) return true;
      if (typeof val === 'string' && (val.toLowerCase() === 'nan' || val.trim() === '')) return true;
      return false;
    },

    // Lekérdezi az Airflow backend aktuális egészségi állapotát (health check).
    async fetchAirflowHealth() {

      // API hívás a /health végpontra.
      try {
        const response = await axios.get('http://localhost:8000/etl/health');
        this.airflowStatus = response.data;
      } catch (err) {
        console.error("Failed to check Airflow health:", err);
        this.airflowStatus = { status: 'unavailable' };
      }
    },

    // Lekéri a rendszerbeállításokat, különösen a felhasználó időzónáját a dátumok helyes formázásához.
    async fetchSettings() {

      // API hívás a /settings végpontra.
      try {
        const response = await axios.get('http://localhost:8000/etl/settings/');
        this.userTimezone = response.data?.timezone || 'Europe/Budapest';
      } catch (err) {
        console.error("Failed to load settings:", err);
      }
    },

    // Lekéri a dashboardon megjelenítendő összes pipeline összefoglaló adatait.
    async fetchPipelines() {

      // API hívás a dashboard végpontra, majd az eredmény elmentése a komponens állapotába.
      try {
        const response = await getDashboardPipelines();
        this.pipelines = response.data;
      } catch (err) {
        console.error("Failed to load:", err);
      }
    },

    // Kezeli a pipeline-ra történő kattintást, megnyitva a részletes nézetet.
    handleTileClick(pipeline) {
      if (this.expandedId === pipeline.id) return;
      this.expandedId = pipeline.id;
    },

    // Visszaadja a megfelelő megjelenítést a pipeline aktuális státusza alapján.
    getStatusClass(status) {
      return {
        'status-success': status === 'success',
        'status-failed': status === 'failed',
        'status-running': status === 'running',
        'status-queued': status === 'queued'
      };
    },

    // Formázza az UTC dátumsztringeket a felhasználó helyi időzónájának megfelelő formátumra.
    formatDate(dateString) {
      if (!dateString || dateString === 'N/A' || dateString === '-') return '-';
      try {
        let date;
        if (typeof dateString === 'string' && !dateString.includes('Z') && !dateString.includes('+')) {
          date = new Date(dateString + 'Z');
        } else {
          date = new Date(dateString);
        }

        // A helyi formátum összeállítása a fetchSettings() által beállított userTimezone alapján.
        return date.toLocaleString('en-US', {
          year: 'numeric', month: '2-digit', day: '2-digit',
          hour: '2-digit', minute: '2-digit',
          timeZone: this.userTimezone
        });
      } catch (e) {
        return dateString;
      }
    },

    // Visszaadja a mintadatok első néhány sorát az előnézeti táblázat számára.
    visibleRows(sampleData) {
      return (sampleData || []).slice(0, MAX_PREVIEW_ROWS);
    },

    // Kinyeri az oszlopneveket egy adatsorból a táblázat fejlécének generálásához, kihagyva az 'id'-t.
    tableKeys(row) {
      if (!row) return [];
      return Object.keys(row).filter(k => k !== "id");
    },

    // Megnyitja a modális ablakot és betölti az adott pipeline teljes adatkészletét.
    async openFullDataModal(pipeline) {
      this.modalPipeline = pipeline;
      this.showModal = true;
      this.modalData = null;

      // Nagy méretű adatkészletek esetén a Vue reaktivitásának kikapcsolása (Object.freeze) a teljesítmény érdekében.
      try {
        const response = await getPipelineData(pipeline.id);
        this.modalData = Object.freeze(response.data.data);
      } catch (err) {
        console.error("Error:", err);
        this.modalData = [];
      }
    },

    // Bezárja a modális ablakot és törli a memóriából a teljes adatkészletet.
    closeModal() {
      this.showModal = false;
      this.modalPipeline = null;
      this.modalData = [];
    },

    // Vágólapra másolja a PowerBI integrációhoz szükséges közvetlen API URL-t.
    copyPowerBiLink(pipelineId) {

      // Az URL összeállítása és másolása, majd  a "Copied!" üzenet megjelenítése.
      const url = `http://localhost:8000/etl/dashboard/pipeline/${pipelineId}/powerbi`;
      navigator.clipboard.writeText(url).then(() => {
        this.copiedId = pipelineId;
        setTimeout(() => {
          this.copiedId = null;
        }, 2000);
      }).catch(err => {
        console.error('Could not copy text: ', err);
      });
    }
  }
};
</script>

<style src="./styles/Dashboard.css"></style>
