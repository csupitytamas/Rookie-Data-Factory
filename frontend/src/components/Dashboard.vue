<template>
  <div class="dashboard">
    <h2>📊 Dashboard</h2>
    <div class="pipeline" v-for="pipeline in pipelines" :key="pipeline.id">
      <h3>{{ pipeline.name }}</h3>
      <p>Last successful run: <strong>{{ pipeline.lastRun }}</strong></p>
      <p>
        Status:
        <strong :class="{
          'status-success': pipeline.status === 'success',
          'status-failed': pipeline.status === 'failed',
          'status-running': pipeline.status === 'running'
        }">
          {{ pipeline.status }}
        </strong>
      </p>
      <p>Next scheduled run: <strong>{{ pipeline.nextRun }}</strong></p>
      <p>Source: <strong>{{ pipeline.alias }}</strong></p>
      <div class="table-preview" v-if="pipeline.sampleData && pipeline.sampleData.length > 0">
        <table>
          <thead>
            <tr>
              <th v-for="key in tableKeys(pipeline.sampleData[0])" :key="key">{{ key }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, index) in visibleRows(pipeline.sampleData)" :key="index">
              <td v-for="key in tableKeys(row)" :key="key" :data-label="key">{{ row[key] }}</td>
            </tr>
          </tbody>
        </table>
        <div v-if="hasMoreRows(pipeline.sampleData)" class="table-ellipsis">
          <button class="ellipsis-btn" @click="openModal(pipeline)" title="Show all data">
            ...
          </button>
        </div>
      </div>
      <div v-else>
        <em> No data to display.</em>
      </div>
    </div>

    <div v-if="showModal && modalPipeline" class="modal-overlay" @click.self="closeModal">
      <div class="modal-content">
        <h2>{{ modalPipeline.name }}</h2>
        
        <div style="overflow-x:auto; max-height:60vh; overflow-y:auto;">
          <table v-if="modalData && modalData.length > 0">
            <thead>
              <tr>
                <th v-for="key in tableKeys(modalData[0])" :key="key">{{ key }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, idx) in modalData" :key="idx">
                <td v-for="key in tableKeys(row)" :key="key">{{ row[key] }}</td>
              </tr>
            </tbody>
          </table>
          <div v-else>
            <em v-if="!modalData">Loading...</em>
            <em v-else>No data available.</em>
          </div>
        </div>
        <button class="modal-close-btn" @click="closeModal">Close</button>
      </div>
    </div>
  </div>
</template>

<script>
// 🟢 JAVÍTÁS: Importáljuk a getPipelineData-t
import { getDashboardPipelines, getPipelineData } from "@/api/dashboard";

const MAX_ROWS = 10;

export default {
  name: "DashboardView",
  data() {
    return {
      pipelines: [],
      showModal: false,
      modalPipeline: null,
      modalData: [], // Itt tároljuk a teljes adatot
      refreshIntervalId: null
    };
  },
  async mounted() {
    await this.fetchPipelines();
    this.refreshIntervalId = setInterval(this.fetchPipelines, 5000);
  },
  unmounted() {
    if (this.refreshIntervalId) {
      clearInterval(this.refreshIntervalId);
      this.refreshIntervalId = null;
    }
  },
  methods: {
    async fetchPipelines() {
      try {
        const response = await getDashboardPipelines();
        this.pipelines = response.data;
      } catch (err) {
        console.error("Hiba a dashboard adatok betöltésekor:", err);
      }
    },
    visibleRows(sampleData) {
      return sampleData.slice(0, MAX_ROWS);
    },
    hasMoreRows(sampleData) {
      return sampleData.length > MAX_ROWS;
    },
    tableKeys(row) {
      if (!row) return [];
      return Object.keys(row).filter(k => k !== "id");
    },
    
    // 🟢 MÓDOSÍTOTT OPENMODAL
    async openModal(pipeline) {
      this.modalPipeline = pipeline;
      this.showModal = true;
      this.modalData = null; // Töröljük, hogy látszódjon a töltés

      try {
        // Lekérjük a teljes adatot a backendről
        const response = await getPipelineData(pipeline.id);
        
        // 🚀 Object.freeze: Kötelező 25k sornál, különben a böngésző megáll
        this.modalData = Object.freeze(response.data.data);
      } catch (err) {
        console.error("Hiba:", err);
        this.modalData = [];
      }
    },
    
    closeModal() {
      this.showModal = false;
      this.modalPipeline = null;
      this.modalData = [];
    }
  }
};
</script>

<style scoped>
.pipeline {
  background: linear-gradient(135deg, #f8fafc 0%, #e9eff6 100%);
  margin: 32px 0;
  padding: 28px 22px;
  border-radius: 14px;
  border: 1.5px solid #b2c1da;
  box-shadow: 0 2px 12px rgba(3, 26, 73, 0.08), 0 1.5px 0.5px rgba(0,0,0,0.02);
  transition: box-shadow 0.2s;
}
.pipeline:hover {
  box-shadow: 0 6px 28px rgba(3, 26, 73, 0.13);
}
.table-preview {
  overflow-x: auto;
  border-radius: 8px;
  background: #fff;
  max-width: 100%;
}
table {
  width: 100%;
  border-collapse: collapse;
}
th, td {
  border: 1px solid #ccc;
  padding: 8px;
  text-align: left;
  min-width: 120px;
}
@media (max-width: 768px) {
  table, thead, tbody, th, td, tr {
    display: block;
  }
  thead tr {
    display: none;
  }
  td {
    position: relative;
    padding-left: 50%;
    border: none;
    border-bottom: 1px solid #ccc;
  }
  td::before {
    position: absolute;
    top: 8px;
    left: 10px;
    width: 45%;
    white-space: nowrap;
    font-weight: bold;
    content: attr(data-label);
  }
}

.table-ellipsis {
  text-align: center;
  font-size: 0.5rem;
  color: #031a49;
  font-weight: bold;
}
.status-success { color: green; }
.status-failed { color: red; }
.status-running { color: orange; }

.modal-overlay {
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.3);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}
.modal-content {
  background: #fff;
  border-radius: 16px;
  box-shadow: 0 8px 48px #031a4925;
  padding: 2em;
  min-width: 600px;
  max-width: 90vw;
  max-height: 90vh;
  overflow-y: auto;
  position: relative;
}
.modal-close-btn {
  display: block;
  margin: 1em auto 0 auto;
  padding: 8px 24px;
  font-size: 1.1em;
  border-radius: 6px;
  border: none;
  background: #e9eff6;
  cursor: pointer;
  transition: background .2s;
}
.modal-close-btn:hover {
  background: #b2c1da;
}
.ellipsis-btn {
  background: #f3f6fb;
  border: 1.5px solid #b2c1da;
  border-radius: 8px;
  font-size: 1rem;
  font-weight: bold;
  color: #46597a;
  padding: 4px 16px;
  cursor: pointer;
  box-shadow: 0 1px 4px rgba(3, 26, 73, 0.05);
  transition: background .18s, border-color .18s, color .18s;
  margin: 0 auto;
  display: inline-block;
}
.ellipsis-btn:hover {
  background: #e9eff6;
  border-color: #7a95b8;
  color: #284363;
}
</style>