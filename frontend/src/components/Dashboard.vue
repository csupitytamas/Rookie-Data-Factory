<template>
  <div class="dashboard">
    <h2>OVERVIEW DASHBOARD</h2>
    
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

          <div class="preview-section">
            <div v-if="pipeline.sampleData && pipeline.sampleData.length > 0" class="table-preview-container">
              <table>
                <thead>
                  <tr>
                    <th v-for="key in tableKeys(pipeline.sampleData[0])" :key="key">{{ key }}</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(row, index) in visibleRows(pipeline.sampleData)" :key="index">
                    <td v-for="key in tableKeys(row)" :key="key" :data-label="key">
                      <span v-if="row[key] === null" class="null-value">NULL</span>
                      <span v-else>{{ row[key] }}</span>
                    </td>
                  </tr>
                </tbody>
              </table>
              <div class="table-actions">
                <button class="ellipsis-btn" @click.stop="openFullDataModal(pipeline)" title="Show ALL data in modal">
                  ➕
                </button>
              </div>
            </div>
            <div v-else class="no-data-msg">
              <em>No sample data available for this pipeline.</em>
            </div>
          </div>
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

const MAX_PREVIEW_ROWS = 11;

export default {
  name: "DashboardView",
  data() {
    return {
      pipelines: [],
      expandedId: null,
      showModal: false,
      modalPipeline: null,
      modalData: [], 
      refreshIntervalId: null,
      userTimezone: 'Europe/Budapest'
    };
  },
  async mounted() {
    await this.fetchSettings(); 
    await this.fetchPipelines();
    this.refreshIntervalId = setInterval(this.fetchPipelines, 5000);
  },
  unmounted() {
    if (this.refreshIntervalId) {
      clearInterval(this.refreshIntervalId);
    }
  },
  methods: {
    async fetchSettings() {
      try {
        const response = await axios.get('http://localhost:8000/etl/settings/');
        this.userTimezone = response.data?.timezone || 'Europe/Budapest';
      } catch (err) {
        console.error("Failed to load settings:", err);
      }
    },
    async fetchPipelines() {
      try {
        const response = await getDashboardPipelines();
        this.pipelines = response.data;
      } catch (err) {
        console.error("Failed to load:", err);
      }
    },
    handleTileClick(pipeline) {
      if (this.expandedId === pipeline.id) return;
      this.expandedId = pipeline.id;
    },
    getStatusClass(status) {
      return {
        'status-success': status === 'success',
        'status-failed': status === 'failed',
        'status-running': status === 'running',
        'status-queued': status === 'queued'
      };
    },
    formatDate(dateString) {
      if (!dateString || dateString === 'N/A' || dateString === '-') return '-';
      try {
        let date;
        if (typeof dateString === 'string' && !dateString.includes('Z') && !dateString.includes('+')) {
          date = new Date(dateString + 'Z');
        } else {
          date = new Date(dateString);
        }
        return date.toLocaleString('hu-HU', {
          year: 'numeric', month: '2-digit', day: '2-digit',
          hour: '2-digit', minute: '2-digit',
          timeZone: this.userTimezone
        });
      } catch (e) {
        return dateString;
      }
    },
    visibleRows(sampleData) {
      return (sampleData || []).slice(0, MAX_PREVIEW_ROWS);
    },
    tableKeys(row) {
      if (!row) return [];
      return Object.keys(row).filter(k => k !== "id");
    },
    async openFullDataModal(pipeline) {
      this.modalPipeline = pipeline;
      this.showModal = true;
      this.modalData = null; 
      try {
        const response = await getPipelineData(pipeline.id);
        this.modalData = Object.freeze(response.data.data);
      } catch (err) {
        console.error("Error:", err);
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

<style src="./styles/Dashboard.css"></style>
