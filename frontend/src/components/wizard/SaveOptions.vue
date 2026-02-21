<template>
  <div class="form-layout">
    <h3>Save Options</h3>

    <div class="content-width">
      <div class="settings-box">
        
        <div class="form-row">
          <div class="label-with-tooltip">
            <label>Update Mode:</label>
            <div class="tooltip-wrapper">
              <span class="hint-icon">💡</span>
              <div class="tooltip-content">
                <b>Append:</b> Adds new records to existing table.<br>
                <b>Overwrite:</b> Replaces the existing table.<br>
                <b>Upsert:</b> Updates existing records.
              </div>
            </div>
          </div>
          
          <select v-model="store.config.update_mode" class="form-control">
            <option value="append">Append</option>
            <option value="overwrite">Overwrite</option>
            <option value="upsert">Upsert</option>
          </select>
        </div>

        <div 
          v-if="store.config.update_mode === 'upsert' && !hasUniqueColumn" 
          class="error-banner transition-fade"
        >
          <span class="warning-icon">⚠️</span>
          <div class="error-text">
            <strong>No unique column selected!</strong><br>
            Upsert mode requires at least one column marked as <b>Unique</b> in the Field Mapping step to identify existing records.
          </div>
        </div>

        <hr class="separator" />

        <div class="form-row">
          <label>Save Destination:</label>
          <select v-model="store.config.save_option" class="form-control">
            <option value="todatabase">Database only</option>
            <option value="createfile">Create file too</option>
          </select>
        </div>

        <div v-if="store.config.save_option === 'createfile'" class="form-row mt-3 transition-fade">
          <label>File Format:</label>
          <select v-model="store.config.file_format" class="form-control">
            <option value="csv">CSV</option>
            <option value="json">JSON</option>
            <option value="excel">Excel</option>
            <option value="parquet">Parquet</option>
          </select>
        </div>

      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { usePipelineStore } from '@/stores/pipelineStore';

const store = usePipelineStore();

// Alapértelmezett értékek beállítása
if (!store.config.update_mode) store.config.update_mode = "append";
if (!store.config.save_option) store.config.save_option = "todatabase";
if (!store.config.file_format) store.config.file_format = "csv";

/**
 * Számított tulajdonság, ami ellenőrzi, hogy van-e legalább egy 
 * unique-nak jelölt oszlop a field_mappings-ben.
 */
const hasUniqueColumn = computed(() => {
  const mappings = store.config.field_mappings || {};
  // Megnézzük, hogy van-e olyan bejegyzés, ahol a unique értéke true
  return Object.values(mappings).some((m: any) => m.unique === true);
});
</script>

<style scoped>
/* KONZISZTENS DIZÁJN */
.form-layout {
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 100%;
}

.content-width {
  width: 70%;
}

h3 {
  margin-bottom: 20px;
}

.settings-box {
  background: #f9f9f9;
  border: 1px solid #e0e0e0;
  padding: 25px;
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.02);
}

.form-row {
  margin-bottom: 15px;
  text-align: left;
}

label {
  display: block;
  font-weight: 600;
  margin-bottom: 8px;
  color: #333;
}

/* ÚJ TOOLTIP STÍLUSOK */
.label-with-tooltip {
  display: flex;
  align-items: center;
  margin-bottom: 8px;
}

.label-with-tooltip label {
  margin-bottom: 0; /* A flex container rendezi, nem kell extra margó */
}

.tooltip-wrapper {
  position: relative;
  display: inline-flex;
  align-items: center;
  margin-left: 8px;
  cursor: help;
}

.hint-icon {
  font-size: 1.1em;
  opacity: 0.6;
  transition: opacity 0.2s, transform 0.2s;
}

.tooltip-wrapper:hover .hint-icon {
  opacity: 1;
  transform: scale(1.1);
}

.tooltip-content {
  visibility: hidden;
  width: 280px;
  background-color: #2c3e50; 
  color: #fff;
  text-align: left;
  border-radius: 6px;
  padding: 12px;
  position: absolute;
  z-index: 100;
  bottom: 150%;
  left: 50%;
  transform: translateX(-50%);
  opacity: 0;
  transition: opacity 0.3s, visibility 0.3s;
  font-size: 0.85rem;
  font-weight: normal;
  line-height: 1.4;
  box-shadow: 0 4px 10px rgba(0,0,0,0.2);
  pointer-events: none;
}

.tooltip-content::after {
  content: "";
  position: absolute;
  top: 100%;
  left: 50%;
  margin-left: -6px;
  border-width: 6px;
  border-style: solid;
  border-color: #2c3e50 transparent transparent transparent;
}

.tooltip-wrapper:hover .tooltip-content {
  visibility: visible;
  opacity: 1;
}
/* TOOLTIP VÉGE */

.form-control {
  width: 100%;
  padding: 10px;
  border: 1px solid #ccc;
  border-radius: 4px;
  font-size: 1em;
  background-color: #fff;
}

.separator {
  margin: 25px 0;
  border: 0;
  border-top: 1px solid #eee;
}

.mt-3 {
  margin-top: 20px;
}

/* PIROS FIGYELMEZTETŐ SÁV */
.error-banner {
  display: flex;
  align-items: flex-start;
  gap: 15px;
  background-color: #fff5f5;
  border: 1px solid #feb2b2;
  border-left: 5px solid #f56565;
  padding: 15px;
  border-radius: 6px;
  margin-top: 15px;
  color: #c53030;
}

.warning-icon {
  font-size: 1.4em;
  line-height: 1;
}

.error-text {
  font-size: 0.9em;
  line-height: 1.5;
}

.error-text b {
  text-decoration: underline;
}

/* Animáció */
.transition-fade {
  animation: fadeIn 0.3s ease-in-out;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(-5px); }
  to { opacity: 1; transform: translateY(0); }
}
</style>