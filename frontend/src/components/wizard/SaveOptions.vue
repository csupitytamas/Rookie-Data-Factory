<template>
  <div class="form-layout">
    <h3>Save Options</h3>
    <div class="form-row">
      <label>Update Mode:</label>
      <select v-model="store.config.update_mode">
        <option value="append">Append</option>
        <option value="overwrite">Overwrite</option>
        <option value="upsert">Upsert</option>
      </select>
    </div>

    <div class="form-row">
      <label>Save Destination:</label>
      <select v-model="store.config.save_option">
        <option value="todatabase">Database only</option>
        <option value="createfile">Create file too</option>
      </select>
    </div>

    <div v-if="store.config.save_option === 'createfile'" class="form-row">
      <label>File Format:</label>
      <select v-model="store.config.file_format">
        <option value="csv">CSV</option>
        <option value="json">JSON</option>
        <option value="excel">Excel</option>
        <option value="parquet">Parquet</option>
      </select>
    </div>
  </div>
</template>

<script setup lang="ts">
import { usePipelineStore } from '@/stores/pipelineStore';
const store = usePipelineStore();

if(!store.config.update_mode) store.config.update_mode = "append";
if(!store.config.save_option) store.config.save_option = "todatabase";
</script>
<style scoped src="../styles/CreateETLPipeline.style.css"></style>