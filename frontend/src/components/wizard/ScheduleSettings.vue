<template>
  <div class="form-layout">
    <h3>Schedule & Conditions</h3>
    
    <div class="form-row">
      <label>Schedule:</label>
      <select v-model="store.config.schedule">
        <option value="daily">Daily (@daily)</option>
        <option value="weekly">Weekly (@weekly)</option>
        <option value="monthly">Monthly (@monthly)</option>
        <option value="hourly">Hourly (@hourly)</option>
        <option value="once">Once</option>
        <option value="custom">Custom (Cron)</option>
      </select>
    </div>

    <div v-if="store.config.schedule === 'custom'" class="form-row">
      <label>Time (HH:MM):</label>
      <input type="time" v-model="store.config.custom_time" />
    </div>

    <div class="form-row">
      <label>Running Condition:</label>
      <select v-model="store.config.condition">
        <option value="none">None</option>
        <option value="withsource">With source file</option>
        <option value="withdependency">Wait for another pipeline</option>
      </select>
    </div>

    <div v-if="store.config.condition === 'withdependency'" class="form-row">
      <label>Dependency ID:</label>
      <input type="text" v-model="store.config.dependency_pipeline_id" placeholder="Pipeline ID" />
    </div>

    <div v-if="store.config.condition === 'withsource'" class="form-row">
      <label>Upload source file:</label>
      <div style="display:flex; gap:10px; align-items:center;">
        <input type="file" @change="handleFileUpload" />
        <span v-if="store.config.uploaded_file_name" style="font-size:0.9em; color:green;">
           Selected: {{ store.config.uploaded_file_name }}
        </span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { usePipelineStore } from '@/stores/pipelineStore';

const store = usePipelineStore();

if(!store.config.condition) store.config.condition = "none";
if(!store.config.schedule) store.config.schedule = "daily";

const handleFileUpload = (event: any) => {
  const file = event.target.files?.[0];
  if (file) {
    store.config.uploaded_file_name = file.name;
  }
};
</script>
<style scoped src="../styles/CreateETLPipeline.style.css"></style>