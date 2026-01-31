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
        <option value="withsource">With source file (Merge Data)</option>
        <option value="withdependency">Wait for another pipeline</option>
      </select>
    </div>

    <div v-if="store.config.condition === 'withdependency'" class="form-row">
      <label>Dependency ID:</label>
      <input type="text" v-model="store.config.dependency_pipeline_id" placeholder="Pipeline ID" />
    </div>

    <div v-if="store.config.condition === 'withsource'" class="form-row">
      <label>Upload source file:</label>
      <div style="display:flex; flex-direction:column; gap:5px;">
        <div style="display:flex; gap:10px; align-items:center;">
          <input 
            type="file" 
            @change="handleFileUpload" 
            accept=".csv,.json,.parquet"
            :disabled="isUploading"
          />
        </div>
        
        <span v-if="isUploading" style="font-size:0.9em; color:blue;">
          Uploading and analyzing...
        </span>
        <span v-else-if="uploadError" style="font-size:0.9em; color:red;">
          {{ uploadError }}
        </span>
        <span v-else-if="store.config.uploaded_file_name" style="font-size:0.9em; color:green;">
          ✅ Ready: {{ store.config.uploaded_file_name }} ({{ columnCount }} columns found)
        </span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue';
import { usePipelineStore } from '@/stores/pipelineStore';
import axios from '@/api/axios'; // Ensure this points to your configured Axios instance
import { uploadExtraFile } from '@/api/pipeline';

const store = usePipelineStore();
const isUploading = ref(false);
const uploadError = ref('');

// Initialize defaults if missing
if(!store.config.condition) store.config.condition = "none";
if(!store.config.schedule) store.config.schedule = "daily";
// Initialize parameters object if missing
if (!store.config.parameters) store.config.parameters = {};

// Computed property to show column count just for UI feedback
const columnCount = computed(() => {
  return store.config.parameters.extra_file_columns?.length || 0;
});

const handleFileUpload = async (event: any) => {
  const file = event.target.files?.[0];
  if (!file) return;

  isUploading.value = true;
  uploadError.value = '';
  
  // Update UI immediately
  store.config.uploaded_file_name = file.name;

  const formData = new FormData();
  formData.append('file', file);

  try {
    // Send to the new backend endpoint
  const response = await uploadExtraFile(formData);

    if (response.data.error) {
      throw new Error(response.data.error);
    }

    // Save the returned path and columns into the store parameters
    // This makes them available for Step 4 (Mapping) and Step 6 (Submit)
    store.config.parameters = {
      ...store.config.parameters,
      extra_file_path: response.data.file_path,
      extra_file_columns: response.data.columns
    };

  } catch (err: any) {
    console.error(err);
    uploadError.value = "Upload failed: " + (err.response?.data?.detail || err.message);
    store.config.uploaded_file_name = ''; // Reset on failure
  } finally {
    isUploading.value = false;
  }
};
</script>

<style scoped src="../styles/CreateETLPipeline.style.css"></style>