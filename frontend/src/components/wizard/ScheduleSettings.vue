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
        <option value="withdependency">With another dataset</option>
      </select>
    </div>

    <div v-if="store.config.condition === 'withdependency'" class="form-row">
      <label>Select from the active Jobs </label>
      <select v-model="store.config.dependency_pipeline_id" class="pipeline-select">
        <option disabled :value="null">-- Please select --</option>
        <option 
          v-for="pipeline in availablePipelines" 
          :key="pipeline.id" 
          :value="pipeline.id"
        >
          {{ pipeline.pipeline_name }}
        </option>
      </select>
      <small v-if="loadingColumns" style="color: blue;">Loading columns...</small>
      <small v-else-if="store.config.parameters?.dependency_columns?.length" style="color: green;">
        ✅ Columns loaded successfully ({{ store.config.parameters.dependency_columns.length }})
      </small>
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
import { ref, computed, onMounted, watch } from 'vue';
import { usePipelineStore } from '@/stores/pipelineStore';
import { uploadExtraFile, getAllPipelines, getPipelineColumns } from '@/api/pipeline';

const store = usePipelineStore();
const isUploading = ref(false);
const uploadError = ref('');
const loadingColumns = ref(false);

const availablePipelines = ref<any[]>([]);

if(!store.config.condition) store.config.condition = "none";
if(!store.config.schedule) store.config.schedule = "daily";
if (!store.config.parameters) store.config.parameters = {};

onMounted(async () => {
  try {
    const response = await getAllPipelines();
    const allPipelines = response.data || [];
    
    availablePipelines.value = allPipelines.filter((p: any) => {
      return p.id !== store.config.id;
    });
    
  } catch (error) {
    console.error("Failed to load pipelines for dropdown:", error);
  }
});

// --- ITT A JAVÍTOTT WATCH LOGIKA ---
watch(
  () => store.config.dependency_pipeline_id,
  async (newId) => {
    if (!newId) {
      if (store.config.parameters) {
        store.config.parameters.dependency_columns = [];
      }
      return;
    }

    loadingColumns.value = true;
    try {
      console.log(`Pipeline selected (${newId}). Fetching columns...`);

      // 1. KULCS FONTOSSÁGÚ SOR: Töröljük a cache-t!
      // Ezzel kényszerítjük a FieldMapping oldalt, hogy újratöltse az adatokat.
      store.config.column_order = []; 

      // 2. Oszlopok lekérése
      const response = await getPipelineColumns(newId);
      
      // 3. Mentés a Store-ba
      store.config.parameters = {
        ...store.config.parameters,
        dependency_columns: response.data || [] 
      };
      
      console.log("Dependency columns saved:", response.data);
      
    } catch (error) {
      console.error("Failed to fetch dependency columns:", error);
      store.config.parameters.dependency_columns = [];
    } finally {
      loadingColumns.value = false;
    }
  }
);

const columnCount = computed(() => {
  return store.config.parameters.extra_file_columns?.length || 0;
});

const handleFileUpload = async (event: any) => {
  const file = event.target.files?.[0];
  if (!file) return;

  isUploading.value = true;
  uploadError.value = '';
  
  store.config.uploaded_file_name = file.name;

  const formData = new FormData();
  formData.append('file', file);

  try {
    const response = await uploadExtraFile(formData);

    if (response.data.error) {
      throw new Error(response.data.error);
    }

    store.config.parameters = {
      ...store.config.parameters,
      extra_file_path: response.data.file_path,
      extra_file_columns: response.data.columns
    };

  } catch (err: any) {
    console.error(err);
    uploadError.value = "Upload failed: " + (err.response?.data?.detail || err.message);
    store.config.uploaded_file_name = ''; 
  } finally {
    isUploading.value = false;
  }
};
</script>

<style scoped src="../styles/CreateETLPipeline.style.css"></style>

<style scoped>
.pipeline-select {
  width: 100%;
  padding: 8px;
  border: 1px solid #ccc;
  border-radius: 4px;
  background-color: white;
}
</style>