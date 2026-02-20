<template>
  <div class="form-layout">
    <h3>Schedule & Conditions</h3>
    
    <div class="content-width">
      
      <div class="settings-box">
        <div class="form-row">
          <label>Schedule Frequency:</label>
          <select v-model="store.config.schedule" class="form-control">
            <option value="daily">Daily</option>
            <option value="weekly">Weekly</option>
            <option value="monthly">Monthly</option>
            <option value="hourly">Hourly</option>
            <option value="once">Once</option>
            <option value="custom">Custom</option>
          </select>
        </div>

        <div v-if="store.config.schedule === 'custom'" class="time-picker-container transition-fade">
          <label class="time-label">Execution Time (HH:MM):</label>
          <input type="time" v-model="store.config.custom_time" class="time-input-large" />
        </div>
      </div>

      <div class="settings-box mt-3">
        <div class="form-row">
          <label>Additional sources</label>
          <select v-model="store.config.condition" class="form-control">
            <option value="none">None</option>
            <option value="withsource">With a source file</option>
            <option value="withdependency">With a job</option>
          </select>
        </div>

        <div v-if="store.config.condition === 'withdependency'" class="form-row mt-3">
          <label>Dependency Job:</label>
          <select v-model="store.config.dependency_pipeline_id" class="form-control">
            <option disabled :value="null">Please select</option>
            <option 
              v-for="pipeline in availablePipelines" 
              :key="pipeline.id" 
              :value="pipeline.id"
            >
              {{ pipeline.pipeline_name }}
            </option>
          </select>
          <small v-if="loadingColumns" class="status-text text-blue mt-1">Loading columns...</small>
        </div>

        <div v-if="store.config.condition === 'withsource'" class="form-row mt-3">
          <label>Upload Source File:</label>
          
          <div class="file-upload-wrapper">
            <input 
              type="file" 
              id="custom-file-upload" 
              class="hidden-file-input"
              @change="handleFileUpload" 
              accept=".csv,.json,.parquet"
              :disabled="isUploading"
            />
            
            <label for="custom-file-upload" class="file-upload-box" :class="{ 'uploading': isUploading }">
              <span v-if="!isUploading && !store.config.uploaded_file_name">
                <strong>Click here</strong> to upload a file (CSV, JSON, Parquet)
              </span>
              <span v-else-if="isUploading">
                Uploading and analyzing... please wait.
              </span>
              <span v-else-if="store.config.uploaded_file_name">
                Change file
              </span>
            </label>
          </div>
          
          <div class="upload-status">
            <span v-if="uploadError" class="status-text text-red">
              ❌ {{ uploadError }}
            </span>
            <span v-else-if="store.config.uploaded_file_name && !isUploading" class="status-text text-green">
              ✅ <strong>File Ready:</strong> {{ store.config.uploaded_file_name }} <br>
              <small>({{ columnCount }} columns found)</small>
            </span>
          </div>

        </div>
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

      store.config.column_order = []; 

      const response = await getPipelineColumns(newId);
      
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

<style scoped>
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

.form-control {
  width: 100%;
  padding: 10px;
  border: 1px solid #ccc;
  border-radius: 4px;
  font-size: 1em;
  font-family: inherit;
  background-color: #fff;
}

.settings-box {
  background: #f9f9f9;
  border: 1px solid #e0e0e0;
  padding: 20px;
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.02);
}

.mt-3 {
  margin-top: 20px;
}

/* --- ÚJ: KIEMELT IDŐVÁLASZTÓ STÍLUSOK --- */
.time-picker-container {
  display: flex;
  flex-direction: column;
  align-items: center;     
  justify-content: center;
  margin-top: 10px;
  padding: 10px;
  background-color: #f0f8ff; 
  border-radius: 8px;
  border: 1px dashed #b8daff;
}

.time-label {
  color: #0056b3;
  font-size: 1em;
  margin-bottom: 10px;
}

.time-input-large {
  font-size: 1.2em;        
  font-weight: bold;       
  padding: 10px 20px;
  color: #333;
  border: 2px solid #007bff; 
  border-radius: 6px;
  background-color: #fff;
  text-align: center;      
  cursor: pointer;
  outline: none;
  transition: all 0.2s ease-in-out;
}

.time-input-large:hover,
.time-input-large:focus {
  box-shadow: 0 0 10px rgba(0, 123, 255, 0.3);
  border-color: #0056b3;
}

/* --- FÁJLFELTÖLTŐ STÍLUSOK --- */
.file-upload-wrapper {
  position: relative;
  width: 100%;
  margin-top: 5px;
}

.hidden-file-input {
  width: 0.1px;
  height: 0.1px;
  opacity: 0;
  overflow: hidden;
  position: absolute;
  z-index: -1;
}

.file-upload-box {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 25px 20px;
  background-color: #fcfcfc;
  border: 2px dashed #007bff;
  border-radius: 8px;
  color: #555;
  cursor: pointer;
  transition: all 0.2s ease-in-out;
}

.file-upload-box:hover {
  background-color: #f0f8ff;
  border-color: #0056b3;
}

.file-upload-box.uploading {
  border-color: #adb5bd;
  background-color: #e9ecef;
  cursor: not-allowed;
  opacity: 0.8;
}

.upload-icon {
  font-size: 2.5em;
  margin-bottom: 10px;
  opacity: 0.8;
}

.upload-status {
  margin-top: 15px;
  text-align: center;
  min-height: 25px; 
}

.status-text {
  display: inline-block;
  padding: 8px 15px;
  border-radius: 4px;
  font-size: 0.9em;
}

.text-blue { color: #004085; background-color: #cce5ff; }
.text-red { color: #721c24; background-color: #f8d7da; border: 1px solid #f5c6cb; }
.text-green { color: #155724; background-color: #d4edda; border: 1px solid #c3e6cb; }

/* Animációk */
.transition-fade {
  animation: fadeIn 0.3s ease-in-out;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(-5px); }
  to { opacity: 1; transform: translateY(0); }
}
</style>