<template>
  <div class="form-layout">
    <h3>Field Mapping</h3>
    
    <p v-if="loading" class="loading-text">Loading fields...</p>
    
    <draggable 
      v-else
      v-model="store.config.column_order" 
      item-key="col" 
      class="draggable-list"
      handle=".drag-handle"
      ghost-class="ghost-row"
    >
      <template #item="{ element: col, index }">
        <div class="mapping-row">
          <div class="mapping-header">
            
            <div class="drag-handle" title="Drag to reorder">
              <svg viewBox="0 0 24 24" width="20" height="20" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="9" cy="12" r="1.5"></circle>
                <circle cx="9" cy="5" r="1.5"></circle>
                <circle cx="9" cy="19" r="1.5"></circle>
                <circle cx="15" cy="12" r="1.5"></circle>
                <circle cx="15" cy="5" r="1.5"></circle>
                <circle cx="15" cy="19" r="1.5"></circle>
              </svg>
            </div>

            <span class="row-number">{{ index + 1 }}.</span>
            
            <span class="column-name">{{ col }}</span>
            <button class="settings-btn" @click.prevent="toggleSettings(col)" title="Settings">⚙️</button>
          </div>

          <div v-if="settingsOpen[col]" class="mapping-settings">
            <div class="setting-item">
               <label><input type="checkbox" v-model="getMapping(col).rename" /> Rename</label>
               <input v-if="getMapping(col).rename" v-model="getMapping(col).newName" placeholder="New name" class="small-input" />
            </div>
            
            <div class="setting-item">
              <label><input type="checkbox" v-model="getMapping(col).unique" /> Unique</label>
            </div>
            
            <div class="setting-item">
              <label><input type="checkbox" v-model="getMapping(col).delete" /> Delete</label>
            </div>
          </div>
        </div>
      </template>
    </draggable>

    <div v-if="!loading && (!store.config.column_order || store.config.column_order.length === 0)">
       <p style="color:red; text-align: center; margin-top: 20px;">Failed to load fields for this source.</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue';
import draggable from 'vuedraggable';
import { usePipelineStore } from '@/stores/pipelineStore';
import { loadSchemaBySource, getPipelineColumns } from '@/api/pipeline';

const store = usePipelineStore();
const loading = ref(false);
const settingsOpen = ref<Record<string, boolean>>({});

const getMapping = (col: string) => {
  if (!store.config.field_mappings[col]) {
    store.config.field_mappings[col] = { rename: false, newName: "", unique: false, delete: false, concat: { enabled: false } };
  }
  return store.config.field_mappings[col];
};

const toggleSettings = (col: string) => {
  settingsOpen.value[col] = !settingsOpen.value[col];
};

onMounted(async () => {
  if (store.config.column_order && store.config.column_order.length > 0) {
      return; 
  }
  loading.value = true;
  try {
      let apiCols: string[] = [];
      if (store.source && store.source !== 'Pipeline') {
        try {
          const payload = {
            source: store.source,
            parameters: store.config.parameters || {} 
          };
          console.log("Loading API schema...");
          const resp = await loadSchemaBySource(payload);
          apiCols = (resp.data.field_mappings || []).map((f: any) => f.name);
        } catch (e) {
          console.warn("API schema load skipped or failed:", e);
        }
      }
      const fileCols = store.config.parameters?.extra_file_columns || [];
      let depCols: string[] = [];
      if (store.config.parameters?.dependency_columns) {
         depCols = store.config.parameters.dependency_columns;
      }
      if (depCols.length === 0 && store.config.dependency_pipeline_id) {
         try {
             console.log(`Fetching columns for dependency pipeline: ${store.config.dependency_pipeline_id}`);
             const res = await getPipelineColumns(store.config.dependency_pipeline_id);
             if (Array.isArray(res.data)) {
                 depCols = res.data;
             } else if (res.data.columns) {
                 depCols = res.data.columns;
             }
         } catch (e) {
             console.error("Failed to load dependency columns:", e);
         }
      }
      
      console.log(`API Cols: ${apiCols.length}, File Cols: ${fileCols.length}, Dep Cols: ${depCols.length}`);
      const allCols = [...new Set([...apiCols, ...fileCols, ...depCols])];
      store.config.column_order = [...allCols];
      store.config.selected_columns = [...allCols];
      const mappings: any = store.config.field_mappings || {};
      allCols.forEach((c: string) => {
        if (!mappings[c]) {
          mappings[c] = {
            rename: false,
            newName: "",
            delete: false,
            unique: false,
            concat: { enabled: false, with: "", separator: " " }
          };
        }
      });
      store.config.field_mappings = mappings;

  } catch (e) {
    console.error("Schema load error:", e);
  } finally {
    loading.value = false;
  }
});
</script>

<style scoped>
.form-layout {
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 100%;
}

h3 {
  margin-bottom: 10px;
}

.info-banner {
  width: 70%;
  background-color: #e8f4fd;
  border-left: 4px solid #007bff;
  color: #004085;
  padding: 10px 15px;
  border-radius: 4px;
  font-size: 0.9em;
  margin-bottom: 15px;
  text-align: left;
}

.loading-text {
  color: #666;
  font-style: italic;
}

.draggable-list { 
  width: 70%; 
  border: 1px solid #ddd; 
  padding: 10px; 
  border-radius: 6px; 
  background: #fff; 
  box-shadow: 0 2px 8px rgba(0,0,0,0.02);
}

.ghost-row {
  opacity: 0.5;
  background: #e2eefd !important;
  border: 1px dashed #007bff;
}

.mapping-row { 
  border-bottom: 1px solid #f0f0f0; 
  padding: 10px 15px; 
  background: #fafafa;
  margin-bottom: 5px;
  border-radius: 4px;
  transition: background 0.2s, box-shadow 0.2s;
}
.mapping-row:hover {
  background: #f1f1f1;
}
.mapping-row:last-child {
  border-bottom: none;
  margin-bottom: 0;
}

.mapping-header { 
  display: flex; 
  align-items: center; 
}

/* ÚJ DRAG HANDLE STÍLUS */
.drag-handle { 
  cursor: grab; 
  color: #adb5bd; /* Világosabb szürke alapból */
  width: 30px; 
  display: flex;
  align-items: center;
  justify-content: flex-start;
  transition: color 0.2s;
}
.drag-handle:hover {
  color: #007bff; /* Egérráhúzáskor kék lesz */
}
.drag-handle:active {
  cursor: grabbing;
  color: #0056b3;
}

/* ERŐTELJESEBB SORSZÁM */
.row-number {
  width: 35px;
  font-weight: 800; /* Vastagabb betű */
  color: #343a40;  /* Sötétebb szín */
  font-size: 1.05em;
  text-align: left;
}

.column-name { 
  flex-grow: 1; 
  font-weight: 500; 
  text-align: left;
}

.settings-btn { 
  background: none; 
  border: none; 
  cursor: pointer; 
  font-size: 1.2em; 
  padding: 0; 
  width: 30px; 
  text-align: right; 
  color: #555;
  transition: transform 0.2s, color 0.2s;
}
.settings-btn:hover {
  color: #007bff;
  transform: scale(1.1);
}

.mapping-settings { 
  background: #fff; 
  padding: 12px; 
  margin-top: 10px; 
  border-radius: 4px; 
  border: 1px solid #e0e0e0; 
  box-shadow: inset 0 1px 3px rgba(0,0,0,0.05);
}

.setting-item { 
  margin-bottom: 8px; 
  display: flex; 
  align-items: center; 
  gap: 10px; 
  font-size: 0.95em;
}
.setting-item:last-child {
  margin-bottom: 0;
}

.small-input { 
  padding: 4px 8px; 
  font-size: 0.9em; 
  width: 180px; 
  border: 1px solid #ccc;
  border-radius: 3px;
}
</style>