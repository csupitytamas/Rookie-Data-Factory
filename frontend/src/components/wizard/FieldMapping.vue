<template>
  <div class="form-layout">
    <h3>Field Mapping</h3>
    <p v-if="loading">Loading fields...</p>
    
    <draggable 
      v-else
      v-model="store.config.column_order" 
      item-key="col" 
      class="draggable-list"
      handle=".drag-handle"
    >
      <template #item="{ element: col, index }">
        <div class="mapping-row">
          <div class="mapping-header">
            <span class="drag-handle">☰</span>
            <span class="column-name">{{ col }}</span>
            <button class="settings-btn" @click.prevent="toggleSettings(col)">⚙️</button>
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
       <p style="color:red">Failed to load fields for this source.</p>
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

<style scoped src="../styles/CreateETLPipeline.style.css"></style>
<style scoped>
.draggable-list { border: 1px solid #eee; padding: 5px; border-radius: 4px; background: #fff; }
.mapping-row { border-bottom: 1px solid #f0f0f0; padding: 8px 5px; }
.mapping-header { display: flex; align-items: center; justify-content: space-between; }
.drag-handle { cursor: grab; margin-right: 10px; color: #888; font-size: 1.2em; }
.column-name { flex-grow: 1; font-weight: 500; }
.settings-btn { background: none; border: none; cursor: pointer; font-size: 1.2em; padding: 0 5px; }
.mapping-settings { background: #f9f9f9; padding: 10px; margin-top: 5px; border-radius: 4px; border: 1px solid #eee; }
.setting-item { margin-bottom: 5px; display: flex; align-items: center; gap: 10px; }
.small-input { padding: 4px; font-size: 0.9em; width: 150px; }
</style>