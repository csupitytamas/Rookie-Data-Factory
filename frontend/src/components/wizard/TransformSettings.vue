<template>
  <div class="form-layout">
    <h3>Transformations</h3>

    <div class="form-row">
      <label>Transformation Type:</label>
      <select v-model="transformationType">
        <option value="none">None (No transformation)</option>
        <option value="select">Select (Filter, Group & Sort)</option>
        <option value="advenced">Advanced (Custom SQL)</option>
      </select>
    </div>

    <div v-if="transformationType === 'select'" class="settings-box">
      <div class="form-row">
        <label>Select Columns to Include:</label>
        <div class="checkbox-group">
          <div class="checkbox-actions">
            <button type="button" @click="selectAll" class="link-btn">Select All</button>
            <button type="button" @click="deselectAll" class="link-btn">Deselect All</button>
          </div>
          <label 
            v-for="col in availableColumns" 
            :key="col" 
            class="checkbox-item"
            :class="{ 'inactive': !isSelected(col) }"
          >
            <input 
              type="checkbox" 
              :value="col" 
              v-model="store.config.selected_columns" 
            />
            {{ col }}
          </label>
        </div>
        <p v-if="store.config.selected_columns.length === 0" class="error-msg">
          You must select at least one column!
        </p>
      </div>

      <hr class="separator" />

      <div class="form-row">
        <label>Group By Columns:</label>
        <div class="checkbox-group">
          <label 
            v-for="col in availableColumns" 
            :key="col" 
            class="checkbox-item"
          >
            <input 
              type="checkbox" 
              :value="col" 
              v-model="store.config.group_by_columns" 
              :disabled="!isSelected(col)"
            />
            <span :class="{ 'disabled-text': !isSelected(col) }">{{ col }}</span>
          </label>
        </div>
      </div>

      <div class="form-row">
        <label>Order By Column:</label>
        <select v-model="store.config.order_by_column">
          <option :value="null">-- No ordering --</option>
          <option 
            v-for="col in availableColumns" 
            :key="col" 
            :value="col"
            :disabled="!isSelected(col)"
          >
            {{ col }}
          </option>
        </select>
      </div>

      <div v-if="store.config.order_by_column" class="form-row">
        <label>Order Direction:</label>
        <select v-model="store.config.order_direction">
          <option value="asc">Ascending (ASC)</option>
          <option value="desc">Descending (DESC)</option>
        </select>
      </div>
    </div>

    <div v-if="transformationType === 'advenced'" class="settings-box">
      <h4>Custom SQL Query</h4>
      <div class="form-row">
        <p class="hint">
          Write a SQL query to filter or transform data. 
          Use <b>input_data</b> as the table name.
          <br>
          <i>Example: SELECT country, count(*) FROM input_data GROUP BY country</i>
        </p>
        
        <div class="editor-container">
          <Codemirror
            v-model="store.config.custom_sql"
            placeholder="SELECT * FROM input_data WHERE..."
            :style="{ height: '200px' }"
            :autofocus="true"
            :indent-with-tab="true"
            :tab-size="2"
            :extensions="extensions"
          />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, computed, onMounted } from 'vue';
import { usePipelineStore } from '@/stores/pipelineStore';

// --- JAVÍTOTT IMPORT: Csak a 'vue-codemirror'-t használjuk, a 6-os szám nélkül ---
import { Codemirror } from 'vue-codemirror';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark'; 

const store = usePipelineStore();

// CodeMirror kiegészítők
const extensions = [sql(), oneDark];

const availableColumns = computed(() => store.config.column_order || []);

const isSelected = (col: string) => {
  return store.config.selected_columns.includes(col);
};

const getInitialType = () => {
  if (store.config.custom_sql) return 'advenced';
  if (
    (store.config.group_by_columns && store.config.group_by_columns.length > 0) || 
    store.config.order_by_column ||
    (store.config.selected_columns && store.config.selected_columns.length > 0 && store.config.selected_columns.length < availableColumns.value.length)
  ) {
    return 'select';
  }
  return 'none';
};

const transformationType = ref(getInitialType());

const selectAll = () => {
  store.config.selected_columns = [...availableColumns.value];
};

const deselectAll = () => {
  store.config.selected_columns = [];
};

onMounted(() => {
  if (!store.config.selected_columns || store.config.selected_columns.length === 0) {
    store.config.selected_columns = [...availableColumns.value];
  }
});

watch(transformationType, (newVal) => {
 if (!store.config.transformation) {
    store.config.transformation = {};
  }
  store.config.transformation.type = newVal;
  if (newVal === 'none') {
    store.config.custom_sql = null;
    store.config.group_by_columns = [];
    store.config.order_by_column = null;
    store.config.order_direction = 'asc';
    store.config.selected_columns = [...availableColumns.value];
  } 
  else if (newVal === 'select') {
    store.config.custom_sql = null;
    if (!store.config.group_by_columns) store.config.group_by_columns = [];
    if (!store.config.order_direction) store.config.order_direction = 'asc';
    if (store.config.selected_columns.length === 0) {
       store.config.selected_columns = [...availableColumns.value];
    }
  }
  else if (newVal === 'advenced') {
    store.config.group_by_columns = [];
    store.config.order_by_column = null;
    store.config.selected_columns = [];
  }
});

watch(() => store.config.selected_columns, (newSelected) => {
  if (store.config.group_by_columns.length > 0) {
     store.config.group_by_columns = store.config.group_by_columns.filter((col: string) => newSelected.includes(col));
  }
  if (store.config.order_by_column && !newSelected.includes(store.config.order_by_column)) {
     store.config.order_by_column = null;
  }
}, { deep: true });
</script>

<style scoped src="../styles/CreateETLPipeline.style.css"></style>
<style scoped>
.settings-box {
  background: #f9f9f9;
  border: 1px solid #e0e0e0;
  padding: 15px;
  border-radius: 8px;
  margin-top: 15px;
}

h4 {
  margin-top: 0;
  margin-bottom: 15px;
  color: #555;
  border-bottom: 1px solid #eee;
  padding-bottom: 5px;
}

.checkbox-group {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-height: 200px;
  overflow-y: auto;
  padding: 10px;
  background: white;
  border: 1px solid #ccc;
  border-radius: 4px;
}

.checkbox-item {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: normal;
  cursor: pointer;
  padding: 2px 5px;
}

.checkbox-item:hover {
  background-color: #f0f8ff;
}

.checkbox-item.inactive {
  opacity: 0.6;
}

.checkbox-actions {
  display: flex;
  gap: 15px;
  margin-bottom: 5px;
}

.link-btn {
  background: none;
  border: none;
  color: #007bff;
  cursor: pointer;
  padding: 0;
  font-size: 0.9em;
  text-decoration: underline;
}

.disabled-text {
  color: #aaa;
  text-decoration: line-through;
}

.separator {
  margin: 20px 0;
  border: 0;
  border-top: 1px solid #ddd;
}

.error-msg {
  color: #dc3545;
  font-size: 0.9em;
  font-weight: bold;
  margin-top: 5px;
}

.editor-container {
  border: 1px solid #ccc;
  border-radius: 4px;
  overflow: hidden; 
}

.hint {
  font-size: 0.85em;
  color: #666;
  margin-bottom: 8px;
  background-color: #eef;
  padding: 8px;
  border-radius: 4px;
  border-left: 3px solid #007bff;
}
</style>