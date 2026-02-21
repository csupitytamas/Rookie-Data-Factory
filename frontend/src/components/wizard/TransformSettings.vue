<template>
  <div class="form-layout">
    <h3>Transformations</h3>

    <div class="content-width">
      <div class="form-row">
        <label>Transformation Type:</label>
        <select v-model="transformationType" class="form-control">
          <option value="none">None</option>
          <option value="select">Simple Query</option>
          <option value="advenced">Advanced Query</option>
        </select>
      </div>

      <div v-if="transformationType === 'select'" class="settings-box">
        <div class="form-row">
          <label>Select Columns to Include:</label>
          <div class="checkbox-group">
            <div class="checkbox-actions">
              <button type="button" @click="selectAll" class="action-btn">Select All</button>
              
              <button 
                v-if="store.config.selected_columns.length === availableColumns.length && availableColumns.length > 0"
                type="button" 
                @click="deselectAll" 
                class="action-btn clear-btn transition-fade" 
                title="Deselect All"
              >
                ✕
              </button>
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
          <select v-model="store.config.order_by_column" class="form-control">
            <option :value="null">Please Select</option>
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

        <div v-if="store.config.order_by_column" class="form-row mt-3">
          <label>Order Direction:</label>
          <select v-model="store.config.order_direction" class="form-control">
            <option value="asc">ASC</option>
            <option value="desc">DESC</option>
          </select>
        </div>
      </div>

      <div v-if="transformationType === 'advenced'" class="settings-box">
        
        <div class="header-with-action">
          <div class="title-with-tooltip">
            <h4>Custom SQL Query</h4>
            <div class="tooltip-wrapper">
              <span class="hint-icon">💡</span>
              <div class="tooltip-content">
               <b>Important</b> refer to the table as <b>input_data</b> in SQL!"<br><br>
                <i>Example: SELECT * FROM input_data GROUP BY id</i>
              </div>
            </div>
          </div>
          
          <button type="button" class="hint-toggle-btn" @click="showSqlHint = !showSqlHint">
            {{ showSqlHint ? 'Hide Hint' : 'Show Hint' }}
          </button>
        </div>
        
        <div v-if="showSqlHint" class="info-banner transition-fade">
          
          <div class="info-text">
            <strong>Available Columns:</strong> 
            <div class="tooltip-wrapper">
              <span class="hint-icon">💡</span>
              <div class="tooltip-content">
                Click the name to copy!
              </div>
            </div>
          </div>
          
          <div class="column-tags">
            <span 
              v-for="col in availableColumns" 
              :key="col" 
              class="column-tag"
              @click="copyToClipboard(col)"
              title="Click to copy"
            >
              {{ col }}
            </span>
          </div>
        </div>

        <div class="form-row">
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
  </div>
</template>

<script setup lang="ts">
import { ref, watch, computed, onMounted } from 'vue';
import { usePipelineStore } from '@/stores/pipelineStore';

import { Codemirror } from 'vue-codemirror';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark'; 

const store = usePipelineStore();
const extensions = [sql(), oneDark];

const showSqlHint = ref(false);

// --- ÚJ LOGIKA: Az availableColumns mostantól okos: kiszűri a törölteket és alkalmazza az átnevezéseket ---
const availableColumns = computed(() => {
  const order = store.config.column_order || [];
  const mappings = store.config.field_mappings || {};

  return order.map(col => {
    const mapping = mappings[col];
    
    // Ha törlésre van jelölve a Mappingnél, dobjuk el a listából
    if (mapping && mapping.delete) {
      return null;
    }
    
    // Ha átnevezték, a "newName" kerül a listába
    if (mapping && mapping.rename && mapping.newName && mapping.newName.trim() !== '') {
      return mapping.newName.trim();
    }
    
    // Alapértelmezett: marad az eredeti név
    return col;
  }).filter(Boolean); // Kiszűrjük a null értékeket
});

const copyToClipboard = (text: string) => {
  navigator.clipboard.writeText(text).then(() => {
    console.log(`Copied: ${text}`);
  }).catch(err => {
    console.error('Failed to copy text: ', err);
  });
};

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
  // A korábbi módosítás alapján: Alapból üres lista várja a felhasználót
  if (!store.config.selected_columns) {
    store.config.selected_columns = [];
  }
});

watch(transformationType, (newVal, oldVal) => {
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
    
    // Ha most váltunk át, üres lappal indítunk
    if (oldVal === 'none' || oldVal === 'advenced') {
      store.config.selected_columns = [];
    } else if (!store.config.selected_columns) {
      store.config.selected_columns = [];
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
}

.settings-box {
  background: #f9f9f9;
  border: 1px solid #e0e0e0;
  padding: 20px;
  border-radius: 8px;
  margin-top: 20px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.02);
}

.header-with-action {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 15px;
  border-bottom: 1px solid #eee;
  padding-bottom: 10px;
}

.title-with-tooltip {
  display: flex;
  align-items: center;
}

.title-with-tooltip h4 {
  margin: 0;
  color: #555;
}

.hint-toggle-btn {
  background: #fff;
  border: 1px solid #007bff;
  color: #007bff;
  padding: 5px 12px;
  border-radius: 4px;
  font-size: 0.85em;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
}

.hint-toggle-btn:hover {
  background: #007bff;
  color: #fff;
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
  width: 250px;
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

.info-banner {
  background-color: #e8f4fd;
  border-left: 4px solid #007bff;
  color: #004085;
  padding: 15px;
  border-radius: 4px;
  margin-bottom: 20px;
}

.info-text {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  margin-bottom: 15px;
  font-size: 0.9em;
}

.column-tags {
  display: flex;
  flex-wrap: wrap;
  justify-content: center; 
  gap: 10px;
}

.column-tag {
  background-color: #fff;
  border: 1px solid #b8daff;
  color: #0056b3;
  padding: 6px 15px; 
  border-radius: 4px; 
  font-size: 0.85em;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
  user-select: none;
  text-align: center; 
  min-width: 120px; 
  box-shadow: 0 1px 2px rgba(0,0,0,0.02);
}

.column-tag:hover {
  background-color: #007bff;
  color: #fff;
  border-color: #007bff;
  transform: translateY(-1px);
  box-shadow: 0 3px 6px rgba(0, 123, 255, 0.15);
}

.column-tag:active {
  transform: translateY(0);
  box-shadow: none;
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
  gap: 10px; 
  margin-bottom: 10px;
  padding-bottom: 10px;
  border-bottom: 1px solid #eee; 
  height: 32px;
  align-items: center;
}

.action-btn {
  background-color: transparent;
  border: 1px solid #007bff;
  color: #007bff;
  border-radius: 4px;
  padding: 5px 12px;
  font-size: 0.85em;
  font-weight: 600;
  cursor: pointer;
  transition: background-color 0.2s, color 0.2s;
}

.action-btn:hover {
  background-color: #007bff;
  color: #fff;
}

.clear-btn {
  border-color: #dc3545;
  color: #dc3545;
  padding: 5px 10px; 
}

.clear-btn:hover {
  background-color: #dc3545;
  color: #fff;
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

.mt-3 {
  margin-top: 15px;
}
.transition-fade {
  animation: fadeIn 0.3s ease-in-out;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(-5px); }
  to { opacity: 1; transform: translateY(0); }
}
</style>