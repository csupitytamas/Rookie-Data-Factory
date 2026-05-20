<!-- A fájl a csoportosítási, rendezési és egyedi SQL lekérdezési beállításokat kezelő wizard lépés. -->
<template>
  <div class="form-layout">
    <h3>Query Configuration</h3>

    <div class="content-width">
      <div class="form-row">
        <label>Query Type:</label>
        <select v-model="transformationType" class="form-control">
          <option value="none">None</option>
          <option value="select">Simple Query</option>
          <option value="advenced">Advanced Query</option>
        </select>
      </div>

      <div v-if="transformationType === 'select'" class="settings-box">
        <div class="form-row">
          <label>Limit Rows:</label>
          <input 
            type="number" 
            v-model.number="store.config.limit_rows" 
            placeholder="e.g. 100 (Leave empty for all)"
            class="form-control"
            min="1"
          />
          <small class="hint-text">Keep only the first N rows of the result.</small>
        </div>

        <div class="form-row">
          <label>Group By Columns:</label>
          <div class="checkbox-group">
            <label 
              v-for="col in activeColumns" 
              :key="col" 
              class="checkbox-item"
            >
              <input 
                type="checkbox" 
                :value="col" 
                v-model="store.config.group_by_columns" 
              />
              <span>{{ col }}</span>
            </label>
          </div>
        </div>

        <div class="form-row">
          <label>Order By Column:</label>
          <select v-model="store.config.order_by_column" class="form-control">
            <option :value="null">Please Select</option>
            <option 
              v-for="col in activeColumns" 
              :key="col" 
              :value="col"
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
                <b>Flexible Aliasing</b>: You can use <b>any name</b> for the source table (e.g., <i>source</i>, <i>data</i>, <i>input</i>).<br><br>
                <i>Example: SELECT * FROM my_data GROUP BY id</i>
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
              v-for="col in sourceColumns" 
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
              placeholder="SELECT * FROM any_alias WHERE..."
              :style="{ height: '200px' }"
              :autofocus="true"
              :indent-with-tab="true"
              :tab-size="2"
              :extensions="extensions"
            />
          </div>
        </div>

        <!-- SQL EXAMPLES SECTION -->
        <div class="sql-examples-container">
          <div class="example-header" @click="showExamples = !showExamples">
            <span>💡 Quick SQL Examples</span>
            <span class="toggle-icon">{{ showExamples ? '▼' : '▶' }}</span>
          </div>
          
          <div v-if="showExamples" class="example-content transition-fade">
            <div class="example-item">
              <p><strong>Filtering:</strong> Use any alias like <i>source</i>, <i>data</i>, etc.</p>
              <pre @click="copyToClipboard('SELECT * FROM source WHERE price > 100')">SELECT * FROM source WHERE price > 100</pre>
            </div>
            
            <div class="example-item">
              <p><strong>Aggregation:</strong> Group your data with a custom name.</p>
              <pre @click="copyToClipboard('SELECT category, COUNT(*) as total FROM input_data GROUP BY category')">SELECT category, COUNT(*) as total FROM input_data GROUP BY category</pre>
            </div>
            
            <div class="example-item">
              <p><strong>Calculated Column:</strong> Create new data from any table name.</p>
              <pre @click="copyToClipboard('SELECT *, (price * 1.27) as price_with_vat FROM my_table')">SELECT *, (price * 1.27) as price_with_vat FROM my_table</pre>
            </div>

            <div class="example-item">
              <p><strong>Sorting & Limiting:</strong> Get the top results.</p>
              <pre @click="copyToClipboard('SELECT * FROM results ORDER BY date DESC LIMIT 10')">SELECT * FROM results ORDER BY date DESC LIMIT 10</pre>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, computed } from 'vue';
import { usePipelineStore } from '@/stores/pipelineStore';
import { Codemirror } from 'vue-codemirror';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark'; 

const store = usePipelineStore();
const extensions = [sql(), oneDark];
const showSqlHint = ref(false);
const showExamples = ref(false);

// Összeállítja az összes leképezett oszlop listáját az átnevezések figyelembevételével.
const allMappedColumns = computed(() => {
  const order = store.config.column_order || [];
  const mappings = store.config.field_mappings || {};

  // Végigmegyünk az eredeti oszlopneveken és kinyerjük a megjelenítendő (alias) nevüket.
  return order.map(origName => {
    const mapping = mappings[origName];

    // Ha az oszlopot átnevezték, az új nevet használjuk; ellenkező esetben az eredetit.
    const displayName = (mapping?.rename && mapping?.newName && mapping.newName.trim() !== '') 
      ? mapping.newName.trim() 
      : origName;
    return {
      origName,
      displayName,
      deleted: !!mapping?.delete
    };
  });
});

// A ténylegesen aktív (nem törölt) oszlopok megjelenítendő neveinek listája.
const activeColumns = computed(() => {
  return allMappedColumns.value
    .filter(c => !c.deleted)
    .map(c => c.displayName);
});

// Az eredeti forrás oszlopok listája (pl. a másolható tagekhez a custom SQL-nél).
const sourceColumns = computed(() => {
  return store.config.column_order || [];
});


// Be- és kikapcsolja egy oszlop 'törölt' állapotát.
const toggleColumn = (origName: string) => {
  if (!store.config.field_mappings[origName]) {
    store.config.field_mappings[origName] = {};
  }
  store.config.field_mappings[origName].delete = !store.config.field_mappings[origName].delete;
};

// Vágólapra másolja a megadott szöveget (pl. oszlopnév vagy példa SQL).
const copyToClipboard = (text: string) => {
  navigator.clipboard.writeText(text).then(() => {
  }).catch(err => {
    console.error('Failed to copy text: ', err);
  });
};


const getInitialType = () => {
  if (store.config.custom_sql) return 'advenced';
  if (
    (store.config.group_by_columns && store.config.group_by_columns.length > 0) || 
    store.config.order_by_column ||
    allMappedColumns.value.some(c => c.deleted)
  ) {
    return 'select';
  }
  return 'none';
};
const transformationType = ref(getInitialType());

// Kijelöli az összes oszlopot (törli a 'delete' jelzőt a leképezésből).
const selectAll = () => {
  allMappedColumns.value.forEach(c => {
    if (store.config.field_mappings[c.origName]) {
      store.config.field_mappings[c.origName].delete = false;
    }
  });
};

// Megszünteti az összes oszlop kijelölését (beállítja a 'delete' jelzőt).
const deselectAll = () => {
  allMappedColumns.value.forEach(c => {
    if (store.config.field_mappings[c.origName]) {
      store.config.field_mappings[c.origName].delete = true;
    }
  });
};

// Figyeli a transzformáció típusának változását és alaphelyzetbe állítja az irreleváns mezőket.
watch(transformationType, (newVal) => {
 if (!store.config.transformation) {
    store.config.transformation = {};
  }
  store.config.transformation.type = newVal;

  // None választása esetén minden adatot kiürítünk.
  if (newVal === 'none') {
    store.config.custom_sql = null;
    store.config.group_by_columns = [];
    store.config.order_by_column = null;
    store.config.order_direction = 'asc';
    selectAll();
  }

  // Simple query választása esetén az SQL mezőt töröljük.
  else if (newVal === 'select') {
    store.config.custom_sql = null;
    if (!store.config.group_by_columns) store.config.group_by_columns = [];
    if (!store.config.order_direction) store.config.order_direction = 'asc';
  }

  // Advanced query választása esetén a vizuális beállításokat töröljük.
  else if (newVal === 'advenced') {
    store.config.group_by_columns = [];
    store.config.order_by_column = null;
  }
});

// Dinamikusan eltávolítja a Group By és Order By opciókból azokat az oszlopokat, amelyeket inaktívvá tettek.
watch(activeColumns, (newActive) => {
  if (store.config.group_by_columns.length > 0) {
     store.config.group_by_columns = store.config.group_by_columns.filter((col: string) => newActive.includes(col));
  }
  if (store.config.order_by_column && !newActive.includes(store.config.order_by_column)) {
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
  border: 1px solid #07085e;
  color: #07085e;
  padding: 5px 12px;
  border-radius: 4px;
  font-size: 0.85em;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
}

.hint-toggle-btn:hover {
  background: #07085e;
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
  border-left: 4px solid #07085e;
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
  background-color: #07085e;
  color: #fff;
  border-color: #07085e;
  transform: translateY(-1px);
  box-shadow: 0 3px 6px rgba(7, 8, 94, 0.15);
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
  border: 1px solid #07085e;
  color: #07085e;
  border-radius: 4px;
  padding: 5px 12px;
  font-size: 0.85em;
  font-weight: 600;
  cursor: pointer;
  transition: background-color 0.2s, color 0.2s;
}

.action-btn:hover {
  background-color: #07085e;
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

/* SQL EXAMPLES STYLING */
.sql-examples-container {
  margin-top: 20px;
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  background: white;
  overflow: hidden;
}

.example-header {
  padding: 10px 15px;
  background: #f8fafc;
  display: flex;
  justify-content: space-between;
  align-items: center;
  cursor: pointer;
  font-size: 0.9rem;
  font-weight: 600;
  color: #475569;
  border-bottom: 1px solid #e2e8f0;
  user-select: none;
}

.example-header:hover {
  background: #f1f5f9;
}

.example-content {
  padding: 15px;
}

.example-item {
  margin-bottom: 15px;
}

.example-item:last-child {
  margin-bottom: 0;
}

.example-item p {
  margin: 0 0 8px 0;
  font-size: 0.85rem;
  color: #64748b;
}

.example-item pre {
  background: #f1f5f9;
  padding: 8px 12px;
  border-radius: 4px;
  font-family: 'Consolas', 'Monaco', monospace;
  font-size: 0.8rem;
  color: #07085e;
  margin: 0;
  cursor: pointer;
  border: 1px solid transparent;
  transition: all 0.2s;
  overflow-x: auto;
}

.example-item pre:hover {
  background: #e2e8f0;
  border-color: #07085e;
}

.toggle-icon {
  font-size: 0.7rem;
  color: #94a3b8;
}

.transition-fade {
  animation: fadeIn 0.3s ease-in-out;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(-5px); }
  to { opacity: 1; transform: translateY(0); }
}
</style>
