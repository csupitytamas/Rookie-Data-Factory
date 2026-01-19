<template>
  <div class="config-container">
    <h2>Configuration</h2>

    <!-- Ütemezés -->
    <div class="form-group">
      <label>Schedule</label>
      <select v-model="schedule">
        <option value="daily">daily</option>
        <option value="hourly">hourly</option>
        <option value="minutes">minutes</option>
        <option value="custom">custom</option>
      </select>
      <div v-if="schedule === 'custom'" class="custom-time-wrapper">
        <input type="time" v-model="customTime" />
      </div>
    </div>

    <!-- Futtatási feltételek -->
  <div class="form-group">
      <label>Running conditions</label>
      <select v-model="conditions">
        <option value="none">None</option>
        <option value="withsource">With different source</option>
        <option value="withdependency">Wait for another pipeline</option>
      </select>
    </div>

    <!-- Dependency selection -->
    <div class="form-group" v-if="conditions === 'withdependency'">
      <label>Select dependency pipeline</label>
      <select v-model="dependencyPipelineId">
        <option value="">None</option>
        <option v-for="pipeline in activePipelines" :key="pipeline.id" :value="pipeline.id">
          {{ pipeline.name }}
        </option>
      </select>
    </div>

    <div class="form-group" v-if="conditions === 'withsource'">
      <label>Own Source feltöltés:</label>
      <div class="custom-file-input">
        <label for="fileUpload" class="upload-label">
          {{ uploadedFileName || "Click to upload file" }}
        </label>
        <input id="fileUpload" type="file" @change="handleFileUpload" />
      </div>
    </div>

    <!-- Field Mapping  -->
  <div class="form-group">
    <label>Field Mapping</label>
    <draggable v-model="columnOrder" item-key="col" class="draggable-list">
      <template #item="{ element: col, index }">
        <div class="mapping-row">
          <div class="mapping-header">
            <span class="drag-handle">☰</span>
            <span class="column-index">{{ index + 1 }}.</span>
            <span class="column-name">{{ col }}</span>
            <button class="settings-button" @click="toggleSettings(col)">⚙️</button>
          </div>

          <div v-if="colSettingsOpen[col]" class="mapping-settings">
            <label><input type="checkbox" v-model="fieldMappings[col].rename" /> Rename</label>
            <input v-if="fieldMappings[col].rename" type="text" v-model="fieldMappings[col].newName" placeholder="New name" /><label>
            <input type="checkbox" v-model="fieldMappings[col].unique" /> Unique </label>

            <label><input type="checkbox" v-model="fieldMappings[col].delete" /> Delete</label>

             <label><input type="checkbox" v-model="fieldMappings[col].split" /> Split</label>
            <select v-if="fieldMappings[col].split" v-model="fieldMappings[col].separator">
              <option disabled value="">Please select</option>
              <option v-for="sep in separatorOptions" :key="sep" :value="sep">{{ sep === ' ' ? 'space' : sep }}</option>
            </select>

                    <label><input type="checkbox" v-model="fieldMappings[col].concat.enabled" @change="onConcatEnableChange(col)" /> Concatenate</label>
            <div v-if="fieldMappings[col].concat.enabled" class="join-options">
              <label>With column:</label>
              <select v-model="fieldMappings[col].concat.with"
                    @change="onConcatWithChange(col, fieldMappings[col].concat.with)">
                <option disabled value="">Please select</option>
                <option v-for="targetCol in allColumns" :key="targetCol" :value="targetCol">
                  {{ targetCol }}
                </option>
              </select>

              <label>Separator:</label>
              <select v-model="fieldMappings[col].concat.separator">
                <option disabled value="">Please select</option>
                <option v-for="sep in separatorOptions" :key="sep" :value="sep">{{ sep === ' ' ? 'space' : sep }}</option>
              </select>
            </div>
          </div>
        </div>
      </template>
    </draggable>
  </div>

    <!-- Transformation -->
    <div class="form-group">
      <label>Transformation on the dataset</label>
      <select v-model="transformation">
        <option value="none">None</option>
        <option value="select">Select</option>
        <option value="advenced">Advanced</option>
      </select>
    </div>

    <!-- Select + GroupBy + OrderBy -->
    <div v-if="transformation === 'select'" class="form-group">
      <label>Select columns</label>
      <button class="small-button" @click="toggleSelectAll">
        {{ selectedColumns.length === allColumns.length ? 'Unselect all' : 'Select all' }}
      </button>

      <div class="grid-checkboxes">
        <label v-for="col in allColumns" :key="col">
          <input type="checkbox" :value="col" v-model="selectedColumns" />
          {{ col }}
        </label>
      </div>

      <!-- GROUP BY -->
      <div class="form-group" v-if="selectedColumns.length">
        <label>Group by</label>
        <div class="none-option">
          <input type="checkbox" id="disableGroupBy" v-model="disableGroupBy" />
          <label for="disableGroupBy">None</label>
        </div>
        <div v-if="!disableGroupBy" class="grid-checkboxes">
          <label v-for="col in selectedColumns" :key="'g-' + col">
            <input type="checkbox" :value="col" v-model="groupBy" />
            {{ col }}
          </label>
        </div>
      </div>

      <!-- ORDER BY -->
      <div class="form-group" v-if="selectedColumns.length">
        <label>Order by</label>
        <div class="none-option">
          <input type="checkbox" id="disableOrderBy" v-model="disableOrderBy" />
          <label for="disableOrderBy">None</label>
        </div>
        <div v-if="!disableOrderBy">
          <select v-model="orderBy" class="standard-select">
            <option disabled value="">-- Select column --</option>
            <option v-for="col in selectedColumns" :key="'o-' + col" :value="col">{{ col }}</option>
          </select>

          <div v-if="orderBy" style="margin-top: 10px;">
            <label>Order direction</label>
            <div class="order-direction-container">
              <label><input type="radio" value="asc" v-model="orderDirection" /> Ascending</label>
              <label><input type="radio" value="desc" v-model="orderDirection" /> Descending</label>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Advanced (SQL) -->
    <div class="form-group" v-if="transformation === 'advenced'">
      <label>Custom SQL query</label>
      <textarea v-model="customSQL" placeholder="Pl. SELECT name, COUNT(*) FROM table GROUP BY name"></textarea>
    </div>

    <!-- Update mód -->
    <div class="form-group">
      <label>Dataset update</label>
      <select v-model="update">
        <option value="overwrite">Overwrite</option>
        <option value="append">Append</option>
        <option value="upsert">Upsert (Update and append)</option>
      </select>
    </div>

    <!-- Mentés mód -->
    <div class="form-group">
      <label>Save options</label>
      <select v-model="saveOption">
        <option value="todatabase">Only database</option>
        <option value="createfile">Create file to</option>
      </select>
    </div>

     <div class="form-group" v-if="saveOption === 'createfile'">
    <label>File format</label>
    <select v-model="selectedFileFormat">
      <option v-for="fmt in fileFormats" :key="fmt.value" :value="fmt.value">
        {{ fmt.label }}
      </option>
    </select>
    </div>

    <div class="form-group">
      <button @click="submitPipelineConfig">Save</button>
      <button @click="$router.go(-1)">Back</button>
    </div>
  </div>
</template>


<script lang="ts">
import draggable from 'vuedraggable';
import { defineComponent, onMounted, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { usePipelineStore } from '@/stores/pipelineStore';
import { loadPipelineData, updatePipeline } from '@/api/pipeline';

export default defineComponent({
  name: 'EditorETLConfig',
  components: { draggable },
  setup() {
    const store = usePipelineStore();
    const router = useRouter();
    const route = useRoute();

    const pipelineId = route.query.id;

    const schedule = ref('daily');
    const customTime = ref('');
    const conditions = ref('none');
    const dependencyPipelineId = ref('');
    const update = ref('append');
    const saveOption = ref('todatabase');
    const uploadedFileName = ref('');
    const fileData = ref<File | null>(null);

    const activePipelines = ref([]);
    const allColumns = ref<string[]>([]);
    const columnOrder = ref<string[]>([]);
    const selectedColumns = ref<string[]>([]);
    const groupBy = ref<string[]>([]);
    const orderBy = ref('');
    const orderDirection = ref('asc');
    const customSQL = ref('');
    const transformation = ref('none');
    const disableGroupBy = ref(false);
    const disableOrderBy = ref(false);

    const fieldMappings = ref<Record<string, any>>({});
    const colSettingsOpen = ref<Record<string, boolean>>({});
    const separatorOptions = ref([" ", "_"]);

    onMounted(async () => {
      if (pipelineId) {
        try {
          const response = await loadPipelineData(pipelineId);
          const pipeline = response.data;

          store.config = { ...pipeline };

          schedule.value = pipeline.schedule || 'daily';
          customTime.value = pipeline.custom_time || '';
          conditions.value = pipeline.condition || 'none';
          dependencyPipelineId.value = pipeline.dependency_pipeline_id || '';
          update.value = pipeline.update_mode || 'append';
          saveOption.value = pipeline.save_option || 'todatabase';
          uploadedFileName.value = pipeline.uploaded_file_name || '';

          columnOrder.value = pipeline.column_order || [];
          selectedColumns.value = pipeline.selected_columns || [];
          groupBy.value = pipeline.group_by_columns || [];
          orderBy.value = pipeline.order_by_column || '';
          orderDirection.value = pipeline.order_direction || 'asc';
          customSQL.value = pipeline.custom_sql || '';

          if (pipeline.transformation) {
            transformation.value = pipeline.transformation.type;
          }

          fieldMappings.value = pipeline.field_mappings || {};
          allColumns.value = Object.keys(fieldMappings.value);

        } catch (err) {
          console.error("Failed to load:", err);
        }
      }
    });

     const fileFormats = ref([
      { value: 'csv', label: 'CSV' },
      { value: 'json', label: 'JSON' },
      { value: 'parquet', label: 'Parquet' },
      { value: 'excel', label: 'Excel (XLSX)' },
      { value: 'txt', label: 'Plain text (TXT)' },
      { value: 'xml', label: 'XML' },
      { value: 'yaml', label: 'YAML' }
    ]);
    const selectedFileFormat = ref('csv');

    const onConcatWithChange = (col, targetCol) => {
    // 1. Ha targetCol üres, csak az aktuális oszlop enabled legyen false
    if (!targetCol) {
      fieldMappings.value[col].concat.enabled = false;
      return;
    }

    // 2. Mindkét oszlopon enabled = true
    fieldMappings.value[col].concat.enabled = true;
    fieldMappings.value[targetCol].concat.enabled = true;

    // 3. Csak az aktuális oszlopnál legyen kitöltve a with
    fieldMappings.value[targetCol].concat.with = "";

    // 4. Tisztítsd a többi mezőt is, ahol visszafelé lenne ilyen with (ne legyen kölcsönös összefűzés)
    Object.keys(fieldMappings.value).forEach(otherCol => {
      if (
        otherCol !== col &&
        fieldMappings.value[otherCol].concat.with === col
      ) {
        fieldMappings.value[otherCol].concat.with = "";
      }
    });
  };

    const onConcatEnableChange = (col) => {
      const enabled = fieldMappings.value[col].concat.enabled;
      const withCol = fieldMappings.value[col].concat.with;

  // Ha kikapcsolod a checkboxot, akkor a pair-en is disabled
      if (!enabled) {
        if (withCol) {
          fieldMappings.value[withCol].concat.enabled = false;
          fieldMappings.value[withCol].concat.with = "";
        }
        fieldMappings.value[col].concat.with = "";
      }
    };

    const handleFileUpload = (event: Event) => {
      const file = (event.target as HTMLInputElement).files?.[0] || null;
      if (file) {
        uploadedFileName.value = file.name;
        fileData.value = file;
      }
    };

    const toggleSelectAll = () => {
      if (selectedColumns.value.length === allColumns.value.length) {
        selectedColumns.value = [];
      } else {
        selectedColumns.value = [...allColumns.value];
      }
    };

    const toggleSettings = (col: string) => {
      colSettingsOpen.value[col] = !colSettingsOpen.value[col];
    };

    const submitPipelineConfig = async () => {
      if (!pipelineId) {
        alert('Missing pipeline ID for update.');
        return;
      }

      try {
        const payload = {
          schedule: schedule.value,
        custom_time: schedule.value === 'custom' ? customTime.value : null,
        condition: conditions.value,
        dependency_pipeline_id: conditions.value === 'withdependency' ? dependencyPipelineId.value : null,
        uploaded_file_name: conditions.value === 'withsource' ? uploadedFileName.value : null,
        update_mode: update.value,
        save_option: saveOption.value,
        field_mappings: fieldMappings.value,
        column_order: columnOrder.value,
        selected_columns: selectedColumns.value,
        group_by_columns: disableGroupBy.value ? [] : groupBy.value,
        order_by_column: disableOrderBy.value ? null : orderBy.value,
        order_direction: disableOrderBy.value ? null : orderDirection.value,
        custom_sql: transformation.value === 'advenced' ? customSQL.value : null,
        file_format: saveOption.value === 'createfile' ? selectedFileFormat.value : null,
        transformation: {
          type: transformation.value }
        };

        console.log("Update payload:", payload);

        await updatePipeline(pipelineId, payload);

        alert('Pipeline updated successfully!');
        router.push('/'); // Visszairányít pl. a Dashboard-ra
      } catch (err) {
        console.error("Error updating pipeline:", err);
        alert('Failed to update pipeline!');
      }
    };

    return {
      store,
      router,
      schedule,
      customTime,
      conditions,
      dependencyPipelineId,
      update,
      saveOption,
      uploadedFileName,
      fileData,
      activePipelines,
      allColumns,
      columnOrder,
      selectedColumns,
      groupBy,
      orderBy,
      orderDirection,
      customSQL,
      transformation,
      fieldMappings,
      colSettingsOpen,
      separatorOptions,
      disableGroupBy,
      disableOrderBy,
      fileFormats,
      selectedFileFormat,
      onConcatWithChange,
      onConcatEnableChange,
      handleFileUpload,
      toggleSelectAll,
      toggleSettings,
      submitPipelineConfig
    };
  }
});
</script>
<style scoped src="./styles/ETLConfig.style.css"></style>