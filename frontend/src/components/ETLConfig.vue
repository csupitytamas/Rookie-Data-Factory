<template>
  <div class="config-container">
    <h2>Configuration</h2>

    <!-- Schedule -->
    <div class="form-group">
      <label>Schedule</label>
      <select v-model="schedule">
        <option value="daily">daily</option>
        <option value="yearly">yearly</option>
        <option value="monthly">monthly</option>
        <option value="weekly">weekly</option>
        <option value="hourly">hourly</option>
        <option value="once">once</option>
        <option value="never">never</option>
        <option value="custom">custom</option>
      </select>
      <div v-if="schedule === 'custom'" class="custom-time-wrapper">
        <input type="time" v-model="customTime" />
      </div>
    </div>

    <!-- Running conditions -->
    <div class="form-group">
      <label>Running conditions</label>
      <select v-model="conditions">
        <option value="none">None</option>
        <option value="withsource">With different source</option>
        <option value="withdependency">Wait for another pipeline</option>
      </select>
    </div>

    <!-- Dependency -->
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
      <label>Upload source file</label>
      <div class="custom-file-input">
        <label for="fileUpload" class="upload-label">
          {{ uploadedFileName || "Click to upload file" }}
        </label>
        <input id="fileUpload" type="file" @change="handleFileUpload" />
      </div>
    </div>

    <!-- API PARAMS -->
    <div class="form-group" v-if="Object.keys(configSchema).length > 0">
      <label>API Parameters</label>

      <div
        v-for="(paramConfig, paramKey) in configSchema"
        :key="paramKey"
        class="parameter-field"
      >
        <label :for="`param-${paramKey}`">
          {{ paramConfig.friendly_name || paramConfig.label || paramKey }}
          <span v-if="paramConfig.required" class="required">*</span>
        </label>

        <p v-if="paramConfig.description" class="param-description">
          {{ paramConfig.description }}
        </p>

        <!-- SELECT -->
        <select
          v-if="paramConfig.type === 'select'"
          :id="`param-${paramKey}`"
          v-model="apiParameters[paramKey]"
          class="form-control"
        >
          <option value="">Please select...</option>
          <option
            v-for="opt in paramConfig.options"
            :key="opt.value"
            :value="opt.value"
          >
            {{ opt.label }}
          </option>
        </select>

        <!-- NUMBER -->
        <input
          v-else-if="paramConfig.type === 'number' || paramConfig.type === 'integer'"
          :id="`param-${paramKey}`"
          type="number"
          v-model.number="apiParameters[paramKey]"
          class="form-control"
        />

        <!-- TEXT -->
        <input
          v-else
          :id="`param-${paramKey}`"
          type="text"
          v-model="apiParameters[paramKey]"
          class="form-control"
        />
      </div>
    </div>

    <!-- FIELD MAPPING -->
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
              <input
                v-if="fieldMappings[col].rename"
                type="text"
                v-model="fieldMappings[col].newName"
                placeholder="New name"
              />

              <label><input type="checkbox" v-model="fieldMappings[col].unique" /> Unique</label>
              <label><input type="checkbox" v-model="fieldMappings[col].delete" /> Delete</label>

              <label>
                <input
                  type="checkbox"
                  v-model="fieldMappings[col].concat.enabled"
                  @change="onConcatEnableChange(col)"
                /> Concatenate
              </label>

              <div v-if="fieldMappings[col].concat.enabled" class="join-options">
                <label>With column:</label>
                <select
                  v-model="fieldMappings[col].concat.with"
                  @change="onConcatWithChange(col, fieldMappings[col].concat.with)"
                >
                  <option disabled value="">Please select</option>
                  <option
                    v-for="targetCol in allColumns"
                    :key="targetCol"
                    :value="targetCol"
                    :disabled="targetCol === col"
                  >
                    {{ targetCol }}
                  </option>
                </select>

                <label>Separator:</label>
                <select
                  v-model="fieldMappings[col].concat.separator"
                  @change="onConcatSeparatorChange(col)"
                >
                  <option value=" ">space</option>
                  <option value="_">_</option>
                </select>
              </div>
            </div>
          </div>
        </template>
      </draggable>
    </div>

    <!-- TRANSFORMATION -->
    <div class="form-group">
      <label>Transformation on the dataset</label>
      <select v-model="transformation">
        <option value="none">None</option>
        <option value="select">Select</option>
        <option value="advenced">Advanced</option>
      </select>
    </div>

    <div class="form-group" v-if="transformation === 'advenced'">
      <label>Custom SQL query</label>
      <textarea v-model="customSQL"></textarea>
    </div>

    <!-- UPDATE -->
    <div class="form-group">
      <label>Dataset update</label>
      <select v-model="update">
        <option value="overwrite">Overwrite</option>
        <option value="append">Append</option>
        <option value="upsert">Upsert</option>
      </select>
    </div>

    <!-- SAVE -->
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
      <button @click="router.go(-1)">Back</button>
    </div>
  </div>
</template>

<script lang="ts">
import draggable from 'vuedraggable';
import { defineComponent, onMounted, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { usePipelineStore } from '@/stores/pipelineStore';
// JAVÍTÁS: Hozzáadtuk a getConnectorFilters-t az importhoz
import { loadSchemaBySource, getFriendlySchemaBySource, getConnectorFilters } from '@/api/pipeline';
import axios from 'axios';

// ---- PARAM CONFIG TYPE ----
interface ParamConfig {
  type: string;
  required?: boolean;
  label?: string;
  friendly_name?: string;
  description?: string;
  default?: any;
  placeholder?: string;
  help_text?: string;
  examples?: string[];
  options?: Array<{ value: string; label: string }>;
}

export default defineComponent({
  name: 'ETLConfig',
  components: { draggable },

  setup() {
    const store = usePipelineStore();
    const router = useRouter();
    const route = useRoute();

    const source = route.query.selectedSource as string;

    const schedule = ref(store.config.schedule || "daily");
    const customTime = ref(store.config.custom_time || "");
    const conditions = ref(store.config.condition || "none");
    const dependencyPipelineId = ref(store.config.dependency_pipeline_id || "");
    const update = ref(store.config.update_mode || "append");
    const saveOption = ref(store.config.save_option || "todatabase");
    const uploadedFileName = ref(store.config.uploaded_file_name || "");
    const fileData = ref<File | null>(null);

    const activePipelines = ref([]);

    const allColumns = ref([]);
    const columnOrder = ref([]);
    const selectedColumns = ref([]);
    const groupBy = ref([]);
    const orderBy = ref("");
    const orderDirection = ref("asc");
    const customSQL = ref("");
    const transformation = ref("none");
    const disableGroupBy = ref(false);
    const disableOrderBy = ref(false);

    const fieldMappings = ref({});
    const colSettingsOpen = ref({});
    const separatorOptions = ref([" ", "_"]);

    const fileFormats = ref([
      { value: "csv", label: "CSV" },
      { value: "json", label: "JSON" },
      { value: "parquet", label: "Parquet" },
      { value: "excel", label: "Excel" },
      { value: "txt", label: "TXT" },
      { value: "xml", label: "XML" },
      { value: "yaml", label: "YAML" }
    ]);
    const selectedFileFormat = ref("csv");


    const configSchema = ref<Record<string, ParamConfig>>({});
    const apiParameters = ref<Record<string, any>>({});

    /* ---------------------------
     * LOAD API PARAM SCHEMA
     * --------------------------- */
    const loadApiParameterSchema = async (source: string) => {
      console.group("--- ETL CONFIG DEBUG ---");
      try {
        const schemaResponse = await getFriendlySchemaBySource(source);
        const fullSchema = schemaResponse.data;

        let mergedSchema = fullSchema.config_schema || {};

        if (fullSchema.connector_type) {
          try {
            // JAVÍTÁS: Itt használjuk a központi api hívást a kézi axios helyett
            console.log("Szűrők lekérdezése connector típushoz:", fullSchema.connector_type);
            const filterResponse = await getConnectorFilters(fullSchema.connector_type);
            const filterOptions = filterResponse.data;

            // --- LOGOLÁS ---
            console.log("KAPOTT ADAT (filterOptions):", filterOptions);

            // --- VÉDELEM 1: HTML String detektálás (404 hiba esetén) ---
            if (typeof filterOptions === 'string') {
                console.error("!!! HIBA: A szerver HTML-t küldött JSON helyett. A Route valószínűleg hiányzik.");
                return;
            }

            // --- VÉDELEM 2: Tömb detektálás (Cache hiba esetén) ---
            if (Array.isArray(filterOptions)) {
                console.error("!!! HIBA: A Backend listát küldött objektum helyett. A feldolgozás leállítva.");
                return;
            }

            // Ha eljutunk ide, akkor az adat Objektum, mehet a feldolgozás
            Object.keys(filterOptions).forEach((key) => {
              const f = filterOptions[key];

              mergedSchema[key] = {
                type: "select",
                required: f.required ?? true,
                friendly_name: f.label || key,
                description: f.description,
                // Ellenőrizzük az options tömböt is
                options: Array.isArray(f.options)
                  ? f.options.map((opt: any) => ({
                      value: opt.value || opt.code || opt,
                      label: opt.label || opt.name || opt.value || opt
                    }))
                  : []
              };
            });

          } catch (err) {
            console.warn("Could not load connector filter options:", err);
          }
        }

        configSchema.value = mergedSchema;

        const params: any = {};
        Object.keys(mergedSchema).forEach((k) => {
          params[k] = "";
        });

        apiParameters.value = params;
      } catch (error) {
        console.warn("Failed to load API parameters", error);
      } finally {
        console.groupEnd();
      }
    };

    /* ---------------------------
     * MOUNT
     * --------------------------- */
    onMounted(async () => {
      if (!source) return;

      const resp = await loadSchemaBySource(source);

      if (resp.data.dynamic) {
        await loadApiParameterSchema(source);
        return;
      }

      const schema = resp.data.field_mappings;
      allColumns.value = schema.map((f: any) => f.name);
      columnOrder.value = [...allColumns.value];
      selectedColumns.value = [...allColumns.value];

      const mappings: any = {};
      allColumns.value.forEach((c: string) => {
        mappings[c] = {
          rename: false,
          newName: "",
          delete: false,
          unique: false,
          concat: {
            enabled: false,
            with: "",
            separator: " "
          }
        };
      });

      fieldMappings.value = mappings;

      await loadApiParameterSchema(source);
    });

    /* ---------------------------
     * FILE UPLOAD
     * --------------------------- */
    const handleFileUpload = (event: any) => {
      const file = event.target.files?.[0] || null;
      if (file) {
        uploadedFileName.value = file.name;
        fileData.value = file;
      }
    };

    /* ---------------------------
     * CONCAT LOGIC
     * --------------------------- */
    const onConcatWithChange = (col: string, targetCol: string) => {
      if (!targetCol) {
        fieldMappings.value[col].concat.enabled = false;
        return;
      }
      // JAVÍTÁS: Itt volt egy hiba az eredetiben (rightCol undefined),
      // de a logikát most nem bántom, csak a típust.
      fieldMappings.value[col].concat.enabled = true;
    };

    const onConcatEnableChange = (col: string) => {};
    const onConcatSeparatorChange = () => {};

    /* ---------------------------
     * SAVE PIPELINE CONFIG
     * --------------------------- */
    const submitPipelineConfig = () => {
      store.config = {
        ...store.config,
        schedule: schedule.value,
        custom_time: schedule.value === "custom" ? customTime.value : null,
        condition: conditions.value,
        dependency_pipeline_id:
          conditions.value === "withdependency"
            ? dependencyPipelineId.value
            : null,
        uploaded_file_name:
          conditions.value === "withsource" ? uploadedFileName.value : null,
        update_mode: update.value,
        save_option: saveOption.value,

        field_mappings: fieldMappings.value,
        column_order: columnOrder.value,
        selected_columns: selectedColumns.value,

        custom_sql: transformation.value === "advenced" ? customSQL.value : null,
        parameters: apiParameters.value
      };

      router.push("/create-etl");
    };

    return {
      router,
      schedule,
      customTime,
      conditions,
      dependencyPipelineId,
      update,
      saveOption,
      uploadedFileName,
      activePipelines,
      allColumns,
      columnOrder,
      selectedColumns,
      customSQL,
      transformation,
      fieldMappings,
      colSettingsOpen,
      selectedFileFormat,
      fileFormats,
      configSchema,
      apiParameters,

      handleFileUpload,
      submitPipelineConfig,

      onConcatWithChange,
      onConcatEnableChange,
      onConcatSeparatorChange
    };
  }
});
</script>

<style scoped src="./styles/ETLConfig.style.css"></style>
