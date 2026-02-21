<template>
  <div class="form-layout">
    <h3 class="title">API Settings ({{ store.source }})</h3>

    <div class="content-width">
      <div v-if="loading" class="status-msg">
        <p>Loading parameters...</p>
      </div>

      <div v-else-if="Object.keys(configSchema).length > 0" class="form-wrapper">
        <div class="settings-box">
          <div v-for="(paramConfig, paramKey) in configSchema" :key="paramKey" class="form-row">
            
            <label :for="`param-${paramKey}`">
              {{ paramConfig.friendly_name || paramConfig.label || paramKey }}
              <span v-if="paramConfig.required" class="required">*</span>
            </label>
            
            <p v-if="paramConfig.description" class="description-text">{{ paramConfig.description }}</p>

            <div v-if="paramConfig.type === 'select'">
              <select
                :id="`param-${paramKey}`"
                v-model="store.config.parameters[paramKey]"
                class="form-control"
              >
                <option value="" disabled selected>Please select...</option>
                <option 
                  v-for="opt in paramConfig.options" 
                  :key="opt.value" 
                  :value="opt.value"
                >
                  {{ opt.label }}
                </option>
              </select>
            </div>

            <div v-else-if="['number', 'integer'].includes(paramConfig.type)">
              <input
                :id="`param-${paramKey}`"
                type="number"
                v-model.number="store.config.parameters[paramKey]"
                class="form-control"
              />
            </div>

            <div v-else>
              <input
                :id="`param-${paramKey}`"
                type="text"
                v-model="store.config.parameters[paramKey]"
                class="form-control"
                placeholder="Enter value..."
              />
            </div>
          </div>
        </div>
      </div>

      <div v-else class="settings-box status-msg">
        <p>No parameters available for this source.</p>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue';
import { usePipelineStore } from '@/stores/pipelineStore';
import { getFriendlySchemaBySource, getConnectorFilters } from '@/api/pipeline';

const store = usePipelineStore();
const configSchema = ref({});
const loading = ref(false);

const loadApiParameterSchema = async (source: string) => {
  loading.value = true;
  try {
    const schemaResponse = await getFriendlySchemaBySource(source);
    const fullSchema = schemaResponse.data;
    let mergedSchema = fullSchema.config_schema || {};

    if (fullSchema.connector_type) {
      try {
        const filterResponse = await getConnectorFilters(fullSchema.connector_type);
        const filterOptions = filterResponse.data;
        if (filterOptions && typeof filterOptions === 'object' && !Array.isArray(filterOptions)) {
          Object.keys(filterOptions).forEach((key) => {
            const f = filterOptions[key];
            mergedSchema[key] = {
              type: "select",
              required: f.required ?? true,
              friendly_name: f.label || key,
              description: f.description,
              options: Array.isArray(f.options)
                ? f.options.map((opt: any) => ({
                    value: opt.value || opt.code || opt,
                    label: opt.label || opt.name || opt.value || opt
                  }))
                : []
            };
          });
        }
      } catch (err) {
        console.warn("Connector filter error:", err);
      }
    }
    configSchema.value = mergedSchema;
    if (!store.config.parameters) store.config.parameters = {};
  } catch (error) {
    console.error("Error loading parameters:", error);
  } finally {
    loading.value = false;
  }
};

onMounted(() => {
  if (store.source) loadApiParameterSchema(store.source);
});
</script>

<style scoped>
/* KONZISZTENS 70%-OS ELRENDEZÉS */
.form-layout {
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 100%;
}

.content-width {
  width: 70%;
  min-width: 350px;
  /* Extra hely alul a lenyílóknak */
  padding-bottom: 200px;
}

h3 {
  margin-bottom: 25px;
  text-align: center;
  color: #333;
}

/* KÁRTYA STÍLUS */
.settings-box {
  background: #f9f9f9;
  border: 1px solid #e0e0e0;
  padding: 30px;
  border-radius: 10px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.02);
  width: 100%;
  box-sizing: border-box;
}

.form-row {
  margin-bottom: 25px;
  text-align: left;
}

.form-row:last-child {
  margin-bottom: 0;
}

label {
  display: block;
  font-weight: 600;
  margin-bottom: 6px;
  color: #333;
  font-size: 15px;
}

/* MEZŐ STÍLUSOK */
.form-control {
  width: 100%;
  padding: 12px;
  border: 1px solid #ccc;
  border-radius: 6px;
  font-size: 15px;
  background-color: #fff;
  box-sizing: border-box;
  transition: border-color 0.2s, box-shadow 0.2s;
}

.form-control:focus {
  border-color: #007bff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(0, 123, 255, 0.1);
}

.description-text {
  font-size: 0.85em;
  color: #666;
  margin-bottom: 10px;
  margin-top: -2px;
  line-height: 1.4;
}

.required {
  color: #e74c3c;
  margin-left: 3px;
}

.status-msg {
  text-align: center;
  padding: 40px;
  color: #666;
  font-style: italic;
}

.mt-3 {
  margin-top: 20px;
}
</style>