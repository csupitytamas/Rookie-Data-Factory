<template>
  <div class="api-settings-container">
    <h3 class="title">API Settings ({{ store.source }})</h3>

    <div v-if="loading" class="status-msg">
      <p>Loading parameters...</p>
    </div>

    <div v-else-if="Object.keys(configSchema).length > 0" class="form-wrapper">
      
      <div v-for="(paramConfig, paramKey) in configSchema" :key="paramKey" class="custom-row">
        
        <label :for="`param-${paramKey}`" class="custom-label">
          {{ paramConfig.friendly_name || paramConfig.label || paramKey }}
          <span v-if="paramConfig.required" class="required">*</span>
        </label>
        
        <p v-if="paramConfig.description" class="description">{{ paramConfig.description }}</p>

        <div v-if="paramConfig.type === 'select'" class="input-wrapper">
          <select
            :id="`param-${paramKey}`"
            v-model="store.config.parameters[paramKey]"
            class="custom-input left-align"
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

        <div v-else-if="['number', 'integer'].includes(paramConfig.type)" class="input-wrapper">
          <input
            :id="`param-${paramKey}`"
            type="number"
            v-model.number="store.config.parameters[paramKey]"
            class="custom-input center-align"
          />
        </div>

        <div v-else class="input-wrapper">
          <input
            :id="`param-${paramKey}`"
            type="text"
            v-model="store.config.parameters[paramKey]"
            class="custom-input center-align"
          />
        </div>

      </div>
    </div>

    <div v-else class="status-msg">
      <p>No extra settings available for this source.</p>
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
.api-settings-container {
  width: 100%;
  display: flex;
  flex-direction: column;
  align-items: center;
  /* Extra hely alul, hogy a menünek legyen helye */
  padding-bottom: 300px; 
}

.title {
  text-align: center;
  margin-bottom: 25px;
  color: #333;
}

.form-wrapper {
  width: 100%;
  max-width: 600px;
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.custom-row {
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  width: 100%;
}

.custom-label {
  display: block;
  font-weight: 600;
  margin-bottom: 8px;
  color: #333;
  width: 100%;
  text-align: center;
}

/* --- ÚJ WRAPPER STRUKTÚRA --- */
.input-wrapper {
  /* Ez a doboz felel azért, hogy KÖZÉPEN legyen az űrlapon */
  width: 100%;
  max-width: 400px; 
  margin: 0 auto;
}

.custom-input {
  /* Az input kitölti a wrappert */
  width: 100%;
  padding: 10px;
  border: 1px solid #ccc;
  border-radius: 6px;
  font-size: 14px;
  background-color: white;
  box-sizing: border-box;
}

/* Selectnél: BALRA igazítjuk a szöveget */
/* Ez KÖTELEZŐ ahhoz, hogy a menü a mező bal szélétől nyíljon lefelé */
.left-align {
  text-align: left;
  cursor: pointer;
}

/* Sima inputnál: maradhat középen, az szebb */
.center-align {
  text-align: center;
}

.custom-input:focus {
  border-color: #007bff;
  outline: none;
}

.description {
  font-size: 0.85em;
  color: #666;
  margin-bottom: 8px;
  margin-top: -4px;
}

.required {
  color: #e74c3c;
  margin-left: 3px;
}

.status-msg {
  text-align: center;
  padding: 20px;
  color: #666;
  font-style: italic;
  background: #f8f9fa;
  border-radius: 8px;
  width: 100%;
}
</style>