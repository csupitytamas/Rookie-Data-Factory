<template>
  <div class="form-layout">
    <h3 class="title">API Settings ({{ store.source }})</h3>

    <div class="content-width">
      <div v-if="loading" class="status-msg">
        <p>Loading parameters...</p>
      </div>

      <div v-else-if="apiError" class="error-container">
        <div class="error-box">
          <span class="error-icon">⚠️</span>
          <div class="error-text">
            <strong>Data Not Found</strong>
            <p>{{ apiError }}</p>
          </div>
        </div>
        <button @click="loadApiParameterSchema(store.source)" class="retry-btn">
          Try Again
        </button>
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
import { ref, onMounted, watch } from 'vue';
import { usePipelineStore } from '@/stores/pipelineStore';
import { getFriendlySchemaBySource, getConnectorFilters } from '@/api/pipeline';

const store = usePipelineStore();
const configSchema = ref<any>({});
const connectorType = ref<string | null>(null);
const loading = ref(false);
const apiError = ref<string | null>(null);

// ÚJ: Figyeljük a paraméterek változását (pl. ha választ egy meetinget)
watch(() => ({ ...store.config.parameters }), async (newParams, oldParams) => {
  // Ha a sportág megváltozott, töröljük a ligát és a keresést
  if (newParams.sport_type !== oldParams.sport_type) {
    console.log("Sport type changed, clearing selection...");
    store.config.parameters.league_id = "";
    store.config.parameters.league_search = "";
    // Várjunk egy picit a Store frissülésre, mielőtt kérünk újat
    setTimeout(() => refreshFilters(connectorType.value!, store.config.parameters), 100);
    return;
  }

  // Csak akkor frissítünk, ha VAN connector_type és VALÓBAN változott valami fontos
  if (connectorType.value && JSON.stringify(newParams) !== JSON.stringify(oldParams)) {
    console.log("Parameters changed, refreshing filters...");
    await refreshFilters(connectorType.value, newParams);
  }
}, { deep: true });

const refreshFilters = async (type: string, params: any) => {
  try {
    const filterResponse = await getConnectorFilters(type, params);
    const filterOptions = filterResponse.data;
    
    if (filterOptions) {
      Object.keys(filterOptions).forEach((key) => {
        if (configSchema.value[key]) {
          configSchema.value[key].options = Array.isArray(filterOptions[key].options)
            ? filterOptions[key].options.map((opt: any) => ({
                value: opt.value || opt.code || opt,
                label: opt.label || opt.name || opt.value || opt
              }))
            : [];
        }
      });
    }
  } catch (err) {
    console.warn("Failed to refresh filters:", err);
  }
};

const loadApiParameterSchema = async (source: string) => {
  loading.value = true;
  apiError.value = null;
  try {
    const schemaResponse = await getFriendlySchemaBySource(source);
    const fullSchema = schemaResponse.data;
    connectorType.value = fullSchema.connector_type;
    let mergedSchema = fullSchema.config_schema || {};

    if (fullSchema.connector_type) {
      try {
        // Kezdeti betöltés az esetlegesen már meglévő paraméterekkel
        const filterResponse = await getConnectorFilters(fullSchema.connector_type, store.config.parameters);
        const filterOptions = filterResponse.data;
        if (filterOptions && typeof filterOptions === 'object' && !Array.isArray(filterOptions)) {
          Object.keys(filterOptions).forEach((key) => {
            const f = filterOptions[key];
            mergedSchema[key] = {
              type: f.type || "select", // JAVÍTÁS: Használjuk a backend által küldött típust!
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
      } catch (err: any) {
        if (err.response && err.response.status === 400) {
          apiError.value = err.response.data.detail;
        } else {
          console.warn("Connector filter error:", err);
        }
      }
    }
    configSchema.value = mergedSchema;
    if (!store.config.parameters) store.config.parameters = {};
  } catch (error) {
    console.error("Error loading parameters:", error);
    apiError.value = "Critical error loading source schema.";
  } finally {
    loading.value = false;
  }
};

onMounted(() => {
  if (store.source) loadApiParameterSchema(store.source);
});
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
  min-width: 350px;
  padding-bottom: 200px;
}

.settings-box {
  background: #f9f9f9;
  border: 1px solid #e0e0e0;
  padding: 30px;
  border-radius: 10px;
}

.form-row { margin-bottom: 25px; text-align: left; }

.form-control {
  width: 100%;
  padding: 12px;
  border: 1px solid #ccc;
  border-radius: 6px;
}

.error-container {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 15px;
  margin-top: 20px;
}

.error-box {
  background-color: #fff3f3;
  border: 1px solid #facccc;
  border-radius: 8px;
  padding: 20px;
  display: flex;
  gap: 15px;
  width: 100%;
  text-align: left;
}

.error-icon { font-size: 24px; }
.error-text strong { color: #c0392b; display: block; }
.retry-btn {
  background-color: #3498db;
  color: white;
  border: none;
  padding: 10px 25px;
  border-radius: 6px;
  cursor: pointer;
}
</style>