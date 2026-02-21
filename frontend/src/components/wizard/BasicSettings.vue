<template>
  <div class="form-layout">
    <h3>Basic Settings</h3>

    <div class="content-width">
      <div class="settings-box">
        <div class="form-row">
          <label for="pipelineName">Job Name:</label>
          <input 
            v-model="pipelineName" 
            type="text" 
            id="pipelineName" 
            class="form-control"
            placeholder="Enter the name" 
            required 
          />
        </div>

        <div class="form-row mt-3">
          <label for="source">Select Data Source:</label>
          <select v-model="selectedSource" id="source" class="form-control">
            <option disabled value="">Please select a source</option>
            <option
              v-for="item in sources"
              :key="item.source"
              :value="item.source"
              :title="item.description || ''"
            >
              {{ item.alias || item.source }}
            </option>
          </select>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { getAvailableSources } from '@/api/pipeline';
import { usePipelineStore } from '@/stores/pipelineStore';

export default {
  name: 'BasicSettings',
  data() {
    return {
      sources: [],
    };
  },
  computed: {
    store() {
      return usePipelineStore();
    },
    pipelineName: {
      get() { return this.store.pipeline_name; },
      set(value) { this.store.pipeline_name = value; }
    },
    selectedSource: {
      get() { return this.store.source; },
      set(value) { this.store.source = value; }
    }
  },
  mounted() {
    this.fetchSources();
  },
  methods: {
    async fetchSources() {
      try {
        const response = await getAvailableSources();
        this.sources = response.data;
      } catch (err) {
        console.error("Can't load the sources:", err);
      }
    }
  }
};
</script>

<style scoped>
/* KONZISZTENS 70%-OS DIZÁJN */
.form-layout {
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 100%;
}

.content-width {
  width: 70%;
  min-width: 320px;
}

h3 {
  margin-bottom: 20px;
  text-align: center;
  color: #333;
}

.settings-box {
  background: #f9f9f9;
  border: 1px solid #e0e0e0;
  padding: 25px;
  border-radius: 10px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.02);
  width: 100%;
  box-sizing: border-box;
}

.form-row {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-bottom: 15px;
  text-align: left;
}

label {
  display: block;
  font-weight: 600;
  margin-bottom: 4px;
  color: #444;
  font-size: 14px;
}

.form-control {
  width: 100%;
  padding: 12px;
  border: 1px solid #ccc;
  border-radius: 6px;
  font-size: 15px;
  font-family: inherit;
  background-color: #fff;
  box-sizing: border-box;
  transition: border-color 0.2s, box-shadow 0.2s;
}

.form-control:focus {
  border-color: #007bff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(0, 123, 255, 0.1);
}

.mt-3 {
  margin-top: 20px;
}
</style>