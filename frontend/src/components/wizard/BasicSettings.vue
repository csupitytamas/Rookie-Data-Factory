<template>
  <div class="form-layout">
    <div class="form-row">
      <label for="pipelineName">Pipeline Name:</label>
      <input 
        v-model="pipelineName" 
        type="text" 
        id="pipelineName" 
        placeholder="Enter pipeline name" 
        required 
      />
    </div>

    <div class="form-row">
      <label for="source">Select Source:</label>
      <select v-model="selectedSource" id="source">
        <option disabled value="">Please select</option>
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
</template>

<script>
import { getAvailableSources } from '@/api/pipeline';
import { usePipelineStore } from '@/stores/pipelineStore';

export default {
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
<style scoped src="../styles/CreateETLPipeline.style.css"></style>