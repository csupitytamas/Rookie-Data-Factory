<template>
  <div class="container">
    <h2>NEW JOB</h2>

    <div class="progress-indicator">
      Step: {{ currentStep }} / 6
    </div>

    <div class="step-content">
      <BasicSettings v-if="currentStep === 1" />
      <ApiSettings v-if="currentStep === 2" />
      <ScheduleSettings v-if="currentStep === 3" />
      <FieldMapping v-if="currentStep === 4" />
      <TransformSettings v-if="currentStep === 5" />
      <SaveOptions v-if="currentStep === 6" />
    </div>

    <div class="button-row" style="margin-top: 20px;">
      <button 
        v-if="currentStep > 1" 
        type="button" 
        class="secondary" 
        @click="handleBack"
      >
        Back
      </button>

      <div v-else></div> 

      <button 
        v-if="currentStep < 6" 
        type="button" 
        class="primary" 
        @click="currentStep++"
      >
        Next
      </button>

      <button 
        v-if="currentStep === 6" 
        type="button" 
        class="primary" 
        @click="submitPipeline"
      >
        Create Pipeline
      </button>
    </div>
  </div>
</template>

<script>
import { createPipeline } from '@/api/pipeline';
import { usePipelineStore } from '@/stores/pipelineStore';

import BasicSettings from './wizard/BasicSettings.vue';
import ApiSettings from './wizard/ApiSettings.vue';
import ScheduleSettings from './wizard/ScheduleSettings.vue';
import FieldMapping from './wizard/FieldMapping.vue';
import TransformSettings from './wizard/TransformSettings.vue';
import SaveOptions from './wizard/SaveOptions.vue';

export default {
  components: {
    BasicSettings,
    ApiSettings,
    ScheduleSettings,
    FieldMapping,
    TransformSettings,
    SaveOptions
  },
  data() {
    return {
      currentStep: 1
    };
  },
  computed: {
    store() {
      return usePipelineStore();
    }
  },
  methods: {
    // --- ÚJ METÓDUS: Visszalépés törléssel ---
    handleBack() {
      // Töröljük a JELENLEGI lépés adatait, mert elhagyjuk "visszafelé"
      // Pl. Ha a 2. lépésnél vagyunk és visszamegyünk az 1-esre, 
      // akkor a 2-es (API paraméterek) törlődnek.
      this.store.clearStepData(this.currentStep);
      
      // Ezután lépünk vissza
      this.currentStep--;
    },
    // -----------------------------------------

    submitPipeline() {
      const payload = {
        pipeline_name: this.store.pipeline_name,
        source: this.store.source,
        ...this.store.config
      };
      
      console.log("Final payload:", payload);
      
      createPipeline(payload)
        .then(response => {
          alert('Successfully created!');
          this.store.reset();
          this.$router.push('/');
        })
        .catch(error => {
          console.error(error);
          alert('Error creating pipeline!');
        });
    }
  }
};
</script>

<style scoped src="./styles/CreateETLPipeline.style.css"></style>
<style scoped>
.progress-indicator {
  text-align: center;
  margin-bottom: 20px;
  font-weight: bold;
  color: #666;
}
</style>