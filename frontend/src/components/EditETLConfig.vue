<!-- A fájl a meglévő pipeline-ok szerkesztésére szolgáló felület. -->
<template>
  <div class="config-container wizard-container">
    <button class="close-btn" @click="closeEditor" title="Exit and clear data">×</button>

    <h2>EDIT THE CONFIGURATION</h2>

    <div class="wizard-header-custom">
      <div 
        v-for="(step, index) in steps" 
        :key="index"
        class="step-item"
        :class="{ 
          'active': currentStep === index + 1, 
          'completed': currentStep > index + 1 
        }"
      >
        <div class="step-circle">
          <span v-if="currentStep > index + 1">✓</span>
          <span v-else>{{ index + 1 }}</span>
        </div>
        
        <div class="step-label">{{ step }}</div>

        <div class="step-line" v-if="index < steps.length - 1"></div>
      </div>
    </div>

    <div class="wizard-content">
      <component :is="currentStepComponent" />
    </div>

    <div class="wizard-footer">
      <button 
        v-if="currentStep > 1" 
        @click="prevStep" 
        class="btn-secondary"
      >
        Back
      </button>
      
      <div class="spacer"></div>

      <button 
        v-if="currentStep < steps.length" 
        @click="nextStep" 
        class="btn-primary"
      >
        Next
      </button>

      <button 
        v-if="currentStep === steps.length" 
        @click="submitChanges" 
        class="btn-success"
      >
        Save Changes
      </button>
    </div>
  </div>
</template>

<script>
import { defineComponent, ref, computed, onMounted, onUnmounted } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { usePipelineStore } from '@/stores/pipelineStore';
import { loadPipelineData, updatePipeline } from '@/api/pipeline';
import ScheduleSettings from './wizard/ScheduleSettings.vue';
import FieldMapping from './wizard/FieldMapping.vue';
import TransformSettings from './wizard/TransformSettings.vue';
import SaveOptions from './wizard/SaveOptions.vue';
export default defineComponent({
  name: 'EditorETLConfig',
  components: {
    ScheduleSettings,
    FieldMapping,
    TransformSettings,
    SaveOptions
  },

  // A Composition API setup függvénye az állapotkezelés és a navigációs logika koordinálásához.
  setup() {
    const store = usePipelineStore();
    const route = useRoute();
    const router = useRouter();
    const pipelineId = route.query.id;
    const currentStep = ref(1);

    // Az állapotkezelő tisztítása a szerkesztő elhagyásakor.
    onUnmounted(() => {
      console.log("Editor Unmounted: resetting store.");
      store.reset();
    });

    // A szerkesztési folyamat lépéseinek és a hozzájuk tartozó komponenseknek a meghatározása.
    const steps = ['Schedule', 'Mapping', 'Query', 'Output'];
    const componentList = ['ScheduleSettings', 'FieldMapping', 'TransformSettings', 'SaveOptions'];

    // Számított tulajdonságok és navigációs metódusok a lépések közötti váltáshoz.
    const currentStepComponent = computed(() => componentList[currentStep.value - 1]);
    const nextStep = () => { if (currentStep.value < steps.length) currentStep.value++; };
    const prevStep = () => { if (currentStep.value > 1) currentStep.value--; };
    const closeEditor = () => {
      if (confirm("Are you sure you want to exit?")) {
        store.reset();
        router.push('/'); 
      }
    };

    // A komponens betöltésekor lekérjük a pipeline meglévő adatait az azonosító alapján.
    onMounted(async () => {
      if (!pipelineId) return;
      const pid = parseInt(pipelineId);
      if (store.config.id === pid) return;

      // API hívás a pipeline aktuális konfigurációjának betöltéséhez.
      try {
        const response = await loadPipelineData(pid);
        const pipeline = response.data;

        // A betöltött adatok szinkronizálása a globális Pinia store-ral a $patch metóduson keresztül.
        store.$patch({
          pipeline_name: pipeline.pipeline_name, 
          source: pipeline.source,               
          config: {
            id: pid,
            source_config: pipeline.source_config,
            parameters: pipeline.parameters,
            schedule: pipeline.schedule,
            custom_time: pipeline.custom_time,
            condition: pipeline.condition,
            dependency_pipeline_id: pipeline.dependency_pipeline_id,
            uploaded_file_name: pipeline.uploaded_file_name,
            update_mode: pipeline.update_mode,
            save_option: pipeline.save_option,
            field_mappings: pipeline.field_mappings || {},
            column_order: pipeline.column_order || [],     
            group_by_columns: pipeline.group_by_columns || [],
            order_by_column: pipeline.order_by_column,
            order_direction: pipeline.order_direction,
            custom_sql: pipeline.custom_sql,
            file_format: pipeline.file_format,
            transformation: pipeline.transformation || { type: 'none' }
          }
        });
      } catch (err) {
        alert("Failed to load");
      }
    });

    // A módosított konfiguráció mentése a backend felé.
    const submitChanges = async () => {

      // Frissítési kérés elküldése és visszatérés a dashboardra sikeres mentés után.
      try {
        await updatePipeline(pipelineId, store.config);
        alert('Pipeline updated successfully!');
        store.reset(); 
        router.push('/'); 
      } catch (err) {
        console.error("Error updating pipeline:", err);
        alert('Failed to update pipeline!');
      }
    };
    return { 
      currentStep, 
      steps, 
      currentStepComponent, 
      nextStep, 
      prevStep, 
      submitChanges, 
      closeEditor 
    };
  }
});
</script>

<style src="./styles/WizardContent.css"></style>
