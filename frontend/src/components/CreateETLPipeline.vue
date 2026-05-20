<!-- A fájl az új pipeline létrehozására szolgáló wizard fő komponense. -->
<template>
  <div class="config-container wizard-container">
    <button class="close-btn" @click="closeWizard" title="Exit">×</button>

    <h2>CREATE NEW JOB</h2>

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
        @click="handleBack"
        class="btn-secondary"
      >
        Back
      </button>
      
      <div class="spacer" v-if="currentStep === 1"></div>
      <div class="spacer" v-else></div>

      <button
        v-if="currentStep < steps.length"
        @click="nextStep"
        class="btn-primary"
      >
        Next
      </button>

      <button
        v-if="currentStep === steps.length"
        @click="submitPipeline"
        class="btn-success"
      >
        CREATE
      </button>
    </div>
  </div>
</template>

<script>
import { defineComponent, ref, computed, onMounted, onUnmounted } from 'vue';
import { useRouter } from 'vue-router';
import { createPipeline } from '@/api/pipeline';
import { usePipelineStore } from '@/stores/pipelineStore';
import BasicSettings from './wizard/BasicSettings.vue';
import ApiSettings from './wizard/ApiSettings.vue';
import ScheduleSettings from './wizard/ScheduleSettings.vue';
import FieldMapping from './wizard/FieldMapping.vue';
import TransformSettings from './wizard/TransformSettings.vue';
import SaveOptions from './wizard/SaveOptions.vue';

export default defineComponent({
  name: 'CreateETLPipeline',

  // A wizard egyes lépéseit reprezentáló alkomponensek regisztrálása.
  components: {
    BasicSettings,
    ApiSettings,
    ScheduleSettings,
    FieldMapping,
    TransformSettings,
    SaveOptions
  },

  // A Composition API setup függvénye a reaktív állapot és a navigációs logika kezeléséhez.
  setup() {
    const store = usePipelineStore();
    const router = useRouter();
    const currentStep = ref(1);

    // Az store alaphelyzetbe állítása a wizard megnyitásakor.
    onMounted(() => {
      store.reset();
    });

    // Az állapotkezelő tisztítása az oldal elhagyásakor a memóriaszivárgás megelőzése érdekében.
    onUnmounted(() => {
      store.reset();
    });

    // A wizard lépéseinek nevei és a hozzájuk tartozó komponensek listája.
    const steps = ['Basic', 'Source', 'Schedule', 'Mapping', 'Query', 'Output'];
    const componentList = [
      'BasicSettings',
      'ApiSettings',
      'ScheduleSettings',
      'FieldMapping',
      'TransformSettings',
      'SaveOptions'
    ];

    // Számított tulajdonság, amely meghatározza az aktuálisan megjelenítendő lépés komponensét.
    const currentStepComponent = computed(() => componentList[currentStep.value - 1]);

    // Navigáció a következő lépésre.
    const nextStep = () => {
      if (currentStep.value < steps.length) {
        currentStep.value++;
      }
    };

    // Visszalépés az előző lépésre, miközben az aktuális lépés adatait töröljük a store-ból.
    const handleBack = () => {
      if (currentStep.value > 1) {
        store.clearStepData(currentStep.value);
        currentStep.value--;
      }
    };

    // A wizard bezárása megerősítés után és visszatérés a dashboardra.
    const closeWizard = () => {
      if (confirm("Do you want to exit?")) {
        store.reset(); 
        router.push('/'); 
      }
    };

    // A teljes pipeline konfiguráció elküldése a backendnek mentésre.
    const submitPipeline = async () => {

      // Összeállítjuk a kérés törzsét a store-ban tárolt adatok alapján.
      try {
        const payload = {
          pipeline_name: store.pipeline_name,
          source: store.source,
          ...store.config
        };

        // Meghívjuk az API-t a pipeline létrehozásához.
        await createPipeline(payload);
        alert('Successfully created!');
        store.reset(); 
        router.push('/');
      } catch (error) {
        console.error(error);
        alert('Error!');
      }
    };

    // Exportáljuk a template számára elérhető változókat és függvényeket.
    return {
      currentStep,
      steps,
      currentStepComponent,
      nextStep,
      handleBack,
      closeWizard, 
      submitPipeline
    };
  }
});
</script>

<style src="./styles/WizardContent.css"></style>
