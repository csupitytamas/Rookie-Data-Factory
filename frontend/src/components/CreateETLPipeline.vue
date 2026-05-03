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
  components: {
    BasicSettings,
    ApiSettings,
    ScheduleSettings,
    FieldMapping,
    TransformSettings,
    SaveOptions
  },
  setup() {
    const store = usePipelineStore();
    const router = useRouter();
    const currentStep = ref(1);

    onMounted(() => {
      console.log("Create Wizard Mounted: Clearing store data.");
      store.reset();
    });

    onUnmounted(() => {
      console.log("Create Wizard Unmounted: Clearing store data.");
      store.reset();
    });

    const steps = ['Basic', 'Source', 'Schedule', 'Mapping', 'Query', 'Output'];
    const componentList = [
      'BasicSettings',
      'ApiSettings',
      'ScheduleSettings',
      'FieldMapping',
      'TransformSettings',
      'SaveOptions'
    ];

    const currentStepComponent = computed(() => componentList[currentStep.value - 1]);

    const nextStep = () => {
      if (currentStep.value < steps.length) {
        currentStep.value++;
      }
    };

    const handleBack = () => {
      if (currentStep.value > 1) {
        store.clearStepData(currentStep.value);
        currentStep.value--;
      }
    };

    const closeWizard = () => {
      if (confirm("Do you want to exit?")) {
        store.reset(); 
        router.push('/'); 
      }
    };

    const submitPipeline = async () => {
      try {
        const payload = {
          pipeline_name: store.pipeline_name,
          source: store.source,
          ...store.config
        };
        
        console.log("Final payload:", payload);
        
        await createPipeline(payload);
        alert('Successfully created!');
        store.reset(); 
        router.push('/');
      } catch (error) {
        console.error(error);
        alert('Error!');
      }
    };

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
