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
      <keep-alive>
        <component :is="currentStepComponent" />
      </keep-alive>
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
import { defineComponent, ref, computed } from 'vue';
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

    const steps = ['Basic', 'Source', 'Schedule', 'Mapping', 'Transform', 'Output'];
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
      if (confirm("You going to exit. Are you sure?")) {
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

<style scoped>

.wizard-container {
  position: relative; 
  max-width: 1000px;
  margin: 0 auto;
  background: #fff;
  padding: 30px;
  border-radius: 8px;
  box-shadow: 0 4px 15px rgba(0,0,0,0.08);
  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
}

.close-btn {
  position: absolute;
  top: 15px;
  right: 20px;
  background: transparent;
  border: none;
  font-size: 28px;
  color: #aaa;
  cursor: pointer;
  line-height: 1;
  padding: 0;
  transition: color 0.2s;
  z-index: 10;
}

.close-btn:hover {
  color: #dc3545; 
}

.wizard-header-custom {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 40px;
  padding: 0 10px;
}

.step-item {
  display: flex;
  align-items: center;
  flex: 1;
}

.step-item:last-child {
  flex: 0;
}

.step-circle {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  background-color: #e0e0e0;
  color: #fff;
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: bold;
  font-size: 14px;
  margin-right: 10px;
  transition: background-color 0.3s ease;
}

.step-label {
  color: #999;
  font-weight: 500;
  font-size: 14px;
  margin-right: 15px;
  white-space: nowrap;
}

.step-line {
  flex: 1;
  height: 2px;
  background-color: #e0e0e0;
  margin-right: 15px;
}

.step-item.active .step-circle {
  background-color: #007bff;
  box-shadow: 0 0 0 3px rgba(0, 123, 255, 0.2);
}
.step-item.active .step-label {
  color: #000;
  font-weight: bold;
}

.step-item.completed .step-circle {
  background-color: #28a745;
}
.step-item.completed .step-label {
  color: #28a745;
}
.step-item.completed .step-line {
  background-color: #28a745;
}

.wizard-content {
  min-height: 350px;
  margin-bottom: 20px;
}

.wizard-footer {
  display: flex;
  justify-content: space-between;
  margin-top: 30px;
  padding-top: 20px;
  border-top: 1px solid #eee;
}

.btn-primary { background: #007bff; color: white; padding: 10px 25px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
.btn-secondary { background: #6c757d; color: white; padding: 10px 25px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
.btn-success { background: #28a745; color: white; padding: 10px 25px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
.btn-primary:hover { background: #0056b3; }
.btn-success:hover { background: #218838; }
.spacer { flex: 1; }
</style>