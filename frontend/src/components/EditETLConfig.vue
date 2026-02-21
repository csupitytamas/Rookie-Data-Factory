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
      <keep-alive>
        <component :is="currentStepComponent" />
      </keep-alive>
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
import { defineComponent, ref, computed, onMounted } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { usePipelineStore } from '@/stores/pipelineStore';
import { loadPipelineData, updatePipeline, loadSchemaBySource } from '@/api/pipeline';

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
  setup() {
    const store = usePipelineStore();
    const route = useRoute();
    const router = useRouter();
    const pipelineId = route.query.id;
    const currentStep = ref(1);
    const steps = ['Schedule', 'Mapping', 'Transform', 'Output'];
    const componentList = ['ScheduleSettings', 'FieldMapping', 'TransformSettings', 'SaveOptions'];
    const currentStepComponent = computed(() => componentList[currentStep.value - 1]);
    
    const nextStep = () => { if (currentStep.value < steps.length) currentStep.value++; };
    const prevStep = () => { if (currentStep.value > 1) currentStep.value--; };
    const closeEditor = () => {
      if (confirm("Are you sure you want to exit?")) {
        store.reset();
        router.push('/'); 
      }
    };

    onMounted(async () => {
      if (!pipelineId) return;
      try {
        const response = await loadPipelineData(pipelineId);
        const pipeline = response.data;
        const mappings = pipeline.field_mappings || {};
        let savedOrder = pipeline.column_order || [];
        let selected = pipeline.selected_columns || [];
        let groups = pipeline.group_by_columns || [];

        if (pipeline.source && pipeline.parameters && Object.keys(pipeline.parameters).length > 0) {
          try {
            const schemaResponse = await loadSchemaBySource({
              source: pipeline.source,
              parameters: pipeline.parameters
            });
            const apiColumns = (schemaResponse.data.field_mappings || []).map(f => f.name);
            const newColumns = apiColumns.filter(col => !savedOrder.includes(col));
            savedOrder = [...savedOrder, ...newColumns];
            newColumns.forEach(col => {
                if (!mappings[col]) {
                    mappings[col] = { rename: false, newName: "", unique: false, delete: false };
                }
            });
          } catch (schemaErr) {
            console.warn("Could not fetch fresh schema:", schemaErr);
          }
        }

        store.$patch({
          pipeline_name: pipeline.pipeline_name, 
          source: pipeline.source,               
          config: {
            source_config: pipeline.source_config,
            parameters: pipeline.parameters,
            schedule: pipeline.schedule,
            custom_time: pipeline.custom_time,
            condition: pipeline.condition,
            dependency_pipeline_id: pipeline.dependency_pipeline_id,
            uploaded_file_name: pipeline.uploaded_file_name,
            update_mode: pipeline.update_mode,
            save_option: pipeline.save_option,
            field_mappings: mappings,
            column_order: savedOrder,     
            selected_columns: selected,  
            group_by_columns: groups,
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

    const submitChanges = async () => {
      try {
        const payload = { ...store.config };
        await updatePipeline(pipelineId, payload);
        alert('Pipeline updated successfully!');
        store.reset(); // Mentés után is takarítunk
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

<style scoped>
/* WIZARD CONTAINER - Fontos a relatív pozíció az X gombnak */
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

/* BEZÁRÓ GOMB STÍLUSA */
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

/* HEADER ÉS EGYÉB ELEMEK (Változatlanul) */
.wizard-header-custom {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 40px;
  padding: 0 10px;
}
/* ... A többi stílusod marad változatlanul ... */

.step-item { display: flex; align-items: center; flex: 1; }
.step-item:last-child { flex: 0; }
.step-circle { width: 32px; height: 32px; border-radius: 50%; background-color: #e0e0e0; color: #fff; display: flex; align-items: center; justify-content: center; font-weight: bold; font-size: 14px; margin-right: 10px; transition: background-color 0.3s ease; }
.step-label { color: #999; font-weight: 500; font-size: 14px; margin-right: 15px; white-space: nowrap; }
.step-line { flex: 1; height: 2px; background-color: #e0e0e0; margin-right: 15px; }
.step-item.active .step-circle { background-color: #007bff; box-shadow: 0 0 0 3px rgba(0, 123, 255, 0.2); }
.step-item.active .step-label { color: #000; font-weight: bold; }
.step-item.completed .step-circle { background-color: #28a745; }
.step-item.completed .step-label { color: #28a745; }
.step-item.completed .step-line { background-color: #28a745; }
.wizard-content { min-height: 300px; margin-bottom: 20px; }
.wizard-footer { display: flex; justify-content: space-between; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; }
.btn-primary { background: #007bff; color: white; padding: 10px 25px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
.btn-secondary { background: #6c757d; color: white; padding: 10px 25px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
.btn-success { background: #28a745; color: white; padding: 10px 25px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
.btn-primary:hover { background: #0056b3; }
.btn-success:hover { background: #218838; }
.spacer { flex: 1; }
</style>