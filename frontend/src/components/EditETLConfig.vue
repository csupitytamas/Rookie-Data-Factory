<template>
  <div class="config-container wizard-container">
    <h2>Edit Pipeline Configuration</h2>

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
import { loadPipelineData, updatePipeline } from '@/api/pipeline';

// A közös wizard komponensek importálása
// (A BasicSettings és ApiSettings szándékosan hiányzik, hogy ne lehessen elrontani a forrást)
import FieldMapping from './wizard/FieldMapping.vue';
import TransformSettings from './wizard/TransformSettings.vue';
import ScheduleSettings from './wizard/ScheduleSettings.vue';
import SaveOptions from './wizard/SaveOptions.vue';

export default defineComponent({
  name: 'EditorETLConfig',
  components: {
    FieldMapping,
    TransformSettings,
    ScheduleSettings,
    SaveOptions
  },
  setup() {
    const store = usePipelineStore();
    const route = useRoute();
    const router = useRouter();
    const pipelineId = route.query.id;

    // --- LÉPÉSEK DEFINIÁLÁSA (4 Lépés) ---
    // Sorrend: Mapping -> Transform -> Schedule -> Output
    const currentStep = ref(1);
    const steps = ['Mapping', 'Transform', 'Schedule', 'Output'];
    const componentList = ['FieldMapping', 'TransformSettings', 'ScheduleSettings', 'SaveOptions'];

    const currentStepComponent = computed(() => {
      return componentList[currentStep.value - 1];
    });

    const nextStep = () => { if (currentStep.value < steps.length) currentStep.value++; };
    const prevStep = () => { if (currentStep.value > 1) currentStep.value--; };

    // --- ADATBETÖLTÉS ÉS MIGRÁCIÓ ---
    onMounted(async () => {
      if (!pipelineId) return;

      try {
        const response = await loadPipelineData(pipelineId);
        const pipeline = response.data;

        // 1. MIGRÁCIÓS LOGIKA (DIM1 -> DIMONE)
        // Ezt itt kell elvégezni, mielőtt betöltjük a Store-ba, 
        // hogy a FieldMapping komponens már a jó adatokat lássa.
        const mappings = pipeline.field_mappings || {};
        let order = pipeline.column_order || [];
        let selected = pipeline.selected_columns || [];
        let groups = pipeline.group_by_columns || [];

        if (mappings['dim1']) {
          console.log("Migrating dim1 to DIMONE in Editor...");
          mappings['DIMONE'] = { 
            ...mappings['dim1'], 
            rename: false, // Az új API-ban már eleve ez a neve
            newName: '' 
          };
          delete mappings['dim1'];

          // Listák frissítése
          const replaceItem = (arr, oldVal, newVal) => {
            const idx = arr.indexOf(oldVal);
            if (idx !== -1) arr[idx] = newVal;
          };

          replaceItem(order, 'dim1', 'DIMONE');
          replaceItem(selected, 'dim1', 'DIMONE');
          replaceItem(groups, 'dim1', 'DIMONE');

          if (pipeline.order_by_column === 'dim1') {
            pipeline.order_by_column = 'DIMONE';
          }
        }

        // 2. ADATOK BETÖLTÉSE A STORE-BA
        // A wizard komponensek innen fogják olvasni az adatokat
        store.$patch({
          pipeline_name: pipeline.pipeline_name, // Csak megjelenítéshez, szerkeszteni nem engedjük
          source: pipeline.source,               // Csak megjelenítéshez
          config: {
            source_config: pipeline.source_config,
            schedule: pipeline.schedule,
            custom_time: pipeline.custom_time,
            condition: pipeline.condition,
            dependency_pipeline_id: pipeline.dependency_pipeline_id,
            uploaded_file_name: pipeline.uploaded_file_name,
            update_mode: pipeline.update_mode,
            save_option: pipeline.save_option,
            field_mappings: mappings,
            column_order: order,
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
        console.error("Failed to load pipeline data:", err);
        alert("Hiba a pipeline betöltésekor!");
      }
    });

    // --- MENTÉS ---
    const submitChanges = async () => {
      try {
        // Összeállítjuk a payloadot a Store-ból
        // Figyelem: A pipeline_name és source nem változik, de a config igen
        const payload = {
          ...store.config
        };

        console.log("Updating pipeline with payload:", payload);

        await updatePipeline(pipelineId, payload);

        alert('Pipeline updated successfully!');
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
      submitChanges
    };
  }
});
</script>

<style scoped>
/* WIZARD CONTAINER (A dobozos kinézetért) */
.wizard-container {
  max-width: 1000px;
  margin: 0 auto;
  background: #fff;
  padding: 30px;
  border-radius: 8px;
  box-shadow: 0 4px 15px rgba(0,0,0,0.08);
  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
}

/* ================= HEADER STÍLUS (A KÉP ALAPJÁN) ================= */
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

/* Az utolsó elemnél ne nyúljon tovább a div */
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

/* AKTÍV LÉPÉS (Jelenlegi - KÉK) */
.step-item.active .step-circle {
  background-color: #007bff;
  box-shadow: 0 0 0 3px rgba(0, 123, 255, 0.2);
}
.step-item.active .step-label {
  color: #000;
  font-weight: bold;
}

/* BEFEJEZETT LÉPÉS (Már kész - ZÖLD) */
.step-item.completed .step-circle {
  background-color: #28a745;
}
.step-item.completed .step-label {
  color: #28a745;
}
.step-item.completed .step-line {
  background-color: #28a745; 
}

/* TARTALOM */
.wizard-content {
  min-height: 300px;
  margin-bottom: 20px;
}

/* GOMBOK */
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