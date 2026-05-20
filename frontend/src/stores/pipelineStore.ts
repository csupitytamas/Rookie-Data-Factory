/**
A fájl a konfigurációl létrehozásához és szerkesztéséhez használt központi Pinia store tartalmazza.
 */
import { defineStore } from 'pinia'

// A pipeline konfigurációs adatait leíró interfész.
export interface ConfigData {
  id?: number | null;
  schedule: string;
  custom_time?: string | null;
  condition?: string | null;
  dependency_pipeline_id?: string | null;
  uploaded_file_path?: string | null;
  uploaded_file_name?: string | null;
  field_mappings?: Record<string, any>;
  transformation?: Record<string, any>;
  group_by_columns?: string[];
  order_by_column?: string | null;
  order_direction?: string | null;
  limit_rows?: number | null;
  custom_sql?: string | null;
  column_order?: string[];
  update_mode: string;
  save_option: string;
  file_format?: string | null;
  parameters?: Record<string, any>; 
}

// Segédfüggvény az alapértelmezett konfigurációs értékek létrehozásához.
const createDefaultConfig = (): ConfigData => ({
  id: null,
  schedule: '@daily',
  custom_time: null,
  condition: 'none',
  dependency_pipeline_id: null,
  uploaded_file_path: null,
  uploaded_file_name: null,
  field_mappings: {},
  transformation: { type: 'select' },
  group_by_columns: [],
  order_by_column: null,
  order_direction: 'asc',
  limit_rows: null,
  custom_sql: null,
  column_order: [],
  update_mode: 'append',
  save_option: 'todatabase',
  file_format: null,
  parameters: {},
});

// A pipeline-ok létrehozásához és szerkesztéséhez használt központi Pinia store.
export const usePipelineStore = defineStore('pipeline', {
  state: () => ({
    pipeline_name: '' as string,
    source: '' as string,
    config: createDefaultConfig(),
  }),

  // Műveletek a store állapotának módosításához.
  actions: {
    reset() {
      console.log("Store Reset: Clearing all data.");
      this.pipeline_name = '';
      this.source = '';
      this.config = createDefaultConfig();
    },

    // Az adott wizard lépéshez tartozó adatok törlése a navigáció során (a folyamatból való kilépés esetén).
    clearStepData(step: number) {
      console.log(`Clearing data for step ${step}...`);
      if (step === 2) {
        this.config.parameters = {};
      }
      else if (step === 3) {
        this.config.custom_time = null;
        this.config.condition = 'none';
        this.config.dependency_pipeline_id = null;
      }
      else if (step === 4) {
        this.config.field_mappings = {};
        this.config.column_order = [];
        this.config.uploaded_file_path = null;
        this.config.uploaded_file_name = null;
      }
      else if (step === 5) {
        this.config.transformation = { type: 'select' };
        this.config.group_by_columns = [];
        this.config.order_by_column = null;
        this.config.order_direction = 'asc';
        this.config.limit_rows = null;
        this.config.custom_sql = null;
      }
      else if (step === 6) {
        this.config.update_mode = 'append';
        this.config.save_option = 'todatabase';
      }
    }
  }
})
