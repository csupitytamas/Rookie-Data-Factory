import { defineStore } from 'pinia'

export interface ConfigData {
  schedule: string;
  custom_time?: string | null;
  condition?: string | null;
  dependency_pipeline_id?: string | null;
  uploaded_file_path?: string | null;
  uploaded_file_name?: string | null;
  field_mappings?: Record<string, any>;
  transformation?: Record<string, any>;
  selected_columns?: string[];
  group_by_columns?: string[];
  order_by_column?: string | null;
  order_direction?: string | null;
  custom_sql?: string | null;
  column_order?: string[];
  update_mode: string;
  save_option: string;
  file_format?: string | null;
  parameters?: Record<string, any>; // Connector paraméterek (pl. {"indicator": "SP.POP.TOTL", "country": "USA"})
}

const DEFAULT_CONFIG: ConfigData = {
  schedule: 'daily',
  custom_time: null,
  condition: null,
  dependency_pipeline_id: null,
  uploaded_file_path: null,
  uploaded_file_name: null,
  field_mappings: {},
  transformation: {},
  selected_columns: [],
  group_by_columns: [],
  order_by_column: null,
  order_direction: null,
  custom_sql: null,
  column_order: [],
  update_mode: 'append',
  save_option: 'todatabase',
  file_format: null,
  parameters: {},
}

export const usePipelineStore = defineStore('pipeline', {
  state: () => ({
    pipeline_name: '' as string,
    source: '' as string,
    config: { ...DEFAULT_CONFIG } as ConfigData,
  }),
  actions: {
    defaultConfig(): ConfigData {
      return { ...DEFAULT_CONFIG }
    },
    reset() {
      this.pipeline_name = ''
      this.source = ''
      this.config = this.defaultConfig()
    }
  }
})