import api from './axios';

const prefix = '/etl/pipeline';

export const createPipeline = (payload) =>
  api.post(`${prefix}/create`, payload);

export const getAllPipelines = () =>
  api.get(`${prefix}/all`);

export const updatePipeline = (id, payload) =>
  api.post(`${prefix}/updated_pipeline/${id}`, payload);

export const loadSchemaBySource = (payload) => {
  const data = typeof payload === 'string' 
    ? { source: payload, parameters: {} } 
    : payload;
 return api.post(`${prefix}/load-schema`, data);
};
export const getSchemaBySource = (source) =>
  api.get(`${prefix}/schema/${source}`);

export const getFriendlySchemaBySource = (source) =>
  api.get(`${prefix}/schema/${source}/friendly`);

export const getConnectorFilters = (connectorType) =>
  api.get(`${prefix}/connector/${connectorType}/filters`);

export const getAvailableSources = () =>
  api.get(`${prefix}/available-sources`);

export const loadPipelineData = (id) =>
  api.post(`${prefix}/load/${id}`);

export const uploadExtraFile = (formData) =>
  api.post(`${prefix}/upload-extra-file`, formData, {
    headers: {
      'Content-Type': 'multipart/form-data'
    }
  });
export const getPipelineColumns = (id) => api.get(`${prefix}/${id}/columns`);
export const getPipelineLog = (pipelineId, runId) =>
  api.get(`${prefix}/logs/${pipelineId}/${runId}`);

export const getPipelineHistory = () =>
  api.get(`${prefix}/history`);

export const getPipelineLogs = (pipelineId) =>
  api.get(`${prefix}/history/logs/${pipelineId}`);