import api from './axios';
export const getDashboardPipelines = () => api.get('/etl/dashboard');
export const getPipelineData = (id, limit) => {
  const params = {};
  if (limit) {
    params.limit = limit;
  }
  // Ha a limit undefined/null, akkor nem küldjük a paramétert -> Backend mindent lekér
  return api.get(`/etl/dashboard/pipeline/${id}/data`, { params });
};
