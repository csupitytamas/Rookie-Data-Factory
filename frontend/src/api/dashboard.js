/**
    Ez a fájl a dashboarddal kapcsolatos API hívásokat tartalmazza.
*/

import api from './axios';
export const getDashboardPipelines = () => api.get('/etl/dashboard');
export const getPipelineData = (id, limit) => {
  const params = {};
  if (limit) {
    params.limit = limit;
  }
  return api.get(`/etl/dashboard/pipeline/${id}/data`, { params });
};
export const getPipelineHistory = (id) => api.get(`/etl/dashboard/pipeline/${id}/history`);
