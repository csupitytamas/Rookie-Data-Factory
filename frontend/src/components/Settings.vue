<template>
  <div class="settings-container">
    <h2>System settings</h2>

    <div v-if="loading" class="loading-msg">Loading...</div>

    <div v-else>
      <div class="form-group">
        <label>Time Zone:</label>
        <select v-model="settings.timezone" class="input-field">
          <option value="UTC">UTC</option>
          <option value="Europe/Budapest">Europe/Budapest (Budapest)</option>
          <option value="Europe/London">Europe/London</option>
          <option value="America/New_York">America/New_York</option>
          <option value="Asia/Tokyo">Asia/Tokyo</option>
        </select>
      </div>

      <div class="form-group">
        <label>Default save path:</label>
        <div class="path-selector">
          <input 
            type="text" 
            v-model="settings.download_path" 
            readonly 
            placeholder="No selected path" 
            class="input-field path-input"
          />
          <button @click="selectFolder" class="btn-browse"> 📂</button>
        </div>
      </div>

      <div class="form-group action-group">
        <button @click="saveSettings" class="btn-save" :disabled="saving">
          {{ saving ? 'Saving..' : 'Save' }}
        </button>
        <p v-if="message" :class="{'success-msg': success, 'error-msg': !success}">
          {{ message }}
        </p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import axios from 'axios';
const API_URL = 'http://localhost:8000/etl/settings'; 

const settings = ref({
  timezone: 'Europe/Budapest',
  download_path: '' 
});

const loading = ref(true);
const saving = ref(false);
const message = ref("");
const success = ref(false);

onMounted(async () => {
  try {
    const response = await axios.get(API_URL);
    if (response.data) {
        settings.value.timezone = response.data.timezone || 'Europe/Budapest';
        settings.value.download_path = response.data.download_path || '';
    }
  } catch (error) {
  } finally {
    loading.value = false;
  }
});

const selectFolder = async () => {
  if (window.electron && window.electron.selectFolder) {
    try {
      const path = await window.electron.selectFolder();
      if (path) {
        settings.value.download_path = path;
      }
    } catch (err) {
}
  }
};

const saveSettings = async () => {
  saving.value = true;
  message.value = "";
  
  try {
    await axios.put(API_URL, settings.value);
    showMessage("Success!", true);
  } catch (error) {
    console.error("Save error:", error);
    showMessage("Error", false);
  } finally {
    saving.value = false;
  }
};

const showMessage = (msg, isSuccess) => {
  message.value = msg;
  success.value = isSuccess;
  setTimeout(() => { message.value = ""; }, 3000);
};
</script>

<style scoped>
.settings-container {
  width: 100%;
  max-width: 700px;
  margin: 40px auto;
  padding: 30px;
  background: #ffffff;
  border-radius: 12px;
  box-shadow: 0 4px 20px rgba(0,0,0,0.08);
  font-family: "Segoe UI", sans-serif;
  color: #333;
}
.path-selector {
  display: flex;
  gap: 10px;
}
.path-input {
  flex-grow: 1;
  background-color: #f8f9fa; 
  cursor: default;
}
.btn-browse {
  background-color: #6c757d;
  color: white;
  border: none;
  border-radius: 6px;
  padding: 0 15px;
  cursor: pointer;
  font-weight: 500;
  white-space: nowrap;
}
.btn-browse:hover { background-color: #5a6268; }
.btn-save {
  background-color: #28a745;
  color: white;
  padding: 12px 30px;
  font-size: 16px;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-weight: bold;
  width: 100%;
  transition: background 0.2s;
}
.btn-save:hover { background-color: #218838; }
.success-msg { color: #28a745; margin-top: 15px; font-weight: bold; text-align: center; }
.error-msg { color: #dc3545; margin-top: 15px; font-weight: bold; text-align: center; }
.form-group { margin-bottom: 25px; display: flex; flex-direction: column; }
label { font-weight: 600; margin-bottom: 8px; color: #555; }
.input-field { padding: 10px 12px; font-size: 15px; border-radius: 6px; border: 1px solid #ced4da; }
</style>