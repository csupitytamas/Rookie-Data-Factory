<template>
  <div class="settings-container">
    <h2>System settings</h2>

    <div v-if="loading" class="loading-msg">Loading...</div>

    <div v-else>
      <div class="form-group">
        <label>Time Zone:</label>
        <select v-model="settings.timezone" class="input-field">
          <option v-for="tz in timezones" :key="tz" :value="tz">
            {{ tz.replace(/_/g, ' ') }}
          </option>
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

// Időzónák lekérése a böngészőből (Intl API)
const timezones = ref([]);
try {
  timezones.value = Intl.supportedValuesOf('timeZone');
} catch (e) {
  // Fallback megoldás arra a ritka esetre, ha a böngésző nem támogatná
  timezones.value = ['UTC', 'Europe/Budapest', 'Europe/London', 'America/New_York', 'Asia/Tokyo'];
}

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
        // Ha valamiért olyan időzóna jön a backendből, ami nincs a listában, hozzáadjuk
        if (response.data.timezone && !timezones.value.includes(response.data.timezone)) {
          timezones.value.push(response.data.timezone);
          timezones.value.sort();
        }
        settings.value.timezone = response.data.timezone || 'Europe/Budapest';
        settings.value.download_path = response.data.download_path || '';
    }
  } catch (error) {
    console.error("Hiba a beállítások betöltésekor:", error);
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
      console.error("Hiba a mappa kiválasztásakor:", err);
    }
  }
};

const saveSettings = async () => {
  saving.value = true;
  message.value = "";
  
  try {
    // 1. Beállítások mentése a backend felé
    await axios.put(API_URL, settings.value);
    
    // 2. Mappák automatikus létrehozása az Electron segítségével
    if (window.electron && window.electron.createDirectories && settings.value.download_path) {
      const dirResult = await window.electron.createDirectories(settings.value.download_path);
      if (dirResult && !dirResult.success) {
        console.error("Hiba a mappák létrehozásakor:", dirResult.error);
      }
    }

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