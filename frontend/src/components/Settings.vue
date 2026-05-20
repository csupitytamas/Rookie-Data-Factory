<!-- A fájl a rendszerbeállítások kezeléséért felelős komponens. -->
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
        <button @click="saveSettings" class="btn-save btn-success" :disabled="saving">
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

// A rendszer által támogatott időzónák listáját tároló reaktív változó.
const timezones = ref([]);

// Megpróbáljuk lekérni az összes érvényes időzónát a böngészőből/rendszerből; hiba esetén alapértelmezett értékeket használunk.
try {
  timezones.value = Intl.supportedValuesOf('timeZone');
} catch (e) {
  timezones.value = ['UTC', 'Europe/Budapest'];
}

const settings = ref({ timezone: 'Europe/Budapest', download_path: '' });
const loading = ref(true);
const saving = ref(false);
const message = ref("");
const success = ref(false);

// A komponens betöltésekor inicializáljuk az aktuális beállításokat a backendről.
onMounted(async () => {

  // Aktuális időzóna és mentési útvonal lekérése az API-n keresztül.
  try {
    const response = await axios.get(API_URL);
    if (response.data) {
        settings.value.timezone = response.data.timezone || 'Europe/Budapest';
        settings.value.download_path = response.data.download_path || '';
    }
  } catch (error) {
    console.error(error);
  } finally {
    loading.value = false;
  }
});


// Mappaválasztó párbeszédablak megnyitása az Electron segítségével a letöltési útvonal beállításához.
const selectFolder = async () => {
  if (window.electron?.selectFolder) {
    const path = await window.electron.selectFolder();
    if (path) settings.value.download_path = path;
  }
};


// A módosított beállítások mentése a backend adatbázisába és a helyi mappaszerkezet inicializálása.
const saveSettings = async () => {
  saving.value = true;

  // Beállítások frissítése a backend felé küldött PUT kéréssel.
  try {
    await axios.put(API_URL, settings.value);

    // Sikeres mentés után létrehozzuk a szükséges alkönyvtárakat (Results, Logs) a választott útvonalon.
    if (window.electron?.createDirectories && settings.value.download_path) {
      await window.electron.createDirectories(settings.value.download_path);
    }
    success.value = true;
    message.value = "Success!";
    setTimeout(() => { message.value = ""; }, 3000);
  } catch (error) {
    success.value = false;
    message.value = "Error";
  } finally {
    saving.value = false;
  }
};
</script>

<style src="./styles/Settings.css"></style>
