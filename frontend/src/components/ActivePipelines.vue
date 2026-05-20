<!-- A fájl az aktív pipeline-ok listázásáért és kezeléséért felelős komponens. -->
<template>
  <div class="container">
    <h2>ACTIVE JOBS</h2>

    <table>
      <thead>
      <tr>
        <th>Name</th>
        <th>Source</th>
        <th>Configuration</th>
      </tr>
      </thead>
      <tbody>
      <tr v-for="(pipeline, index) in pipelines" :key="index">
        <td>{{ pipeline.pipeline_name }}</td>
        <td>{{ pipeline.alias || pipeline.source }}</td>
        <td>
          <button @click="configurePipeline(pipeline)" class="btn-icon">
            ⚙️
          </button>
        </td>
      </tr>
      </tbody>
    </table>
    <div class="footer">
      <router-link to="/" class="btn-back">← Back to Dashboard</router-link>
    </div>
  </div>
</template>

<script>
import {getAllPipelines} from "@/api/pipeline";

// A komponens felelős a jelenleg aktív (nem archivált) pipeline-ok megjelenítéséért és szerkesztésük elindításáért.
export default {
  name: "ActivePipelines",

  // Lokális állapot a lekérdezett pipeline-ok tárolására.
  data() {
    return {
      pipelines: []
    };
  },

  // A komponens példányosításakor azonnal lekérjük az adatokat a backendről.
  created() {
    this.fetchPipelines();
  },

  // Metódusok az adatok lekéréséhez és a navigációhoz.
  methods: {
    async fetchPipelines() {

      // Lekérjük az összes aktív pipeline-t az API-n keresztül, majd csökkenő sorrendbe rendezzük őket az azonosítójuk alapján.
      try {
        const response = await getAllPipelines();
        this.pipelines = response.data.sort((a, b) => b.id - a.id);
      } catch (error) {
        console.error("Error fetching pipelines:", error);
      }
    },

    // Átirányítja a felhasználót a szerkesztő (edit-config) nézetre a kiválasztott pipeline azonosítójával.
    configurePipeline(pipeline) {
      this.$router.push({
        path: "/edit-config",
        query: { id: pipeline.id }
      });
    },

    // Visszanavigál az előző oldalra, vagy adashboard), ha nincs előzmény.
    goBack() {
      if (window.history.length > 1) {
        this.$router.go(-1);
      } else {
        this.$router.push('/');
      }
    }
  }
};
</script>

<style src="./styles/ActivePipelines.css"></style>
