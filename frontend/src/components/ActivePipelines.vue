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
  <button @click="goBack" class="action-button">Back</button>
  </div>
</template>

<script>
import {getAllPipelines} from "@/api/pipeline";

export default {
  name: "ActivePipelines",
  data() {
    return {
      pipelines: []
    };
  },
  created() {
    this.fetchPipelines();
  },
  methods: {
    async fetchPipelines() {
      try {
        const response = await getAllPipelines();
        this.pipelines = response.data.sort((a, b) => b.id - a.id);

      } catch (error) {
        console.error("Error fetching pipelines:", error);
      }
    },
    configurePipeline(pipeline) {
      this.$router.push({
        path: "/edit-config",
        query: { id: pipeline.id }
      });
    },
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
