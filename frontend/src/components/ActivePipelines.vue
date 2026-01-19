<template>
  <div class="container">
    <h2>Active ETL Pipelines</h2>

    <table>
      <thead>
      <tr>
        <th>Name</th>
        <th>Source</th>
        <th>Config</th>
      </tr>
      </thead>
      <tbody>
      <tr v-for="(pipeline, index) in pipelines" :key="index">
        <td>{{ pipeline.pipeline_name }}</td>
        <td>{{ pipeline.alias || pipeline.source }}</td>
        <td>
          <button @click="configurePipeline(pipeline)">
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
      this.pipelines = response.data;
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

<style scoped>
.container {
  width: 80%;
  margin: auto;
  padding: 20px;
  background: #f2f2f2;
  border-radius: 10px;
  text-align: center;
}
.action-button {
  width: 40%;
  background-color: #007bff;
  color: white;
  padding: 10px;
  border: none;
  border-radius: 5px;
  margin: 10px 0;
  cursor: pointer;
  font-size: 16px;
}

.action-button:hover {
  background-color: #0056b3;
}

table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 20px;
}

th,
td {
  padding: 12px;
  border-bottom: 1px solid #ccc;
  text-align: left;
}

button {
  background-color: transparent;
  border: none;
  font-size: 18px;
  cursor: pointer;
}

button:hover {
  color: #007bff;
}
</style>
