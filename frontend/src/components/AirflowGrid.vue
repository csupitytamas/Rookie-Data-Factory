<!-- A a fájl az Airflow feladatok állapotának vizuális megjelenítéséért (grafikon) felelős komponens. -->
<template>
  <div class="airflow-grid-container">
    <h4>Jobs execution details</h4>
    <div v-if="loading" class="grid-loading">Loading execution history...</div>
    <div v-else-if="history.length === 0" class="no-history">No execution history found for this pipeline.</div>
    <div v-else class="grid-wrapper">
      <div class="grid-scroll">
        <table class="airflow-status-grid">
          <thead>
            <tr>
              <th class="task-col">Task / Run</th>
              <th v-for="(run, idx) in sortedHistory" :key="run.run_id" class="run-col">
                <span class="run-date" :title="run.execution_date">
                  {{ formatShortDate(run.execution_date) }}
                </span>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="taskId in allTaskIds" :key="taskId">
              <td class="task-name">{{ formatTaskId(taskId) }}</td>
              <td v-for="run in sortedHistory" :key="run.run_id" class="status-cell">
                <div 
                  class="status-square" 
                  :class="getTaskStatusClass(run, taskId)"
                  :title="getTaskTooltip(run, taskId)"
                ></div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="grid-legend">
        <span class="legend-item"><span class="square success"></span> Success</span>
        <span class="legend-item"><span class="square failed"></span> Failed</span>
        <span class="legend-item"><span class="square running"></span> Running</span>
        <span class="legend-item"><span class="square queued"></span> Queued</span>
        <span class="legend-item"><span class="square skipped"></span> Skipped</span>
        <span class="legend-item"><span class="square none"></span> Not Run</span>
      </div>
    </div>
  </div>
</template>

<script>
import { getPipelineHistory } from "@/api/dashboard";

export default {
  name: "AirflowGrid",
  props: {
    pipelineId: {
      type: Number,
      required: true
    }
  },

  // A komponens belső állapota: futtatási előzmények, betöltési állapot és az összes előforduló feladat azonosítója.
  data() {
    return {
      history: [],
      loading: true,
      allTaskIds: []
    };
  },

  // Számított tulajdonságok: az előzményeket fordított időrendbe rendezzük a megjelenítéshez.
  computed: {
    sortedHistory() {
      return [...this.history].reverse();
    }
  },

  // A komponens betöltésekor azonnal lekérjük az adott pipeline előzményeit.
  async mounted() {
    await this.fetchHistory();
  },

  // Figyeljük a pipeline azonosító változását, hogy frissíthessük a táblázatot.
  watch: {
    pipelineId: {
      handler: 'fetchHistory',
      immediate: true
    }
  },

  // Metódusok az adatok lekéréséhez, feldolgozásához és formázásához.
  methods: {
    async fetchHistory() {
      if (!this.pipelineId) return;
      this.loading = true;

      // Lekérdezzük a pipeline futtatási előzményeit az API-n keresztül.
      try {
        const response = await getPipelineHistory(this.pipelineId);
        this.history = response.data;
        this.extractTaskIds();
      } catch (err) {
        console.error("Failed to fetch Airflow history:", err);
      } finally {
        this.loading = false;
      }
    },

    // Kigyűjti az összes egyedi feladat azonosítót  az előzményekből a grafikon sorainak felépítéséhez.
    extractTaskIds() {
      const ids = new Set();

      // Végigmegyünk minden futtatáson és kigyűjtjük a bennük szereplő összes feladatot.
      this.history.forEach(run => {
        run.tasks.forEach(task => ids.add(task.task_id));
      });

      // Azonosítók rendezése logikai sorrendbe (Extract -> Create -> Transform -> Load).
      this.allTaskIds = Array.from(ids).sort((a, b) => {
        const order = ['extract', 'create', 'transform', 'load'];
        const getIdx = (id) => order.findIndex(o => id.toLowerCase().includes(o));
        const idxA = getIdx(a);
        const idxB = getIdx(b);
        if (idxA !== idxB) return idxA - idxB;
        return a.localeCompare(b);
      });
    },

    // Meghatározza megjelenést egy adott feladat állapota alapján a vizuális megjelenítéshez.
    getTaskStatusClass(run, taskId) {
      const task = run.tasks.find(t => t.task_id === taskId);
      if (!task) return 'status-none';
      return `status-${task.state || 'none'}`;
    },

    // Összeállítja a részletes információkat tartalmazó tooltip egy feladathoz.
    getTaskTooltip(run, taskId) {
      const task = run.tasks.find(t => t.task_id === taskId);
      if (!task) return 'No status';
      return `Task: ${taskId}\nRun ID: ${run.run_id}\nStatus: ${task.state}\nStarted: ${task.start_date || '-'}\nEnded: ${task.end_date || '-'}`;
    },

    // Rövidített dátum formázása a táblázat fejlécéhez.
    formatShortDate(dateStr) {
      if (!dateStr) return '-';
      const d = new Date(dateStr);
      return `${d.getMonth() + 1}.${d.getDate()} ${d.getHours()}:${String(d.getMinutes()).padStart(2, '0')}`;
    },

    // A technikai feladat azonosítóból olvashatóbb, rövidített nevet képez.
    formatTaskId(id) {
      const parts = id.split('_');
      if (parts.length > 0) {
        const keyword = parts[0].toUpperCase();
        if (['EXTRACT', 'CREATE', 'TRANSFORM', 'LOAD'].includes(keyword)) return keyword;
      }
      return id;
    }
  }
};
</script>

<style scoped src="./styles/AirflowGrid.css"></style>
