<!-- A fájl a Vue alkalmazás gyökérkomponense, amely a router-view befoglalásáért felelős. -->
<template>
  <div id="app">
    <router-view />
  </div>
</template>

<script setup lang="ts">
// A beállítások ellenőrzése mostantól a router/index.ts-ben történik (global navigation guard)
</script>

<style scoped>
#app {
  padding: 20px;
  font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
}
</style>
