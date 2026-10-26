<script setup lang="ts">
import Monitor from "~icons/ep/monitor";
import Setting from "~icons/ep/set-up";
import Box from "~icons/ep/box";
import Chart from "~icons/ep/data-line";
import Shield from "~icons/ep/circle-check";
import Bell from "~icons/ep/bell";
import Clock from "~icons/ep/clock";
import Arrow from "~icons/ep/right";
import Back from "~icons/ep/back";
import Info from "~icons/ep/info-filled";
import Search from "~icons/ep/search";
const props = defineProps<{ name: string }>();
const icons = {
  monitor: Monitor,
  settings: Setting,
  box: Box,
  chart: Chart,
  shield: Shield,
  bell: Bell,
  clock: Clock,
  arrow: Arrow,
  back: Back,
  info: Info,
  search: Search
};
</script>
<template>
  <component
    :is="icons[props.name] ?? Info"
    class="console-icon"
    aria-hidden="true"
  />
</template>
