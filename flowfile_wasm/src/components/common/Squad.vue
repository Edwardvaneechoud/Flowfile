<!--
  Squad — a tiny harness/scaffold for orchestrating future agents.

  A "squad" is a named group of members (think: future AI agents or helper
  handlers) that a task can be dispatched to. This component is a deliberate
  template: it renders the squad and echoes a dispatched task back per member,
  with no store/Pyodide wiring, so future work can grow real behaviour on top
  of the same shape shared across the Flowfile packages.
-->
<template>
  <div class="squad">
    <h3 v-if="name" class="squad-name">{{ name }}</h3>
    <ul v-if="members.length" class="squad-members">
      <li v-for="member in members" :key="member.name" class="squad-member">
        <span class="squad-member-name">{{ member.name }}</span>
        <span v-if="member.role" class="squad-member-role">{{ member.role }}</span>
      </li>
    </ul>
    <p v-else class="squad-empty">No members yet — add some to build out the squad.</p>
    <ul v-if="results.length" class="squad-results">
      <li v-for="result in results" :key="result.member" class="squad-result">
        <strong>{{ result.member }}</strong>: {{ result.output }}
      </li>
    </ul>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";

export interface SquadMember {
  name: string;
  role?: string;
}

export interface SquadResult {
  member: string;
  output: string;
}

const props = withDefaults(
  defineProps<{
    name?: string;
    members?: SquadMember[];
  }>(),
  {
    name: "",
    members: () => [],
  },
);

const results = ref<SquadResult[]>([]);

// Dispatch a task to every member. Override this seam to give the squad real
// behaviour; the default simply echoes the task back, preserving member order.
function dispatch(task: string): SquadResult[] {
  results.value = props.members.map((member) => ({ member: member.name, output: task }));
  return results.value;
}

defineExpose({ dispatch });
</script>

<style scoped>
.squad {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  padding: 1rem;
  color: var(--color-text-primary, #2d3748);
}

.squad-name {
  margin: 0;
  font-size: 1rem;
  font-weight: 600;
}

.squad-members,
.squad-results {
  margin: 0;
  padding-left: 1rem;
}

.squad-member-role {
  margin-left: 0.5rem;
  color: var(--color-text-secondary, #718096);
  font-size: 0.875rem;
}

.squad-empty {
  margin: 0;
  color: var(--color-text-secondary, #718096);
  font-size: 0.875rem;
}
</style>
