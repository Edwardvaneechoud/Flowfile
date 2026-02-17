import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      name: 'app',
      component: () => import('../views/AppPage.vue')
    },
    {
      path: '/embed-example',
      name: 'embed-example',
      component: () => import('../views/EmbedExample.vue')
    }
  ]
})

export default router
