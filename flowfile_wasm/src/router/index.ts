import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      name: 'docs',
      component: () => import('../views/DocsPage.vue')
    },
    {
      path: '/app',
      name: 'app',
      component: () => import('../views/AppPage.vue')
    }
  ]
})

export default router
