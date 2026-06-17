import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      // Persistent app shell (icon rail). Stays mounted across child route
      // changes so Pyodide/theme init and flow state persist.
      path: '/',
      component: () => import('../views/AppLayout.vue'),
      children: [
        {
          path: '',
          name: 'home',
          component: () => import('../views/HomeView.vue')
        },
        {
          path: 'designer',
          name: 'designer',
          component: () => import('../views/AppPage.vue')
        },
        {
          path: 'catalog',
          name: 'catalog',
          component: () => import('../views/CatalogView.vue')
        }
      ]
    },
    {
      // Standalone embedded-editor example — intentionally rail-less, so it
      // stays a sibling of the layout (not a child).
      path: '/embed-example',
      name: 'embed-example',
      component: () => import('../views/EmbedExample.vue')
    }
  ]
})

export default router
