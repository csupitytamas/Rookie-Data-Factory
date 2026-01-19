
import { defineStore } from 'pinia'
import { getCurrentUser } from '../api/user.js'

export const useUserStore = defineStore('user', {
  state: () => ({
    user: null as any,
  }),
  actions: {
    async loadUser() {
      try {
        const res = await getCurrentUser()
        this.user = res.data
      } catch (err) {
        console.warn('Nem sikerült betölteni a felhasználót (lejárt token vagy nincs bejelentkezés)')
        this.user = null
      }
    },
    isAuthenticated(): boolean {
      return this.user !== null
    }
  }

})
