import axios, { type AxiosError, type InternalAxiosRequestConfig } from 'axios'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api/v1'

export const api = axios.create({
  baseURL: API_BASE,
  headers: { 'Content-Type': 'application/json' },
  timeout: 15_000,
})

let isRefreshing = false
let pendingQueue: Array<{
  resolve: (token: string) => void
  reject: (err: unknown) => void
}> = []

function processQueue(error: unknown, token: string | null) {
  pendingQueue.forEach((p) => {
    if (error) p.reject(error)
    else p.resolve(token!)
  })
  pendingQueue = []
}

api.interceptors.request.use((config: InternalAxiosRequestConfig) => {
  const token = localStorage.getItem('access_token')
  if (token && config.headers) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

api.interceptors.response.use(
  (response) => response,
  async (error: AxiosError) => {
    const originalRequest = error.config as InternalAxiosRequestConfig & { _retry?: boolean; _retryCount?: number }

    if (error.response?.status !== 401 || originalRequest._retry) {
      return Promise.reject(error)
    }

    if (isRefreshing) {
      return new Promise<string>((resolve, reject) => {
        pendingQueue.push({ resolve, reject })
      }).then((token) => {
        if (originalRequest.headers) {
          originalRequest.headers.Authorization = `Bearer ${token}`
        }
        return api(originalRequest)
      })
    }

    isRefreshing = true
    originalRequest._retry = true

    try {
      const refreshToken = localStorage.getItem('refresh_token')
      if (!refreshToken) throw new Error('No refresh token')

      const { data } = await axios.post(`${API_BASE}/auth/refresh`, {
        refresh_token: refreshToken,
      })

      localStorage.setItem('access_token', data.access_token)
      localStorage.setItem('refresh_token', data.refresh_token)

      processQueue(null, data.access_token)

      if (originalRequest.headers) {
        originalRequest.headers.Authorization = `Bearer ${data.access_token}`
      }
      return api(originalRequest)
    } catch (refreshError) {
      processQueue(refreshError, null)
      localStorage.removeItem('access_token')
      localStorage.removeItem('refresh_token')
      window.location.href = '/login'
      return Promise.reject(refreshError)
    } finally {
      isRefreshing = false
    }
  },
)
