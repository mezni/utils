export interface TransportOptions {
  timeout?: number
}

export interface Transport {
  request(path: string, init?: RequestInit): Promise<Response>
}

export function createTransport(baseUrl: string, defaultTimeout = 10_000): Transport {
  const url = baseUrl.replace(/\/+$/, '')

  return {
    async request(path: string, init?: RequestInit): Promise<Response> {
      const resolved = path.startsWith('/') ? `${url}${path}` : `${url}/${path}`
      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), defaultTimeout)

      try {
        const response = await fetch(resolved, { ...init, signal: controller.signal })
        return response
      } finally {
        clearTimeout(timeoutId)
      }
    },
  }
}
