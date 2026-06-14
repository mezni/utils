export class ApiError extends Error {
  readonly status: number | null
  readonly data: unknown | null

  constructor(status: number | null, message: string, data?: unknown) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.data = data ?? null
  }
}
