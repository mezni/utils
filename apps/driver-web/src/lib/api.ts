import { ApiClient } from '@bornemap/api-client'
import * as auth from '@bornemap/auth-client'

export const apiClient = new ApiClient({
  baseUrl: 'http://localhost/api/v1/driver',
  getToken: async () => auth.getToken(),
})
