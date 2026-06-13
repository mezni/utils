import AsyncStorage from '@react-native-async-storage/async-storage'

const LOG_KEY = 'bornemap_notification_logs'

export interface NotificationDeliveryLog {
  id: string
  notification: any
  delivered: boolean
  deliveredAt?: Date
  error?: string
  userResponse?: 'open' | 'dismiss' | 'acknowledge'
  userResponseTime?: number
  metadata?: {
    platform: string
    deviceType: string
    appVersion: string
  }
}

export interface DeliveryLogFilters {
  delivered?: boolean
  type?: string
  from?: Date
  to?: Date
}

class DeliveryLogService {
  private logs: NotificationDeliveryLog[] = []
  private maxLogs = 1000

  // Initialize logs from storage
  public async initialize(): Promise<void> {
    try {
      const saved = await AsyncStorage.getItem(LOG_KEY)
      if (saved) {
        const parsed: NotificationDeliveryLog[] = JSON.parse(saved)
        this.logs = parsed
      }
    } catch (error) {
      console.error('Failed to load delivery logs:', error)
      this.logs = []
    }
  }

  // Save logs to storage
  private async saveLogs(): Promise<void> {
    try {
      const toSave = this.logs.slice(-this.maxLogs)
      await AsyncStorage.setItem(LOG_KEY, JSON.stringify(toSave))
    } catch (error) {
      console.error('Failed to save delivery logs:', error)
    }
  }

  // Add a delivery log
  public addLog(log: Omit<NotificationDeliveryLog, 'id'>): string {
    const newLog: NotificationDeliveryLog = {
      ...log,
      id: `log-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`,
    }

    this.logs.unshift(newLog)
    this.saveLogs()

    return newLog.id
  }

  // Get logs with filters
  public getLogs(filters: DeliveryLogFilters = {}): NotificationDeliveryLog[] {
    let filtered = [...this.logs]

    if (filters.delivered !== undefined) {
      filtered = filtered.filter(log => log.delivered === filters.delivered)
    }

    if (filters.type) {
      filtered = filtered.filter(log => log.notification?.type === filters.type)
    }

    if (filters.from) {
      filtered = filtered.filter(log => log.deliveredAt && log.deliveredAt >= filters.from!)
    }

    if (filters.to) {
      filtered = filtered.filter(log => log.deliveredAt && log.deliveredAt <= filters.to!)
    }

    return filtered
  }

  // Get logs by ID
  public getLogById(id: string): NotificationDeliveryLog | undefined {
    return this.logs.find(log => log.id === id)
  }

  // Update log with user response
  public async updateUserResponse(
    id: string,
    response: 'open' | 'dismiss' | 'acknowledge',
  ): Promise<boolean> {
    const log = this.logs.find(log => log.id === id)
    if (!log) {
      return false
    }

    const responseTime = log.userResponseTime || Date.now()

    if (log.userResponseTime === undefined) {
      // First response
      log.userResponse = response
      log.userResponseTime = responseTime
    }

    return true
  }

  // Clear logs
  public async clearLogs(): Promise<void> {
    this.logs = []
    await this.saveLogs()
  }

  // Get log statistics
  public getStatistics(): {
    totalDelivered: number
    totalFailed: number
    deliveryRate: number
    byType: { [type: string]: number }
    avgResponseTime: number
  } {
    const delivered = this.logs.filter(log => log.delivered)
    const failed = this.logs.filter(log => !log.delivered)

    const byType: { [type: string]: number } = {}
    this.logs.forEach(log => {
      const type = log.notification?.type || 'unknown'
      byType[type] = (byType[type] || 0) + 1
    })

    const totalResponseTime = this.logs.reduce((sum, log) => sum + (log.userResponseTime || 0), 0)
    const avgResponseTime = this.logs.length > 0 ? totalResponseTime / this.logs.length : 0

    return {
      totalDelivered: delivered.length,
      totalFailed: failed.length,
      deliveryRate: this.logs.length > 0 ? (delivered.length / this.logs.length) * 100 : 0,
      byType,
      avgResponseTime,
    }
  }

  // Export logs
  public exportLogs(): string {
    return JSON.stringify(this.logs, null, 2)
  }

  // Import logs
  public async importLogs(logs: NotificationDeliveryLog[]): Promise<void> {
    this.logs = logs
    await this.saveLogs()
  }

  // Clean old logs
  public async cleanOldLogs(daysOld: number = 30): Promise<void> {
    const cutoffDate = new Date()
    cutoffDate.setDate(cutoffDate.getDate() - daysOld)

    this.logs = this.logs.filter(log => !log.deliveredAt || log.deliveredAt >= cutoffDate)

    await this.saveLogs()
  }

  // Get log count
  public getLogCount(): number {
    return this.logs.length
  }
}

export const deliveryLogService = new DeliveryLogService()
