// Notification analytics service for mobile app

import AsyncStorage from '@react-native-async-storage/async-storage'

const ANALYTICS_KEY = 'bornemap_notification_analytics'

export interface NotificationAnalytics {
  totalNotifications: number
  delivered: number
  failed: number
  opened: number
  dismissed: number
  clicked: number
  byType: { [type: string]: number }
  byHour: { [hour: string]: number }
  byDay: { [day: string]: number }
  avgResponseTime: number
  conversionRate: number
  lastUpdated: Date
}

export interface NotificationAnalyticsEvent {
  type: 'delivery' | 'open' | 'dismiss' | 'click'
  notificationType: string
  timestamp: Date
  responseTime?: number
  platform: string
  deviceType: string
}

class AnalyticsService {
  private events: NotificationAnalyticsEvent[] = []
  private currentAnalytics: NotificationAnalytics | null = null

  // Initialize analytics from storage
  public async initialize(): Promise<void> {
    try {
      const saved = await AsyncStorage.getItem(ANALYTICS_KEY)
      if (saved) {
        this.currentAnalytics = JSON.parse(saved)
      } else {
        this.currentAnalytics = this.getInitialAnalytics()
      }
    } catch (error) {
      console.error('Failed to load analytics:', error)
      this.currentAnalytics = this.getInitialAnalytics()
    }
  }

  // Get initial analytics
  private getInitialAnalytics(): NotificationAnalytics {
    return {
      totalNotifications: 0,
      delivered: 0,
      failed: 0,
      opened: 0,
      dismissed: 0,
      clicked: 0,
      byType: {},
      byHour: {},
      byDay: {},
      avgResponseTime: 0,
      conversionRate: 0,
      lastUpdated: new Date(),
    }
  }

  private async saveAnalytics(): Promise<void> {
    try {
      if (!this.currentAnalytics) return
      this.currentAnalytics.lastUpdated = new Date()
      await AsyncStorage.setItem(ANALYTICS_KEY, JSON.stringify(this.currentAnalytics))
    } catch (error) {
      console.error('Failed to save analytics:', error)
    }
  }

  // Record an event
  public async recordEvent(event: NotificationAnalyticsEvent): Promise<void> {
    const eventId = `event-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`

    this.events.push({
      ...event,
      timestamp: new Date(),
    })

    // Update analytics
    this.updateAnalytics(event)

    // Save to storage
    await this.saveAnalytics()
  }

  // Update analytics based on event
  private updateAnalytics(event: NotificationAnalyticsEvent): void {
    if (!this.currentAnalytics) {
      this.currentAnalytics = this.getInitialAnalytics()
    }

    const now = new Date()
    const dayKey = now.toISOString().split('T')[0]
    const hourKey = now.getHours().toString()

    // Update counters
    this.currentAnalytics.totalNotifications++

    switch (event.type) {
      case 'delivery':
        this.currentAnalytics.delivered++
        break
      case 'open':
        this.currentAnalytics.opened++
        break
      case 'dismiss':
        this.currentAnalytics.dismissed++
        break
      case 'click':
        this.currentAnalytics.clicked++
        break
    }

    // Update by type
    const eventType = event.notificationType || 'unknown'
    this.currentAnalytics.byType[eventType] = (this.currentAnalytics.byType[eventType] || 0) + 1

    // Update by hour
    this.currentAnalytics.byHour[hourKey] = (this.currentAnalytics.byHour[hourKey] || 0) + 1

    // Update by day
    this.currentAnalytics.byDay[dayKey] = (this.currentAnalytics.byDay[dayKey] || 0) + 1

    // Update response time if available
    if (event.responseTime) {
      const currentAvg = this.currentAnalytics.avgResponseTime
      const totalResponses = this.currentAnalytics.opened + this.currentAnalytics.dismissed + this.currentAnalytics.clicked
      const newAvg = ((currentAvg * (totalResponses - 1)) + event.responseTime) / totalResponses
      this.currentAnalytics.avgResponseTime = newAvg
    }

    // Update conversion rate
    this.currentAnalytics.conversionRate = this.calculateConversionRate()
  }

  private calculateConversionRate(): number {
    const analytics = this.currentAnalytics
    if (!analytics) return 0

    const totalDelivered = analytics.delivered
    if (totalDelivered === 0) return 0

    const totalInteractions = analytics.opened + analytics.dismissed + analytics.clicked
    return (totalInteractions / totalDelivered) * 100
  }

  public getAnalytics(): NotificationAnalytics {
    return { ...(this.currentAnalytics || this.getInitialAnalytics()) }
  }

  // Get events for a specific period
  public getEvents(
    from?: Date,
    to?: Date,
    limit: number = 100,
  ): NotificationAnalyticsEvent[] {
    let filtered = [...this.events]

    if (from) {
      filtered = filtered.filter(event => event.timestamp >= from)
    }

    if (to) {
      filtered = filtered.filter(event => event.timestamp <= to)
    }

    return filtered.slice(0, limit)
  }

  // Get analytics for a specific time period
  public getAnalyticsForPeriod(
    from: Date,
    to: Date,
  ): NotificationAnalytics {
    const events = this.getEvents(from, to)

    return {
      totalNotifications: events.length,
      delivered: events.filter(e => e.type === 'delivery').length,
      failed: 0, // Failed deliveries are not recorded as events
      opened: events.filter(e => e.type === 'open').length,
      dismissed: events.filter(e => e.type === 'dismiss').length,
      clicked: events.filter(e => e.type === 'click').length,
      byType: this.aggregateByType(events),
      byHour: this.aggregateByHour(events),
      byDay: this.aggregateByDay(events),
      avgResponseTime: this.calculateAvgResponseTime(events),
      conversionRate: this.calculateConversionRate(),
      lastUpdated: new Date(),
    }
  }

  // Aggregate events by type
  private aggregateByType(events: NotificationAnalyticsEvent[]): { [type: string]: number } {
    const aggregated: { [type: string]: number } = {}

    events.forEach(event => {
      const type = event.notificationType || 'unknown'
      aggregated[type] = (aggregated[type] || 0) + 1
    })

    return aggregated
  }

  // Aggregate events by hour
  private aggregateByHour(events: NotificationAnalyticsEvent[]): { [hour: string]: number } {
    const aggregated: { [hour: string]: number } = {}

    events.forEach(event => {
      const hour = event.timestamp.getHours().toString()
      aggregated[hour] = (aggregated[hour] || 0) + 1
    })

    return aggregated
  }

  // Aggregate events by day
  private aggregateByDay(events: NotificationAnalyticsEvent[]): { [day: string]: number } {
    const aggregated: { [day: string]: number } = {}

    events.forEach(event => {
      const day = event.timestamp.toISOString().split('T')[0]
      aggregated[day] = (aggregated[day] || 0) + 1
    })

    return aggregated
  }

  // Calculate average response time
  private calculateAvgResponseTime(events: NotificationAnalyticsEvent[]): number {
    const eventsWithResponse = events.filter(e => e.responseTime !== undefined)

    if (eventsWithResponse.length === 0) return 0

    const totalResponseTime = eventsWithResponse.reduce((sum, e) => sum + (e.responseTime || 0), 0)
    return totalResponseTime / eventsWithResponse.length
  }

  // Clear all events
  public async clearEvents(): Promise<void> {
    this.events = []
    this.currentAnalytics = this.getInitialAnalytics()
    await this.saveAnalytics()
  }

  // Export analytics as JSON
  public exportAnalytics(): string {
    return JSON.stringify(this.currentAnalytics, null, 2)
  }

  // Import analytics
  public async importAnalytics(analytics: NotificationAnalytics): Promise<void> {
    this.currentAnalytics = analytics
    await this.saveAnalytics()
  }

  public getErrorStats(): {
    totalErrors: number
    mostCommonError: string
    byType: Record<string, number>
    byRetryCount: Record<string, number>
  } {
    return {
      totalErrors: this.events.filter(e => e.type === 'delivery' && e.notificationType === 'error').length,
      mostCommonError: 'Unknown',
      byType: {},
      byRetryCount: {},
    }
  }

  public getPerformanceMetrics(): {
    deliverySuccessRate: number
    avgResponseTime: number
    conversionRate: number
    mostPopularType: string
    peakHour: string
  } {
    const analytics = this.getAnalytics()

    return {
      deliverySuccessRate: analytics.delivered > 0
        ? (analytics.delivered / analytics.totalNotifications) * 100
        : 0,
      avgResponseTime: analytics.avgResponseTime,
      conversionRate: analytics.conversionRate,
      mostPopularType: Object.entries(analytics.byType)
        .sort((a, b) => b[1] - a[1])[0]?.[0] || 'unknown',
      peakHour: Object.entries(analytics.byHour)
        .sort((a, b) => b[1] - a[1])[0]?.[0] || 'unknown',
    }
  }

  // Get daily summary
  public getDailySummary(): {
    totalNotifications: number
    avgPerDay: number
    bestDay: string
    worstDay: string
  } {
    const analytics = this.getAnalytics()

    const days = Object.keys(analytics.byDay)
    if (days.length === 0) {
      return {
        totalNotifications: 0,
        avgPerDay: 0,
        bestDay: '',
        worstDay: '',
      }
    }

    const notificationsPerDay = days.map(day => analytics.byDay[day] || 0)
    const totalNotifications = notificationsPerDay.reduce((a, b) => a + b, 0)
    const avgPerDay = totalNotifications / days.length

    const bestDay = days[notificationsPerDay.indexOf(Math.max(...notificationsPerDay))]
    const worstDay = days[notificationsPerDay.indexOf(Math.min(...notificationsPerDay))]

    return {
      totalNotifications,
      avgPerDay,
      bestDay,
      worstDay,
    }
  }
}

export const analyticsService = new AnalyticsService()
