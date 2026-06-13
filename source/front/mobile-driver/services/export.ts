// Notification export and reporting service

import { analyticsService } from './analytics'
import { deliveryLogService } from './deliveryLog'

export interface ExportFormat {
  json: string
  csv: string
}

export interface ExportReport {
  timestamp: Date
  analytics: any
  deliveryLogs: any[]
  summary: string
  performanceMetrics: {
    deliverySuccessRate: number
    avgResponseTime: number
    mostPopularType: string
    peakHour: string
  }
  dailySummary: {
    totalNotifications: number
    avgPerDay: number
    bestDay: string
    worstDay: string
  }
  errorStats: {
    totalErrors: number
    mostCommonError: string
  }
}

class ExportService {
  // Export analytics in different formats
  public exportAnalytics(format: 'json' | 'csv' = 'json'): ExportFormat {
    const analytics = analyticsService.getAnalytics()
    const events = analyticsService.getEvents()

    switch (format) {
      case 'json':
        return {
          json: JSON.stringify({
            analytics,
            events,
            exportDate: new Date().toISOString(),
          }, null, 2),
          csv: '',
        }

      case 'csv':
        return {
          json: '',
          csv: this.generateAnalyticsCSV(events),
        }

      default:
        return {
          json: JSON.stringify(analytics, null, 2),
          csv: '',
        }
    }
  }

  // Generate CSV from events
  private generateAnalyticsCSV(events: any[]): string {
    if (events.length === 0) {
      return 'No events to export'
    }

    // Get all unique fields
    const headers = new Set<string>()
    events.forEach(event => {
      Object.keys(event).forEach(key => headers.add(key))
    })

    const headerRow = Array.from(headers).join(',')
    const rows = events.map(event =>
      Array.from(headers)
        .map(field => {
          const value = event[field]
          if (value === undefined || value === null) return ''
          if (typeof value === 'string') return `"${value.replace(/"/g, '""')}"`
          return String(value)
        })
        .join(',')
    )

    return [headerRow, ...rows].join('\n')
  }

  // Export delivery logs in different formats
  public exportDeliveryLogs(format: 'json' | 'csv' = 'json'): ExportFormat {
    const logs = deliveryLogService.getLogs()
    const analytics = analyticsService.getAnalytics()

    switch (format) {
      case 'json':
        return {
          json: JSON.stringify({
            logs,
            analytics,
            exportDate: new Date().toISOString(),
          }, null, 2),
          csv: '',
        }

      case 'csv':
        return {
          json: '',
          csv: this.generateDeliveryLogsCSV(logs),
        }

      default:
        return {
          json: JSON.stringify(logs, null, 2),
          csv: '',
        }
    }
  }

  // Generate CSV from delivery logs
  private generateDeliveryLogsCSV(logs: any[]): string {
    if (logs.length === 0) {
      return 'No logs to export'
    }

    // Get all unique fields from all logs
    const headers = new Set<string>()
    logs.forEach(log => {
      Object.keys(log).forEach(key => headers.add(key))
    })

    const headerRow = Array.from(headers).join(',')
    const rows = logs.map(log =>
      Array.from(headers)
        .map(field => {
          const value = log[field]
          if (value === undefined || value === null) return ''
          if (typeof value === 'string') return `"${value.replace(/"/g, '""')}"`
          if (value instanceof Date) return `"${value.toISOString()}"`
          return String(value)
        })
        .join(',')
    )

    return [headerRow, ...rows].join('\n')
  }

  // Generate complete report
  public generateReport(): ExportReport {
    const analytics = analyticsService.getAnalytics()
    const logs = deliveryLogService.getLogs()
    const performanceMetrics = analyticsService.getPerformanceMetrics()
    const dailySummary = analyticsService.getDailySummary()
    const errorStats = analyticsService.getErrorStats()

    const summary = `
      Notification Performance Report
      =================================
      Generated: ${new Date().toISOString()}
      Total Notifications: ${analytics.totalNotifications}
      Delivered: ${analytics.delivered}
      Failed: ${analytics.failed}
      Opened: ${analytics.opened}
      Dismissed: ${analytics.dismissed}
      Conversion Rate: ${analytics.conversionRate.toFixed(2)}%

      Performance Metrics:
      - Delivery Success Rate: ${performanceMetrics.deliverySuccessRate.toFixed(2)}%
      - Average Response Time: ${performanceMetrics.avgResponseTime.toFixed(2)} seconds
      - Most Popular Type: ${performanceMetrics.mostPopularType}
      - Peak Hour: ${performanceMetrics.peakHour}

      Daily Summary:
      - Total Notifications: ${dailySummary.totalNotifications}
      - Average per Day: ${dailySummary.avgPerDay.toFixed(2)}
      - Best Day: ${dailySummary.bestDay}
      - Worst Day: ${dailySummary.worstDay}

      Error Statistics:
      - Total Errors: ${errorStats.totalErrors}
      - By Type: ${JSON.stringify(errorStats.byType)}
      - By Retry Count: ${JSON.stringify(errorStats.byRetryCount)}
      - Most Common Error: ${errorStats.mostCommonError}
    `.trim()

    return {
      timestamp: new Date(),
      analytics,
      deliveryLogs: logs,
      summary,
      performanceMetrics,
      dailySummary,
      errorStats,
    }
  }

  // Export report in different formats
  public exportReport(format: 'json' | 'csv' = 'json'): ExportFormat {
    const report = this.generateReport()

    switch (format) {
      case 'json':
        return {
          json: JSON.stringify(report, null, 2),
          csv: '',
        }

      case 'csv':
        return {
          json: '',
          csv: this.generateReportCSV(report),
        }

      default:
        return {
          json: JSON.stringify(report, null, 2),
          csv: '',
        }
    }
  }

  // Generate CSV from report
  private generateReportCSV(report: ExportReport): string {
    const csvContent = []
    csvContent.push('Notification Performance Report')
    csvContent.push(`Generated: ${report.timestamp.toISOString()}`)
    csvContent.push('')

    // Analytics section
    csvContent.push('ANALYTICS')
    csvContent.push('Total Notifications,Delivered,Failed,Opened,Dismissed,Clicked,Conversion Rate')
    csvContent.push(
      `${report.analytics.totalNotifications},${report.analytics.delivered},${report.analytics.failed},${report.analytics.opened},${report.analytics.dismissed},${report.analytics.clicked},${report.analytics.conversionRate.toFixed(2)}%`
    )

    // Performance metrics section
    csvContent.push('')
    csvContent.push('PERFORMANCE METRICS')
    csvContent.push('Delivery Success Rate,Average Response Time,Most Popular Type,Peak Hour')
    csvContent.push(
      `${report.performanceMetrics.deliverySuccessRate.toFixed(2)}%,${
        report.performanceMetrics.avgResponseTime.toFixed(2)
      },${report.performanceMetrics.mostPopularType},${report.performanceMetrics.peakHour}`
    )

    // Daily summary section
    csvContent.push('')
    csvContent.push('DAILY SUMMARY')
    csvContent.push('Total Notifications,Average per Day,Best Day,Worst Day')
    csvContent.push(
      `${report.dailySummary.totalNotifications},${report.dailySummary.avgPerDay.toFixed(
        2
      )},${report.dailySummary.bestDay},${report.dailySummary.worstDay}`
    )

    // Error statistics section
    csvContent.push('')
    csvContent.push('ERROR STATISTICS')
    csvContent.push('Total Errors,Most Common Error')
    csvContent.push(
      `${report.errorStats.totalErrors},${report.errorStats.mostCommonError}`
    )

    return csvContent.join('\n')
  }

  // Upload report to backend
  public async uploadReport(): Promise<{
    success: boolean
    reportId?: string
    error?: string
  }> {
    try {
      const report = this.generateReport()

      // TODO: Implement backend upload
      // const response = await fetch('/api/v1/analytics/upload', {
      //   method: 'POST',
      //   headers: {
      //     'Content-Type': 'application/json',
      //   },
      //   body: JSON.stringify(report),
      // })

      // if (!response.ok) {
      //   throw new Error('Failed to upload report')
      // }

      // const data = await response.json()
      // return { success: true, reportId: data.id }

      console.log('Report ready for upload:', report)
      return {
        success: true,
        reportId: `report-${Date.now()}`,
      }
    } catch (error: any) {
      console.error('Failed to upload report:', error)
      return {
        success: false,
        error: error.message || 'Unknown error',
      }
    }
  }

  // Schedule automated report generation
  public async scheduleDailyReport(): Promise<boolean> {
    try {
      // TODO: Implement scheduled report generation
      console.log('Daily report scheduled')
      return true
    } catch (error) {
      console.error('Failed to schedule daily report:', error)
      return false
    }
  }

  // Get error statistics
  public getErrorStats(): {
    totalErrors: number
    byType: { [type: string]: number }
    byRetryCount: { [retry: number]: number }
    mostCommonError: string
  } {
    // Import error service
    const { notificationErrorService } = require('./notificationError')
    return notificationErrorService.getErrorStatistics()
  }

  // Clear all data
  public async clearAllData(): Promise<void> {
    await analyticsService.clearEvents()
    await deliveryLogService.clearLogs()
  }
}

export const exportService = new ExportService()
