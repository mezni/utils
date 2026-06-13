// Notification error handling service

import { Notification } from './notificationDelivery'
import { NotificationDeliveryResult } from './notificationDelivery'
import { deliveryLogService } from './deliveryLog'

export interface NotificationError {
  type: 'network_error' | 'service_unavailable' | 'permission_denied' | 'invalid_notification' | 'unknown'
  message: string
  originalError: any
  notification: Notification
  timestamp: Date
  retryCount: number
  maxRetries: number
}

export class NotificationErrorService {
  private maxRetries = 3
  private retryDelays = [1000, 5000, 15000] // 1s, 5s, 15s
  private retryCount = new Map<string, number>()
  private listeners: Set<(error: NotificationError) => void> = new Set()

  // Register error listener
  public subscribeToErrors(listener: (error: NotificationError) => void): () => void {
    this.listeners.add(listener)

    return () => {
      this.listeners.delete(listener)
    }
  }

  // Schedule retry for notification
  public async scheduleRetry(
    notification: Notification,
    delayIndex: number = 0,
  ): Promise<NotificationDeliveryResult | null> {
    const currentRetry = this.retryCount.get(notification.type) || 0
    const delay = this.retryDelays[delayIndex] || 30000

    console.log(`Scheduling retry for notification type: ${notification.type}, delay: ${delay}ms`)

    try {
      await new Promise<void>(resolve => setTimeout(() => resolve(), delay))

      const result = await this.retryNotification(notification, currentRetry + 1)

      if (result.success) {
        this.retryCount.delete(notification.type)
        console.log('Notification retry succeeded')
        return result
      } else {
        if (currentRetry + 1 < this.maxRetries) {
          return this.scheduleRetry(notification, delayIndex + 1)
        } else {
          this.retryCount.delete(notification.type)
          console.log('Notification retry limit reached')
          return null
        }
      }
    } catch (error) {
      console.error('Error during notification retry:', error)
      return null
    }
  }

  // Retry notification
  private async retryNotification(
    notification: Notification,
    retryCount: number,
  ): Promise<NotificationDeliveryResult> {
    try {
      // Import notification delivery service
      const { notificationDelivery } = await import('./notificationDelivery')

      // Check if notification type is valid
      if (!this.isValidNotificationType(notification)) {
        throw new Error('Invalid notification type')
      }

      const result = await notificationDelivery.scheduleNotification(notification)

      if (result.success) {
        // Update delivery log with retry info
        const log = deliveryLogService.getLogById(result.messageId)
        if (log) {
          deliveryLogService.addLog({
            ...log,
            delivered: true,
            deliveredAt: new Date(),
          })
        }
      } else {
        // Log the error
        this.handleError('service_unavailable', result.error || 'Unknown error', notification, retryCount)
      }

      return result
    } catch (error: any) {
      this.handleError('service_unavailable', error.message || 'Unknown error', notification, retryCount)
      throw error
    }
  }

  // Validate notification type
  private isValidNotificationType(notification: Notification): boolean {
    const validTypes = ['station_available', 'location_alert', 'rate_limit', 'system']
    return validTypes.includes(notification.type)
  }

  // Handle notification error
  private handleError(
    type: NotificationError['type'],
    message: string,
    notification: Notification,
    retryCount: number,
  ): void {
    const error: NotificationError = {
      type,
      message,
      originalError: null,
      notification,
      timestamp: new Date(),
      retryCount,
      maxRetries: this.maxRetries,
    }

    console.error('Notification error:', error)

    // Log the error
    deliveryLogService.addLog({
      notification: notification,
      delivered: false,
      error: message,
    })

    // Notify listeners
    this.listeners.forEach(listener => listener(error))
  }

  // Get error statistics
  public getErrorStatistics(): {
    totalErrors: number
    byType: { [type: string]: number }
    byRetryCount: { [retry: number]: number }
    mostCommonError: string
  } {
    const errors = this.retryCount.size
    const byType: { [type: string]: number } = {}
    const byRetryCount: { [retry: number]: number } = {}

    this.retryCount.forEach((count, type) => {
      byType[type] = count
      byRetryCount[count] = (byRetryCount[count] || 0) + 1
    })

    const mostCommonError = Object.keys(byType).sort((a, b) => byType[b] - byType[a])[0]

    return {
      totalErrors: errors,
      byType,
      byRetryCount,
      mostCommonError,
    }
  }

  // Reset retry counts
  public resetRetryCounts(): void {
    this.retryCount.clear()
  }

  // Set max retries
  public setMaxRetries(max: number): void {
    this.maxRetries = max
  }

  // Add custom retry delay
  public addRetryDelay(delay: number, delayIndex?: number): void {
    if (delayIndex !== undefined) {
      this.retryDelays[delayIndex] = delay
    } else {
      this.retryDelays.push(delay)
    }
  }
}

export const notificationErrorService = new NotificationErrorService()
