// Web notification delivery service

import { pushNotificationConfig } from '../services/pushNotifications'
import { deliveryLogService } from './deliveryLog'
import { analyticsService } from './analytics'
import { notificationErrorService } from './notificationError'

// Define notification types for web
export enum WebNotificationType {
  STATION_AVAILABLE = 'station_available',
  LOCATION_ALERT = 'location_alert',
  RATE_LIMIT = 'rate_limit',
  SYSTEM = 'system',
}

export interface StationNotification {
  type: WebNotificationType.STATION_AVAILABLE
  stationId: string
  stationName: string
  distance?: number
}

export interface LocationNotification {
  type: WebNotificationType.LOCATION_ALERT
  location: {
    lat: number
    lng: number
  }
  message: string
}

export interface RateLimitNotification {
  type: WebNotificationType.RATE_LIMIT
  limit: number
  remaining: number
  resetTime: Date
}

export interface SystemNotification {
  type: WebNotificationType.SYSTEM
  message: string
}

export type WebNotification = StationNotification | LocationNotification | RateLimitNotification | SystemNotification

// Web notification delivery service
export class WebNotificationDelivery {
  private listeners: Set<(log: any) => void> = new Set()

  // Subscribe to delivery logs
  public subscribeToLogs(listener: (log: any) => void): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  // Schedule notification for delivery
  public async scheduleNotification(notification: WebNotification): Promise<boolean> {
    try {
      // Check if notifications are enabled
      if (!pushNotificationConfig.enabled) {
        console.warn('Notifications are not enabled')
        return false
      }

      // Log delivery attempt
      deliveryLogService.addLog({
        notification: notification,
        delivered: false,
        error: 'Notifications not enabled',
      })

      // Show browser notification
      const notification = new Notification(notification.type, {
        body: this.getMessage(notification),
        data: {
          type: notification.type,
          ...notification,
        },
        requireInteraction: true,
      })

      // Add click handler
      notification.onclick = (event) => {
        event.preventDefault()
        notification.close()
        this.handleNotificationClick(notification)
      }

      // Log successful delivery
      deliveryLogService.addLog({
        notification: notification,
        delivered: true,
        deliveredAt: new Date(),
      })

      // Record analytics
      analyticsService.recordEvent({
        type: 'delivery',
        notificationType: notification.type,
        timestamp: new Date(),
        platform: 'web',
        deviceType: 'browser',
      })

      return true
    } catch (error: any) {
      console.error('Failed to schedule notification:', error)

      // Log error
      deliveryLogService.addLog({
        notification: notification,
        delivered: false,
        error: error.message || 'Unknown error',
      })

      // Schedule retry
      notificationErrorService.scheduleRetry(notification)

      return false
    }
  }

  // Cancel scheduled notification
  public cancelNotification(notificationId: string): boolean {
    try {
      // Web notifications don't have built-in scheduling, so we just log it
      console.log('Cancelling notification:', notificationId)
      return true
    } catch (error: any) {
      console.error('Failed to cancel notification:', error)
      return false
    }
  }

  // Clear all notifications
  public async clearAllNotifications(): Promise<boolean> {
    try {
      if (typeof Notification !== 'undefined') {
        Notification.requestPermission().then(permission => {
          if (permission === 'granted') {
            Notification.close()
          }
        })
      }

      return true
    } catch (error: any) {
      console.error('Failed to clear all notifications:', error)
      return false
    }
  }

  // Handle notification click
  private handleNotificationClick(notification: Notification): void {
    const data = notification.data || {}

    console.log('Notification clicked:', data)

    // Record analytics
    analyticsService.recordEvent({
      type: 'click',
      notificationType: data.type,
      timestamp: new Date(),
      responseTime: 0, // Web notifications don't track response time
      platform: 'web',
      deviceType: 'browser',
    })

    // Handle different notification types
    switch (data.type) {
      case WebNotificationType.STATION_AVAILABLE:
        console.log('Navigating to station detail:', data.stationId)
        // TODO: Navigate to station detail
        // window.location.href = `/stations/${data.stationId}`
        break

      case WebNotificationType.LOCATION_ALERT:
        console.log('Location alert:', data.message)
        // TODO: Show location alert
        break

      case WebNotificationType.RATE_LIMIT:
        console.log('Rate limit alert:', data)
        // TODO: Show rate limit alert
        break

      default:
        console.log('Unknown notification type:', data.type)
    }

    // Update delivery log
    const log = deliveryLogService.getLogs().find(log =>
      log.notification?.type === data.type
    )

    if (log) {
      deliveryLogService.addLog({
        ...log,
        userResponse: 'open',
        userResponseTime: Date.now(),
      })
    }
  }

  // Get message from notification
  private getMessage(notification: WebNotification): string {
    switch (notification.type) {
      case WebNotificationType.STATION_AVAILABLE:
        return `Station ${notification.stationName} is now available!`

      case WebNotificationType.LOCATION_ALERT:
        return notification.message

      case WebNotificationType.RATE_LIMIT:
        return `Rate limit reached. Remaining: ${notification.remaining}. Resets at ${notification.resetTime.toLocaleTimeString()}`

      case WebNotificationType.SYSTEM:
        return notification.message

      default:
        return 'Notification'
    }
  }

  // Get notification status
  public getStatus(): {
    enabled: boolean
    permissions: {
      enabled: boolean
      permission: string
    }
  } {
    return {
      enabled: pushNotificationConfig.enabled,
      permissions: {
        enabled: Notification.permission === 'granted',
        permission: Notification.permission || 'unknown',
      },
    }
  }

  // Check if notifications are supported
  public isSupported(): boolean {
    return 'Notification' in window
  }
}

export const webNotificationDelivery = new WebNotificationDelivery()

// Export notification types
export { WebNotificationType }
