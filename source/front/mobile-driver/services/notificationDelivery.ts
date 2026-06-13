// Notification delivery service for mobile app

import * as Notifications from 'expo-notifications'
import { Platform } from 'react-native'

// Define notification types
export enum NotificationType {
  STATION_AVAILABLE = 'station_available',
  LOCATION_ALERT = 'location_alert',
  RATE_LIMIT = 'rate_limit',
  SYSTEM = 'system',
}

export interface StationNotification {
  type: NotificationType.STATION_AVAILABLE
  stationId: string
  stationName: string
  distance?: number
}

export interface LocationNotification {
  type: NotificationType.LOCATION_ALERT
  location: {
    lat: number
    lng: number
  }
  message: string
}

export interface RateLimitNotification {
  type: NotificationType.RATE_LIMIT
  limit: number
  remaining: number
  resetTime: Date
}

export interface SystemNotification {
  type: NotificationType.SYSTEM
  message: string
}

export type Notification = StationNotification | LocationNotification | RateLimitNotification | SystemNotification

export interface NotificationDeliveryResult {
  success: boolean
  messageId: string
  notification: Notification
  timestamp: Date
  error?: string
}

export interface NotificationDeliveryLog {
  id: string
  notification: Notification
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

// Notification delivery service
class NotificationDeliveryService {
  private listeners: Set<(log: NotificationDeliveryLog) => void> = new Set()
  private deliveryLogs: NotificationDeliveryLog[] = []

  // Register a listener for delivery logs
  public subscribeToDeliveryLogs(listener: (log: NotificationDeliveryLog) => void): () => void {
    this.listeners.add(listener)

    return () => {
      this.listeners.delete(listener)
    }
  }

  // Add a delivery log
  private addDeliveryLog(result: NotificationDeliveryResult): void {
    const log: NotificationDeliveryLog = {
      id: `log-${Date.now()}-${Math.random()}`,
      notification: result.notification,
      delivered: result.success,
      deliveredAt: result.success ? result.timestamp : undefined,
      error: result.error,
      userResponse: undefined,
      userResponseTime: undefined,
      metadata: {
        platform: Platform.OS,
        deviceType: Platform.select({
          ios: 'iPhone',
          android: 'Android',
          default: 'Unknown',
        }),
        appVersion: '1.0.0', // TODO: Get actual app version
      },
    }

    this.deliveryLogs.unshift(log)

    // Notify listeners
    this.listeners.forEach(listener => listener(log))

    // Keep only last 100 logs
    if (this.deliveryLogs.length > 100) {
      this.deliveryLogs = this.deliveryLogs.slice(0, 100)
    }
  }

  // Schedule notification for delivery
  public async scheduleNotification(notification: Notification): Promise<NotificationDeliveryResult> {
    try {
      let notificationRequest: any

      switch (notification.type) {
        case NotificationType.STATION_AVAILABLE:
          notificationRequest = this.createStationAvailableNotification(notification as StationNotification)
          break

        case NotificationType.LOCATION_ALERT:
          notificationRequest = this.createLocationNotification(notification as LocationNotification)
          break

        case NotificationType.RATE_LIMIT:
          notificationRequest = this.createRateLimitNotification(notification as RateLimitNotification)
          break

        case NotificationType.SYSTEM:
          notificationRequest = this.createSystemNotification(notification as SystemNotification)
          break

        default:
          return {
            success: false,
            messageId: '',
            notification,
            timestamp: new Date(),
            error: 'Unknown notification type',
          }
      }

      const messageId = await Notifications.scheduleNotificationAsync(notificationRequest)

      this.addDeliveryLog({
        success: true,
        messageId,
        notification,
        timestamp: new Date(),
      })

      return {
        success: true,
        messageId,
        notification,
        timestamp: new Date(),
      }
    } catch (error: any) {
      console.error('Failed to schedule notification:', error)

      this.addDeliveryLog({
        success: false,
        messageId: '',
        notification,
        timestamp: new Date(),
        error: error.message || 'Unknown error',
      })

      return {
        success: false,
        messageId: '',
        notification,
        timestamp: new Date(),
        error: error.message || 'Unknown error',
      }
    }
  }

  // Cancel scheduled notification
  public async cancelNotification(messageId: string): Promise<boolean> {
    try {
      await Notifications.cancelScheduledNotificationAsync(messageId)

      return true
    } catch (error: any) {
      console.error('Failed to cancel notification:', error)
      return false
    }
  }

  // Clear all scheduled notifications
  public async clearAllScheduledNotifications(): Promise<boolean> {
    try {
      await Notifications.cancelAllScheduledNotificationsAsync()

      return true
    } catch (error: any) {
      console.error('Failed to clear all notifications:', error)
      return false
    }
  }

  // Get delivery logs
  public getDeliveryLogs(limit: number = 50): NotificationDeliveryLog[] {
    return this.deliveryLogs.slice(0, limit)
  }

  // Clear delivery logs
  public clearDeliveryLogs(): void {
    this.deliveryLogs = []
  }

  // Create notification request for station available
  private createStationAvailableNotification(notification: StationNotification) {
    return {
      content: {
        title: 'Charging Station Available',
        body: `${notification.stationName} is now available nearby!`,
        data: {
          type: notification.type,
          stationId: notification.stationId,
          stationName: notification.stationName,
          distance: notification.distance,
        },
        sound: 'default',
        priority: Notifications.AndroidNotificationPriority.HIGH,
      },
      trigger: null, // Immediate delivery
    }
  }

  // Create notification request for location alert
  private createLocationNotification(notification: LocationNotification) {
    return {
      content: {
        title: 'Location Alert',
        body: notification.message,
        data: {
          type: notification.type,
          location: notification.location,
          message: notification.message,
        },
        sound: 'default',
        priority: Notifications.AndroidNotificationPriority.HIGH,
      },
      trigger: null,
    }
  }

  // Create notification request for rate limit
  private createRateLimitNotification(notification: RateLimitNotification) {
    return {
      content: {
        title: 'Rate Limit Reached',
        body: `You've exceeded the rate limit (${notification.limit} requests). Remaining: ${notification.remaining}. Resets at ${notification.resetTime.toLocaleTimeString()}`,
        data: {
          type: notification.type,
          limit: notification.limit,
          remaining: notification.remaining,
          resetTime: notification.resetTime.toISOString(),
        },
        sound: 'default',
        priority: Notifications.AndroidNotificationPriority.HIGH,
      },
      trigger: null,
    }
  }

  // Create notification request for system notification
  private createSystemNotification(notification: SystemNotification) {
    return {
      content: {
        title: 'System Notification',
        body: notification.message,
        data: {
          type: notification.type,
          message: notification.message,
        },
        sound: 'default',
        priority: Notifications.AndroidNotificationPriority.DEFAULT,
      },
      trigger: null,
    }
  }
}

export const notificationDelivery = new NotificationDeliveryService()
