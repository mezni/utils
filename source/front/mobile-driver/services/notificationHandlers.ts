import * as Notifications from 'expo-notifications'
import { useEffect } from 'react'
import { notificationDelivery } from './notificationDelivery'
import { deliveryLogService } from './deliveryLog'
import { notificationErrorService } from './notificationError'
import { analyticsService } from './analytics'

// Setup notification handlers
export function setupNotificationHandlers(): void {
  Notifications.addNotificationReceivedListener((notification: any) => {
    console.log('Notification received:', notification)

    // Record analytics event
    const notificationData = notification.request.content.data
    analyticsService.recordEvent({
      type: 'delivery',
      notificationType: notificationData.type || 'unknown',
      timestamp: new Date(),
      platform: notification.platform || 'unknown',
      deviceType: notification.platform || 'unknown',
    })

    // Log delivery attempt
    deliveryLogService.addLog({
      notification: {
        type: notificationData.type || 'unknown',
        ...(notificationData as any),
      },
      delivered: false,
    })
  })

  Notifications.addNotificationResponseReceivedListener((response: any) => {
    console.log('Notification response:', response)

    const notificationData = response.notification.request.content.data
    const eventTime = response.notification.date?.getTime() || Date.now()

    const notificationTime = response.notification.date?.getTime() || 0
    const responseTime = eventTime - notificationTime

    // Record analytics event
    analyticsService.recordEvent({
      type: response.actionIdentifier === 'DISMISS' ? 'dismiss' : 'click',
      notificationType: notificationData.type || 'unknown',
      timestamp: new Date(),
      responseTime: response.actionIdentifier === 'DISMISS' ? responseTime : undefined,
      platform: response.notification.platform || 'unknown',
      deviceType: response.notification.platform || 'unknown',
    })

    // Handle different actions
    switch (response.actionIdentifier) {
      case 'DISMISS':
        console.log('Notification dismissed')
        break

      case 'OPEN':
        console.log('Notification opened')
        handleNotificationOpen(notificationData)
        break

      case 'ACKNOWLEDGE':
        console.log('Notification acknowledged')
        handleNotificationAcknowledge(notificationData)
        break

      default:
        console.log('Notification action:', response.actionIdentifier)
        break
    }

    // Update delivery log
    const log = deliveryLogService.getLogById(response.notification.request.identifier)
    if (log) {
      deliveryLogService.addLog({
        ...log,
        userResponse: response.actionIdentifier === 'DISMISS' ? 'dismiss' : 'open',
        userResponseTime: responseTime,
      })
    }
  })

  // Handle notification click
  function handleNotificationOpen(data: any): void {
    console.log('Opening notification content:', data)

    switch (data.type) {
      case 'station_available':
        console.log('Navigating to station detail:', data.stationId)
        // TODO: Navigate to station detail screen
        // navigation.navigate('station/[id]', { id: data.stationId })
        break

      case 'location_alert':
        console.log('Location alert:', data.message)
        // TODO: Show location alert
        break

      case 'rate_limit':
        console.log('Rate limit alert:', data)
        // TODO: Show rate limit alert
        break

      default:
        console.log('Unknown notification type:', data.type)
    }
  }

  // Handle notification acknowledge
  function handleNotificationAcknowledge(data: any): void {
    console.log('Acknowledging notification:', data)

    // TODO: Implement acknowledge action
    // Log user acknowledgment
    deliveryLogService.addLog({
      notification: {
        type: data.type || 'unknown',
      },
      delivered: false,
      error: 'Acknowledged',
    })
  }

  Notifications.addNotificationFailedListener((notification: any) => {
    console.error('Notification failed:', notification)

    const notificationData = notification.notification.request.content.data
    const error = notification.error?.message || 'Unknown error'

    // Log the error
    deliveryLogService.addLog({
      notification: {
        type: notificationData.type || 'unknown',
      },
      delivered: false,
      error: error,
    })

    // Schedule retry
    if (notificationData.type) {
      notificationErrorService.scheduleRetry({
        type: notificationData.type,
        ...(notificationData as any),
      })
    }
  })

  Notifications.addNotificationReceivedBackgroundHandler((notification: any) => {
    console.log('Background notification received:', notification)

    const notificationData = notification.request.content.data

    analyticsService.recordEvent({
      type: 'delivery',
      notificationType: notificationData.type || 'unknown',
      timestamp: new Date(),
      platform: 'background',
      deviceType: 'background',
    })

    return notification
  })
}

// Request notification permissions
export async function requestNotificationPermissions(): Promise<boolean> {
  try {
    const { status } = await Notifications.requestPermissionsAsync()

    if (status === 'granted') {
      console.log('Notification permissions granted')
      return true
    }

    console.log('Notification permissions denied')
    return false
  } catch (error) {
    console.error('Failed to request notification permissions:', error)
    return false
  }
}

// Get notification permissions status
export async function getNotificationPermissions(): Promise<{
  granted: boolean
  canAskAgain: boolean
  permission: string
}> {
  try {
    const { status, canAskAgain } = await Notifications.getPermissionsAsync()

    return {
      granted: status === 'granted',
      canAskAgain,
      permission: status,
    }
  } catch (error) {
    console.error('Failed to get notification permissions:', error)
    return {
      granted: false,
      canAskAgain: false,
      permission: 'unknown',
    }
  }
}

// Register for foreground notifications
export async function registerForForegroundNotifications(): Promise<boolean> {
  try {
    await Notifications.setNotificationHandler({
      handleNotification: async () => ({
        shouldShowAlert: true,
        shouldPlaySound: true,
        shouldSetBadge: true,
      }),
    })

    console.log('Foreground notifications registered')
    return true
  } catch (error) {
    console.error('Failed to register foreground notifications:', error)
    return false
  }
}

// Setup analytics service on app start
export async function setupAnalytics(): Promise<void> {
  await analyticsService.initialize()
  console.log('Analytics service initialized')
}

// Setup delivery log service on app start
export async function setupDeliveryLogs(): Promise<void> {
  await deliveryLogService.initialize()
  console.log('Delivery log service initialized')
}

// Setup notification handlers on app start
export async function setupNotificationServices(): Promise<void> {
  await setupAnalytics()
  await setupDeliveryLogs()
  setupNotificationHandlers()
  console.log('Notification services setup complete')
}
