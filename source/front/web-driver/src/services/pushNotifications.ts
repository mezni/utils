// Web push notification service

export interface PushNotificationConfig {
  enabled: boolean
  subscription: PushSubscription | null
  serviceWorker: ServiceWorkerRegistration | null
}

export const pushNotificationConfig: PushNotificationConfig = {
  enabled: false,
  subscription: null,
  serviceWorker: null,
}

export async function requestPushNotificationPermission(): Promise<boolean> {
  try {
    const permission = await navigator.permissions.query({ name: 'notifications' })

    if (permission.state === 'granted') {
      pushNotificationConfig.enabled = true
      return true
    }

    if (permission.state === 'prompt') {
      const permissionStatus = await Notification.requestPermission()

      if (permissionStatus === 'granted') {
        pushNotificationConfig.enabled = true
        return true
      }
    }

    return false
  } catch (error) {
    console.error('Failed to request push notification permission:', error)
    return false
  }
}

export async function registerPushNotificationsService(): Promise<ServiceWorkerRegistration | null> {
  try {
    // Check if service workers are supported
    if ('serviceWorker' in navigator) {
      const serviceWorker = await navigator.serviceWorker.register('/service-worker.js')

      pushNotificationConfig.serviceWorker = serviceWorker
      console.log('Service worker registered:', serviceWorker)

      return serviceWorker
    }

    console.warn('Service workers not supported')
    return null
  } catch (error) {
    console.error('Failed to register service worker:', error)
    return null
  }
}

export async function subscribeToPushNotifications(): Promise<PushSubscription | null> {
  try {
    if (!pushNotificationConfig.serviceWorker) {
      console.warn('Service worker not registered')
      return null
    }

    const subscription = await pushNotificationConfig.serviceWorker.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: URL.createObjectURL(new Blob([getVapidPublicKey()]) as any),
    })

    pushNotificationConfig.subscription = subscription
    console.log('Push notification subscription successful:', subscription)

    // TODO: Send subscription to backend for push notification routing
    // await sendPushSubscriptionToBackend(subscription)

    return subscription
  } catch (error) {
    console.error('Failed to subscribe to push notifications:', error)
    return null
  }
}

export async function unsubscribeFromPushNotifications(): Promise<boolean> {
  try {
    if (!pushNotificationConfig.subscription) {
      return false
    }

    await pushNotificationConfig.subscription.unsubscribe()
    pushNotificationConfig.subscription = null
    console.log('Push notification subscription unsubscribed')
    return true
  } catch (error) {
    console.error('Failed to unsubscribe from push notifications:', error)
    return false
  }
}

export async function sendPushNotificationToTopic(
  title: string,
  body: string,
  data?: any,
): Promise<boolean> {
  try {
    // TODO: Implement web push notification sending to topic
    console.log('Sending push notification:', { title, body, data })

    // Example: Use Web Push API
    if (pushNotificationConfig.subscription) {
      // Send notification to current subscription
      // await new Notification(title, { body, data })
    }

    return true
  } catch (error) {
    console.error('Failed to send push notification:', error)
    return false
  }
}

export function handlePushNotificationClick(notification: Notification): void {
  console.log('Push notification clicked:', notification)

  // Handle different notification types
  const data = notification.data || {}

  switch (data.type) {
    case 'station_available':
      console.log('Navigate to available stations')
      // TODO: Navigate to station list or map
      break

    case 'location_alert':
      console.log('Location alert:', data.message)
      // TODO: Show location alert
      break

    default:
      console.log('Unknown notification type:', data.type)
  }
}

export function setupPushNotificationHandlers(): void {
  // Setup notification click handler
  if ('Notification' in window) {
    Notification.addEventListener('click', (event) => {
      event.preventDefault()
      handlePushNotificationClick(event.notification)
    })
  }

  // Setup permission change handler
  if ('Notification' in window) {
    Notification.addEventListener('permissionchange', (event) => {
      const permission = event.permission
      console.log('Notification permission changed:', permission)

      if (permission === 'granted') {
        pushNotificationConfig.enabled = true
      } else {
        pushNotificationConfig.enabled = false
      }
    })
  }
}

export function clearAllNotifications(): void {
  if ('Notification' in window) {
    // Close all notifications
    const notifications = document.querySelectorAll('.notification')
    notifications.forEach(notification => {
      notification.close()
    })
  }
}

export function cancelScheduledNotification(notificationId: string): void {
  // TODO: Implement cancel scheduled notification
  console.log(`Cancelling notification: ${notificationId}`)
}

export function schedulePushNotification(
  title: string,
  body: string,
  data?: any,
  trigger?: any,
): void {
  // TODO: Implement push notification scheduling
  console.log('Scheduling push notification:', { title, body, data, trigger })
}

export function isPushNotificationSupported(): boolean {
  return 'Notification' in window && 'serviceWorker' in navigator
}

export function getPushNotificationStatus(): {
  supported: boolean
  enabled: boolean
  permissions: {
    enabled: boolean
    permission: string
  }
} {
  if (!isPushNotificationSupported()) {
    return {
      supported: false,
      enabled: false,
      permissions: {
        enabled: false,
        permission: 'unsupported',
      },
    }
  }

  let permission = 'default'
  if ('Notification' in window) {
    permission = Notification.permission
  }

  return {
    supported: true,
    enabled: pushNotificationConfig.enabled,
    permissions: {
      enabled: permission === 'granted',
      permission,
    },
  }
}

// VAPID public key (in production, this should be from your server)
function getVapidPublicKey(): string {
  return 'BEl62iUYgUivxIkv69yViEuiBIa-Ib9-SkvKeatzBCEkGhOkPH1dh76pGk4chWyLB3uc30QFbq3qP5J5AXMQVw'