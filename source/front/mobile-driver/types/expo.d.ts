declare module 'expo-notifications' {
  export interface NotificationContent {
    title?: string
    subtitle?: string
    body?: string
    data?: Record<string, unknown>
  }

  export interface NotificationRequest {
    identifier: string
    content: NotificationContent
    trigger: unknown
  }

  export interface Notification {
    request: NotificationRequest
    date: number
    platform?: string
  }

  export interface NotificationResponse {
    notification: Notification
    actionIdentifier: string
  }

  export interface PermissionResponse {
    status: string
    granted: boolean
    canAskAgain: boolean
    expires: string
  }

  export const AndroidNotificationPriority: {
    DEFAULT: string
    HIGH: string
    LOW: string
    MAX: string
    MIN: string
  }

  export function getExpoPushTokenAsync(options?: {
    experienceId?: string
    deviceId?: string
    applicationId?: string
  }): Promise<{ data: string; type: string }>

  export function getPermissionsAsync(): Promise<PermissionResponse>
  export function requestPermissionsAsync(): Promise<PermissionResponse>

  export function scheduleNotificationAsync(request: {
    content: NotificationContent
    trigger?: unknown
  }): Promise<string>

  export function cancelScheduledNotificationAsync(identifier: string): Promise<void>
  export function cancelAllScheduledNotificationsAsync(): Promise<void>
  export function dismissNotificationAsync(identifier: string): Promise<void>
  export function dismissAllNotificationsAsync(): Promise<void>

  export function addNotificationReceivedListener(
    listener: (notification: Notification) => void,
  ): { remove: () => void }

  export function addNotificationResponseReceivedListener(
    listener: (response: NotificationResponse) => void,
  ): { remove: () => void }

  export function addNotificationFailedListener(
    listener: (notification: { notification: Notification; error: Error }) => void,
  ): { remove: () => void }

  export function addNotificationReceivedBackgroundHandler(
    listener: (notification: Notification) => void,
  ): void

  export function setNotificationHandler(handler: {
    handleNotification: () => Promise<{
      shouldShowAlert: boolean
      shouldPlaySound: boolean
      shouldSetBadge: boolean
    }>
  }): void
}

declare module 'expo-background-fetch' {
  export enum BackgroundFetchResult {
    NoData = 'BackgroundFetchResultNoData',
    NewData = 'BackgroundFetchResultNewData',
    Failed = 'BackgroundFetchResultFailed',
  }

  export enum BackgroundFetchStatus {
    Denied = 2,
    Restricted = 3,
    Available = 4,
  }

  export function registerTaskAsync(taskName: string, options?: { minimumInterval?: number; stopOnTerminate?: boolean; enableHeadless?: boolean }): Promise<void>
  export function unregisterTaskAsync(taskName: string): Promise<void>
  export function setMinimumIntervalAsync(minimumInterval: number): Promise<void>
  export function getStatusAsync(taskName?: string): Promise<BackgroundFetchStatus>
  export function scheduleTaskAsync(options: { taskName?: string; name?: string; minimumInterval?: number; stopOnTerminate?: boolean }): Promise<void>
  export function stopTaskAsync(taskName: string): Promise<void>
  export { BackgroundFetchResult as Result, BackgroundFetchStatus as Status }
}

declare module 'expo-task-manager' {
  export interface TaskManagerTask {
    data: Record<string, unknown>
    error: Error | null
    executionInfo: {
      triggerId: string
    }
  }

  export function defineTask(
    taskName: string,
    task: (task: TaskManagerTask) => Promise<BackgroundFetchResult>,
  ): void
  export function isTaskRegisteredAsync(taskName: string): Promise<boolean>
  export function getRegisteredTasksAsync(): Promise<Array<{ taskName: string }>>
  export function unregisterAllTasksAsync(): Promise<void>
}
