import * as BackgroundFetch from 'expo-background-fetch'
import * as TaskManager from 'expo-task-manager'

const REFRESH_STATIONS_TASK = 'refresh-stations-task'

export interface BackgroundFetchConfig {
  minimumInterval: number
  stopOnTerminate: boolean
  enableHighAccuracyLocation: boolean
}

export const backgroundFetchConfig: BackgroundFetchConfig = {
  minimumInterval: 15 * 60 * 1000, // 15 minutes
  stopOnTerminate: false,
  enableHighAccuracyLocation: false,
}

export async function registerBackgroundFetchTask(): Promise<boolean> {
  try {
    await BackgroundFetch.registerTaskAsync(REFRESH_STATIONS_TASK, {
      minimumInterval: backgroundFetchConfig.minimumInterval,
      stopOnTerminate: backgroundFetchConfig.stopOnTerminate,
      enableHeadless: true,
    })

    console.log('Background fetch task registered')
    return true
  } catch (error) {
    console.error('Failed to register background fetch task:', error)
    return false
  }
}

export async function unregisterBackgroundFetchTask(): Promise<boolean> {
  try {
    await BackgroundFetch.unregisterTaskAsync(REFRESH_STATIONS_TASK)
    console.log('Background fetch task unregistered')
    return true
  } catch (error) {
    console.error('Failed to unregister background fetch task:', error)
    return false
  }
}

export async function checkBackgroundFetchStatus(): Promise<number | null> {
  try {
    const status = await BackgroundFetch.getStatusAsync(REFRESH_STATIONS_TASK)
    return status
  } catch (error) {
    console.error('Failed to check background fetch status:', error)
    return null
  }
}

export async function scheduleBackgroundFetch(): Promise<boolean> {
  try {
    const status = await checkBackgroundFetchStatus()
    if (status === BackgroundFetch.Status.Available) {
      const result = await BackgroundFetch.scheduleTaskAsync({
        name: REFRESH_STATIONS_TASK,
        minimumInterval: backgroundFetchConfig.minimumInterval,
      })

      console.log('Background fetch scheduled:', result)
      return true
    }

    console.log('Background fetch not available')
    return false
  } catch (error) {
    console.error('Failed to schedule background fetch:', error)
    return false
  }
}

export async function stopBackgroundFetch(): Promise<boolean> {
  try {
    const status = await checkBackgroundFetchStatus()
    if (status !== null) {
      await BackgroundFetch.stopTaskAsync(REFRESH_STATIONS_TASK)
      console.log('Background fetch stopped')
      return true
    }

    console.log('No background fetch task running')
    return false
  } catch (error) {
    console.error('Failed to stop background fetch:', error)
    return false
  }
}

// Task manager definition
TaskManager.defineTask(REFRESH_STATIONS_TASK, async (data) => {
  console.log('Background fetch task started', data)

  try {
    // TODO: Implement station refresh logic here
    console.log('Fetching stations...')

    // Simulate work
    await new Promise<void>(resolve => setTimeout(() => resolve(), 5000))

    console.log('Background fetch task completed successfully')
    return BackgroundFetch.Result.NewData
  } catch (error) {
    console.error('Background fetch task failed:', error)
    return BackgroundFetch.Result.Failed
  }
})

export function isBackgroundFetchSupported(): boolean {
  return typeof BackgroundFetch !== 'undefined'
}

export function isBackgroundFetchEnabled(): boolean {
  // Check if background fetch is available and enabled
  return isBackgroundFetchSupported()
}
