import type { EventEnvelope, EventName } from '@bornemap/event-taxonomy'

let sessionId: string | null = null

function getSessionId(): string {
  if (!sessionId) {
    sessionId = crypto.randomUUID()
  }
  return sessionId
}

function generateEventId(): string {
  return crypto.randomUUID()
}

function createEnvelope(
  eventName: EventName,
  payload?: Record<string, unknown>,
): EventEnvelope {
  const now = new Date().toISOString()
  return {
    event_id: generateEventId(),
    event_version: 1,
    schema_namespace: 'clickstream',
    event_name: eventName,
    occurred_at: now,
    ingested_at: now,
    channel: 'driver_web',
    session_id: getSessionId(),
    actor_role: 'anonymous',
    path: window.location.pathname,
    payload,
  }
}

export function emitEvent(
  eventName: EventName,
  payload?: Record<string, unknown>,
): void {
  const envelope = createEnvelope(eventName, payload)

  fetch('/api/v1/clickstream/events', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ events: [envelope] }),
  }).catch(() => {
    // silently ignore failures
  })
}
