import { useCallback } from 'react';
import { ClickstreamEvent, UseClickstreamResult } from '../types';
import { sendClickstreamEvent } from '../services/api';

export function useClickstream(): UseClickstreamResult {
  const track = useCallback((event: ClickstreamEvent) => {
    sendClickstreamEvent(event).catch(() => {});
  }, []);

  return { track };
}
