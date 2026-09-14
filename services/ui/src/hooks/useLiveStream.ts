import { useEffect, useRef, useState } from 'react';
import type { LiveEvent } from '../types';

// ── Reconnect schedule ───────────────────────────────────────────────────────
// Delay doubles on each failure, capped at MAX_DELAY_MS, with random jitter to
// prevent thundering-herd when many tabs reconnect simultaneously.
const INITIAL_DELAY_MS = 1_000;
const MAX_DELAY_MS     = 30_000;
const JITTER_MS        = 500;

export type WsStatus = 'connecting' | 'connected' | 'disconnected';

export interface UseLiveStreamResult {
  lastEvent: LiveEvent | null;
  /** Current WebSocket lifecycle state. */
  status: WsStatus;
  /**
   * Number of reconnect attempts since the last successful connection.
   * 0 on the very first connect attempt, ≥1 after any drop.
   * Useful in the UI to distinguish "initial connect" from "reconnecting".
   */
  attempts: number;
}

export function useLiveStream(): UseLiveStreamResult {
  const [lastEvent, setLastEvent] = useState<LiveEvent | null>(null);
  const [status,    setStatus]    = useState<WsStatus>('connecting');
  const [attempts,  setAttempts]  = useState(0);

  // Refs hold mutable values that should NOT re-trigger the effect when changed.
  const wsRef      = useRef<WebSocket | null>(null);
  const timerRef   = useRef<ReturnType<typeof setTimeout> | null>(null);
  const delayRef   = useRef(INITIAL_DELAY_MS);
  const unmounted  = useRef(false);

  useEffect(() => {
    unmounted.current = false;

    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url   = `${proto}//${location.host}/ws/live`;

    function connect(attempt: number) {
      if (unmounted.current) return;

      setStatus('connecting');
      setAttempts(attempt);

      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        if (unmounted.current) { ws.close(); return; }
        delayRef.current = INITIAL_DELAY_MS; // reset backoff on success
        setStatus('connected');
      };

      ws.onmessage = (e: MessageEvent<string>) => {
        if (unmounted.current) return;
        try {
          setLastEvent(JSON.parse(e.data) as LiveEvent);
        } catch {
          // ignore malformed frames
        }
      };

      ws.onerror = () => {
        // Browser always fires onclose after onerror; reconnect logic lives there.
      };

      ws.onclose = () => {
        if (unmounted.current) return;

        wsRef.current = null;
        setStatus('disconnected');

        // Schedule reconnect with exponential backoff + jitter.
        const delay = delayRef.current + Math.random() * JITTER_MS;
        timerRef.current = setTimeout(() => {
          timerRef.current = null;
          delayRef.current = Math.min(delayRef.current * 2, MAX_DELAY_MS);
          connect(attempt + 1);
        }, delay);
      };
    }

    connect(0);

    return () => {
      unmounted.current = true;

      // Clear any pending reconnect timer first, then close the socket.
      // Nulling ws.onclose before close() prevents the close handler from
      // scheduling another reconnect after teardown.
      if (timerRef.current !== null) {
        clearTimeout(timerRef.current);
        timerRef.current = null;
      }
      if (wsRef.current !== null) {
        wsRef.current.onclose = null;
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, []); // runs once — WebSocket lifecycle is self-managed via reconnect loop

  return { lastEvent, status, attempts };
}
