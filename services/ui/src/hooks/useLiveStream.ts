import { useEffect, useState } from 'react';
import type { LiveEvent } from '../types';

interface UseLiveStreamResult {
  lastEvent: LiveEvent | null;
  connected: boolean;
}

/**
 * Maintains a single WebSocket to /ws/live.
 * Components subscribe by reading `lastEvent` and reacting via useEffect.
 * The connection is created once on mount and closed on unmount.
 */
export function useLiveStream(): UseLiveStreamResult {
  const [lastEvent, setLastEvent] = useState<LiveEvent | null>(null);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/ws/live`;
    let ws: WebSocket;
    let closed = false;

    function connect() {
      ws = new WebSocket(url);

      ws.onopen = () => {
        if (!closed) setConnected(true);
      };

      ws.onclose = () => {
        setConnected(false);
        // Reconnect after 3 s if the component is still mounted
        if (!closed) setTimeout(connect, 3_000);
      };

      ws.onerror = () => {
        ws.close();
      };

      ws.onmessage = (e) => {
        try {
          const msg = JSON.parse(e.data as string) as LiveEvent;
          if (!closed) setLastEvent(msg);
        } catch {
          // ignore malformed messages
        }
      };
    }

    connect();

    return () => {
      closed = true;
      ws?.close();
    };
  }, []);

  return { lastEvent, connected };
}
