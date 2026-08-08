import { useSyncExternalStore } from 'react';

const TICK_MS = 60_000;

// One shared clock rather than a timer per component. The snapshot is a cached
// module value — reading Date.now() on every render would be impure and would
// also make useSyncExternalStore loop.
let now = Date.now();
let timer: number | null = null;
const listeners = new Set<() => void>();

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  if (timer === null) {
    // The module value can be an hour stale if the tab sat idle; React re-reads
    // the snapshot right after subscribing, so refreshing here is enough.
    now = Date.now();
    timer = window.setInterval(() => {
      now = Date.now();
      for (const notify of listeners) notify();
    }, TICK_MS);
  }
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0 && timer !== null) {
      window.clearInterval(timer);
      timer = null;
    }
  };
}

function getSnapshot(): number {
  return now;
}

/** Wall-clock milliseconds, refreshed once a minute. */
export function useNow(): number {
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
