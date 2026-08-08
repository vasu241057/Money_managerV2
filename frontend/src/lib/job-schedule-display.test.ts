import { describe, expect, it } from 'vitest';

import { describeNextRun, formatIstDateTime, isDueNow } from './job-schedule-display';

const NOW = Date.parse('2026-08-09T05:00:00.000Z'); // 10:30 IST

describe('describeNextRun', () => {
  it('reports a future slot in IST', () => {
    // 2026-08-10T04:30:00Z is 10:00 IST the next morning.
    expect(
      describeNextRun(
        { enabled: true, next_run_at: '2026-08-10T04:30:00.000Z', kill_switched: false },
        NOW,
      ),
    ).toBe('Next run 10 Aug, 10:00 am IST');
  });

  // A slot that has passed is not a missed run: the claim query still takes it
  // on the next tick, so showing a past date would be wrong.
  it('reports an already-passed slot as due now', () => {
    expect(
      describeNextRun(
        { enabled: true, next_run_at: '2026-08-09T04:30:00.000Z', kill_switched: false },
        NOW,
      ),
    ).toBe('Due now — the next 10-minute check will start it.');
  });

  it('says nothing is scheduled while disabled', () => {
    expect(
      describeNextRun({ enabled: false, next_run_at: null, kill_switched: false }, NOW),
    ).toBe('Off — nothing is scheduled.');
  });

  it('lets the env kill switch outrank an enabled schedule', () => {
    expect(
      describeNextRun(
        { enabled: true, next_run_at: '2026-08-10T04:30:00.000Z', kill_switched: true },
        NOW,
      ),
    ).toContain('Forced off');
  });
});

describe('isDueNow', () => {
  it('is true only for an enabled job whose slot has arrived', () => {
    const past = '2026-08-09T04:30:00.000Z';
    expect(isDueNow({ enabled: true, next_run_at: past, kill_switched: false }, NOW)).toBe(true);
    expect(isDueNow({ enabled: false, next_run_at: past, kill_switched: false }, NOW)).toBe(false);
    expect(isDueNow({ enabled: true, next_run_at: past, kill_switched: true }, NOW)).toBe(false);
    expect(
      isDueNow(
        { enabled: true, next_run_at: '2026-08-10T04:30:00.000Z', kill_switched: false },
        NOW,
      ),
    ).toBe(false);
  });
});

describe('formatIstDateTime', () => {
  // 21:30 UTC is 03:00 IST the *next* day — the boundary a naive formatter gets
  // wrong, and the default schedule sits exactly on it.
  it('crosses the UTC date boundary into IST correctly', () => {
    expect(formatIstDateTime('2026-08-09T21:30:00.000Z')).toBe('10 Aug, 3:00 am');
  });
});
