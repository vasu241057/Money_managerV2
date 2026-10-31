import { describe, expect, it } from 'vitest';
import {
  buildReturnToAppUrl,
  parseGoogleOAuthCallbackParams,
  resolveAppUrlOpenPath,
} from './gmail-oauth';

const SITE_HOST = 'backend.circuit-issh.workers.dev';

describe('gmail oauth helpers', () => {
  it('parses callback params when code/state are present', () => {
    expect(parseGoogleOAuthCallbackParams('?code=abc&state=xyz')).toEqual({
      code: 'abc',
      state: 'xyz',
      oauthError: null,
      hasCallbackParams: true,
    });
  });

  it('marks callback params absent when oauth error is present', () => {
    expect(parseGoogleOAuthCallbackParams('?error=access_denied&state=xyz')).toEqual({
      code: null,
      state: 'xyz',
      oauthError: 'access_denied',
      hasCallbackParams: false,
    });
  });
});

describe('android app url handling', () => {
  it('opens the callback page for the verified link to this site', () => {
    expect(
      resolveAppUrlOpenPath(`https://${SITE_HOST}/oauth/google/callback?code=abc&state=xyz`, SITE_HOST),
    ).toBe('/oauth/google/callback?code=abc&state=xyz');
  });

  it('round-trips the return-to-app fallback link, including a Google error', () => {
    for (const search of ['?code=a%2Fb&state=xyz', '?error=access_denied&state=xyz']) {
      expect(resolveAppUrlOpenPath(buildReturnToAppUrl(search), SITE_HOST)).toBe(
        `/oauth/google/callback${search}`,
      );
    }
  });

  it('ignores other hosts, other paths and unparseable urls', () => {
    expect(resolveAppUrlOpenPath('https://evil.example/oauth/google/callback?code=a', SITE_HOST)).toBeNull();
    expect(resolveAppUrlOpenPath(`https://${SITE_HOST}/ledger`, SITE_HOST)).toBeNull();
    expect(resolveAppUrlOpenPath('com.vasu.moneymanager://ledger', SITE_HOST)).toBeNull();
    expect(resolveAppUrlOpenPath('not a url', SITE_HOST)).toBeNull();
  });
});
