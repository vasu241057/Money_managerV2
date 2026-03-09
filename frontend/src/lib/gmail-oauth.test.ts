import { describe, expect, it } from 'vitest';
import {
  parseGoogleOAuthCallbackParams,
  resolveGoogleOAuthCallbackMessage,
} from './gmail-oauth';

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

  it('resolves message for unavailable remote mode', () => {
    expect(
      resolveGoogleOAuthCallbackMessage({
        gmailAvailable: false,
        oauthError: null,
        code: 'abc',
        state: 'xyz',
        callbackError: null,
        isLoading: false,
        callbackCompleted: false,
      }),
    ).toBe('Gmail sync is unavailable while remote data mode is disabled.');
  });

  it('resolves message for oauth provider error', () => {
    expect(
      resolveGoogleOAuthCallbackMessage({
        gmailAvailable: true,
        oauthError: 'access_denied',
        code: null,
        state: null,
        callbackError: null,
        isLoading: false,
        callbackCompleted: false,
      }),
    ).toBe('Google OAuth error: access_denied');
  });

  it('resolves message for missing callback params', () => {
    expect(
      resolveGoogleOAuthCallbackMessage({
        gmailAvailable: true,
        oauthError: null,
        code: null,
        state: 'xyz',
        callbackError: null,
        isLoading: false,
        callbackCompleted: false,
      }),
    ).toBe('Missing OAuth callback parameters. Please retry connecting Gmail.');
  });

  it('resolves message for backend callback failure', () => {
    expect(
      resolveGoogleOAuthCallbackMessage({
        gmailAvailable: true,
        oauthError: null,
        code: 'abc',
        state: 'xyz',
        callbackError: 'Invalid OAuth state',
        isLoading: false,
        callbackCompleted: false,
      }),
    ).toBe('Invalid OAuth state');
  });

  it('resolves success message once callback completes', () => {
    expect(
      resolveGoogleOAuthCallbackMessage({
        gmailAvailable: true,
        oauthError: null,
        code: 'abc',
        state: 'xyz',
        callbackError: null,
        isLoading: false,
        callbackCompleted: true,
      }),
    ).toBe('Gmail is connected successfully.');
  });
});
