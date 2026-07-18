import { describe, expect, it } from 'vitest';
import { parseGoogleOAuthCallbackParams } from './gmail-oauth';

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
