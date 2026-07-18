interface GoogleOAuthCallbackParams {
  code: string | null;
  state: string | null;
  oauthError: string | null;
  hasCallbackParams: boolean;
}

export function parseGoogleOAuthCallbackParams(search: string): GoogleOAuthCallbackParams {
  const params = new URLSearchParams(search);
  const code = params.get('code');
  const state = params.get('state');
  const oauthError = params.get('error');

  return {
    code,
    state,
    oauthError,
    hasCallbackParams: !oauthError && Boolean(code) && Boolean(state),
  };
}
