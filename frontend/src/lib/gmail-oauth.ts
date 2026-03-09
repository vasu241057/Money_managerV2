interface GoogleOAuthCallbackParams {
  code: string | null;
  state: string | null;
  oauthError: string | null;
  hasCallbackParams: boolean;
}

interface GoogleOAuthCallbackMessageInput {
  gmailAvailable: boolean;
  oauthError: string | null;
  code: string | null;
  state: string | null;
  callbackError: string | null;
  isLoading: boolean;
  callbackCompleted: boolean;
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

export function resolveGoogleOAuthCallbackMessage(
  input: GoogleOAuthCallbackMessageInput,
): string {
  if (!input.gmailAvailable) {
    return 'Gmail sync is unavailable while remote data mode is disabled.';
  }

  if (input.oauthError) {
    return `Google OAuth error: ${input.oauthError}`;
  }

  if (!input.code || !input.state) {
    return 'Missing OAuth callback parameters. Please retry connecting Gmail.';
  }

  if (input.callbackError) {
    return input.callbackError;
  }

  if (!input.isLoading && input.callbackCompleted) {
    return 'Gmail is connected successfully.';
  }

  return 'Completing Gmail connection...';
}
