interface GoogleOAuthCallbackParams {
  code: string | null;
  state: string | null;
  oauthError: string | null;
  hasCallbackParams: boolean;
}

export const GOOGLE_OAUTH_CALLBACK_PATH = '/oauth/google/callback';

// Must match `custom_url_scheme` in the Android project's strings.xml.
export const ANDROID_APP_URL_SCHEME = 'com.vasu.moneymanager';

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

/**
 * The in-app path for a URL Android handed to the app, or null to ignore it.
 * Two forms reach the app, both only for the consent redirect: the verified
 * https link to this site, and the custom-scheme fallback built below.
 */
export function resolveAppUrlOpenPath(url: string, siteHost: string): string | null {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return null;
  }

  let path: string | null = null;
  if (parsed.protocol === 'https:' && parsed.host === siteHost) {
    path = parsed.pathname;
  } else if (parsed.protocol === `${ANDROID_APP_URL_SCHEME}:`) {
    path = `/${parsed.host}${parsed.pathname}`;
  }

  return path === GOOGLE_OAUTH_CALLBACK_PATH ? `${GOOGLE_OAUTH_CALLBACK_PATH}${parsed.search}` : null;
}

export function buildReturnToAppUrl(search: string): string {
  return `${ANDROID_APP_URL_SCHEME}://${GOOGLE_OAUTH_CALLBACK_PATH.slice(1)}${search}`;
}
