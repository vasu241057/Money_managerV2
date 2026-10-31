import { App } from '@capacitor/app';
import { Browser } from '@capacitor/browser';
import { Capacitor } from '@capacitor/core';
import { resolveAppUrlOpenPath } from './gmail-oauth';

const HANDLED_APP_URL_KEY = 'money-manager:handled-app-url';

/** True inside the Android app, which loads this same site in a web view. */
export function isNativeApp(): boolean {
  return Capacitor.isNativePlatform();
}

export function isAndroidBrowser(): boolean {
  return !isNativeApp() && /Android/i.test(navigator.userAgent);
}

function openAppUrl(url: string): void {
  const path = resolveAppUrlOpenPath(url, window.location.host);
  // The launch URL is reported again after every page load, and the
  // authorization code in it only works once.
  if (!path || sessionStorage.getItem(HANDLED_APP_URL_KEY) === url) return;

  sessionStorage.setItem(HANDLED_APP_URL_KEY, url);
  window.location.assign(path);
}

export function initNativeApp(): void {
  if (!isNativeApp()) return;

  document.documentElement.classList.add('native-app');
  void App.addListener('appUrlOpen', ({ url }) => openAppUrl(url));
  // Covers Android having killed the app while the Chrome tab was open.
  void App.getLaunchUrl().then((launch) => {
    if (launch?.url) openAppUrl(launch.url);
  });
}

/**
 * Google refuses to show its consent screen inside an app's web view, so the
 * app opens it in a Chrome tab; the redirect comes back via `appUrlOpen`.
 */
export async function openGoogleConsent(authUrl: string): Promise<void> {
  if (isNativeApp()) {
    await Browser.open({ url: authUrl });
    return;
  }

  window.location.assign(authUrl);
}
