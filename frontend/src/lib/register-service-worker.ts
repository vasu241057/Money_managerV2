import { isNativeApp } from './native-app';

export function registerServiceWorker(): void {
	// In the app, a cached shell could be served without Capacitor's bridge
	// injected, and the plugins would silently stop working.
	if (!import.meta.env.PROD || isNativeApp() || !('serviceWorker' in navigator)) return;

	window.addEventListener('load', () => {
		void navigator.serviceWorker.register('/service-worker.js').catch((error: unknown) => {
			console.warn('Service worker registration failed', error);
		});
	});
}
