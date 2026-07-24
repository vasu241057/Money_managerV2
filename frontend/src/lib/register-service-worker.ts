export function registerServiceWorker(): void {
	if (!import.meta.env.PROD || !('serviceWorker' in navigator)) return;

	window.addEventListener('load', () => {
		void navigator.serviceWorker.register('/service-worker.js').catch((error: unknown) => {
			console.warn('Service worker registration failed', error);
		});
	});
}
