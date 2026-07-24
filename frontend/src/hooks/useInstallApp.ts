import { useCallback, useEffect, useState } from 'react';

interface BeforeInstallPromptEvent extends Event {
	prompt: () => Promise<void>;
	userChoice: Promise<{
		outcome: 'accepted' | 'dismissed';
		platform: string;
	}>;
}

function isStandaloneDisplay(): boolean {
	const navigatorWithStandalone = navigator as Navigator & { standalone?: boolean };
	return (
		window.matchMedia('(display-mode: standalone)').matches
		|| navigatorWithStandalone.standalone === true
	);
}

function isAppleMobileDevice(): boolean {
	return /iphone|ipad|ipod/i.test(navigator.userAgent);
}

export function useInstallApp() {
	const [installPrompt, setInstallPrompt] = useState<BeforeInstallPromptEvent | null>(null);
	const [isInstalled, setIsInstalled] = useState(isStandaloneDisplay);
	const [isPrompting, setIsPrompting] = useState(false);
	const [lastOutcome, setLastOutcome] = useState<'accepted' | 'dismissed' | null>(null);

	useEffect(() => {
		const displayMode = window.matchMedia('(display-mode: standalone)');
		const handleDisplayModeChange = () => setIsInstalled(isStandaloneDisplay());
		const handleBeforeInstallPrompt = (event: Event) => {
			event.preventDefault();
			setInstallPrompt(event as BeforeInstallPromptEvent);
		};
		const handleInstalled = () => {
			setInstallPrompt(null);
			setLastOutcome('accepted');
			setIsInstalled(true);
		};

		displayMode.addEventListener('change', handleDisplayModeChange);
		window.addEventListener('beforeinstallprompt', handleBeforeInstallPrompt);
		window.addEventListener('appinstalled', handleInstalled);

		return () => {
			displayMode.removeEventListener('change', handleDisplayModeChange);
			window.removeEventListener('beforeinstallprompt', handleBeforeInstallPrompt);
			window.removeEventListener('appinstalled', handleInstalled);
		};
	}, []);

	const install = useCallback(async () => {
		if (!installPrompt || isPrompting) return;

		setIsPrompting(true);
		try {
			await installPrompt.prompt();
			const choice = await installPrompt.userChoice;
			setLastOutcome(choice.outcome);
			if (choice.outcome === 'accepted') {
				setInstallPrompt(null);
			}
		} finally {
			setIsPrompting(false);
		}
	}, [installPrompt, isPrompting]);

	return {
		canPrompt: installPrompt !== null,
		install,
		isAppleMobile: isAppleMobileDevice(),
		isInstalled,
		isPrompting,
		lastOutcome,
	};
}
