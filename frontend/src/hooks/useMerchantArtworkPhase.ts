import { useEffect, useState } from 'react';

export type ArtworkMode = 'auto' | 'category' | 'merchant';

export interface MerchantArtworkPhase {
	showMerchantLogos: boolean;
	animationCycle: number;
}

export function useMerchantArtworkPhase(mode: ArtworkMode): MerchantArtworkPhase {
	const [phase, setPhase] = useState<MerchantArtworkPhase>({
		showMerchantLogos: false,
		animationCycle: 0,
	});

	useEffect(() => {
		let intervalId: ReturnType<typeof setInterval> | null = null;

		const showFace = (showMerchantLogos: boolean) => {
			setPhase((current) => {
				if (current.showMerchantLogos === showMerchantLogos) return current;
				return {
					showMerchantLogos,
					animationCycle: current.animationCycle + 1,
				};
			});
		};

		const restart = () => {
			if (intervalId) {
				clearInterval(intervalId);
				intervalId = null;
			}

			if (mode !== 'auto') {
				showFace(mode === 'merchant');
				return;
			}

			showFace(false);
			intervalId = setInterval(() => {
				setPhase((current) => ({
					showMerchantLogos: !current.showMerchantLogos,
					animationCycle: current.animationCycle + 1,
				}));
			}, 10_000);
		};

		restart();
		return () => {
			if (intervalId) clearInterval(intervalId);
		};
	}, [mode]);

	return phase;
}
