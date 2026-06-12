export interface VersionPayload {
	name: string;
	version: string;
	environment: string;
	runtime: 'cloudflare-workers';
}

export function getVersionPayload(config: {
	appName: string;
	appVersion: string;
	nodeEnv: string;
}): VersionPayload {
	return {
		name: config.appName,
		version: config.appVersion,
		environment: config.nodeEnv,
		runtime: 'cloudflare-workers',
	};
}
