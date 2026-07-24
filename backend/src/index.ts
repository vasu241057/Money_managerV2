import fetchWorker from './workers/fetch.worker';
import { toBackendRequest } from './lib/api-request';
import { handleQueue } from './workers/queue.worker';
import { handleScheduled } from './workers/scheduled.worker';

export default {
	fetch(request, env, ctx) {
		return fetchWorker.fetch!(toBackendRequest(request), env, ctx);
	},
	scheduled: handleScheduled,
	queue: handleQueue,
} satisfies ExportedHandler<Env>;
