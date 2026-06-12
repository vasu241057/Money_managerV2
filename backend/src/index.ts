import fetchWorker from './workers/fetch.worker';
import { handleQueue } from './workers/queue.worker';
import { handleScheduled } from './workers/scheduled.worker';

export default {
	fetch: fetchWorker.fetch,
	scheduled: handleScheduled,
	queue: handleQueue,
} satisfies ExportedHandler<Env>;
