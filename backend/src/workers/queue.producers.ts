// Which queue a job is sent to (Task 8). Consumers do not care: handling is
// keyed by job_type, and the main consumer accepts nightly types as well, so
// a message on the "wrong" queue still runs correctly.
//
// The fallback below is what makes the split deployable in one step. The
// queue must exist before a producer can send to it, and bindings appear
// only after a deploy — so an unbound NIGHTLY_QUEUE (local dev, unit tests,
// the moment between `wrangler queues create` and the next deploy) routes
// nightly work back to the main queue rather than failing the send.

import { QUEUE_NAMES } from '../lib/infra';

interface NightlyQueueBindings {
	EMAIL_SYNC_QUEUE: Queue;
	NIGHTLY_QUEUE?: Queue;
}

let missingBindingLogged = false;

/** Test seam: the warning below is once-per-isolate. */
export function resetNightlyQueueBindingWarning(): void {
	missingBindingLogged = false;
}

export function resolveNightlyQueue(env: NightlyQueueBindings): Queue {
	if (env.NIGHTLY_QUEUE) {
		return env.NIGHTLY_QUEUE;
	}

	if (!missingBindingLogged) {
		missingBindingLogged = true;
		// Once per isolate: the sender-classification fan-out sends one
		// message per user, and this is a configuration fact, not an event.
		console.warn('NIGHTLY_QUEUE_BINDING_MISSING', {
			fallbackQueue: QUEUE_NAMES.EMAIL_SYNC,
		});
	}

	return env.EMAIL_SYNC_QUEUE;
}
