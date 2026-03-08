import type { UUID } from '../../../shared/types';

declare global {
	namespace Express {
		interface Request {
			auth?: {
				userId: UUID;
			};
		}
	}
}

export {};
