import { httpServerHandler } from 'cloudflare:node';

import { createApp } from '../app';

const app = createApp();
const server = app.listen(0);
const address = server.address();
if (!address || typeof address === 'string') {
	throw new Error('Failed to determine internal Express server port');
}

const fetchWorker = httpServerHandler({
	port: address.port,
});

export default fetchWorker;
