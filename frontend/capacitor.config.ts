import type { CapacitorConfig } from '@capacitor/cli';

const config: CapacitorConfig = {
  appId: 'com.vasu.moneymanager',
  appName: 'Money Manager',
  // Only the offline page ships in the APK; the app itself is the deployed
  // site, so a deploy updates it and it can never drift from the API.
  webDir: 'native-shell',
  backgroundColor: '#FAF8F5',
  server: {
    url: 'https://backend.circuit-issh.workers.dev',
    errorPath: 'offline.html',
  },
};

export default config;
