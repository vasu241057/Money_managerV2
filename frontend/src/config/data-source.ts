const REMOTE_DATA_ENV_VALUE = import.meta.env.VITE_USE_REMOTE_DATA;
const API_BASE_URL_ENV_VALUE = import.meta.env.VITE_API_BASE_URL;

function parseBooleanFlag(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined || value.trim() === '') {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();
  if (normalized === 'true' || normalized === '1' || normalized === 'yes') {
    return true;
  }

  if (normalized === 'false' || normalized === '0' || normalized === 'no') {
    return false;
  }

  return fallback;
}

function getStorageOverride(): 'local' | 'remote' | null {
  if (typeof window === 'undefined') {
    return null;
  }

  const override = window.localStorage.getItem('money-manager:data-source');
  if (override === 'local' || override === 'remote') {
    return override;
  }

  return null;
}

export function isRemoteDataEnabled(): boolean {
  const storageOverride = getStorageOverride();
  if (storageOverride !== null) {
    return storageOverride === 'remote';
  }

  return parseBooleanFlag(REMOTE_DATA_ENV_VALUE, true);
}

export function getApiBaseUrl(): string {
  if (!API_BASE_URL_ENV_VALUE || API_BASE_URL_ENV_VALUE.trim() === '') {
    return '/api';
  }

  return API_BASE_URL_ENV_VALUE.trim();
}
