const API_BASE_URL_ENV_VALUE = import.meta.env.VITE_API_BASE_URL;

export function getApiBaseUrl(): string {
  if (!API_BASE_URL_ENV_VALUE || API_BASE_URL_ENV_VALUE.trim() === '') {
    return '/api';
  }

  return API_BASE_URL_ENV_VALUE.trim();
}
