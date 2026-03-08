import type {
  AccountRow,
  CategoryRow,
  CreateAccountRequest,
  CreateCategoryRequest,
  CreateManualTransactionRequest,
  ListTransactionsQuery,
  PaginatedResponse,
  TransactionFeedItem,
  UpdateAccountRequest,
  UpdateCategoryRequest,
  UpdateTransactionRequest,
} from '../../../shared/types';
import { getApiBaseUrl } from '../config/data-source';
import { getAuthToken, getBootstrapEmailHeader } from './auth-token';

interface ApiEnvelope<T> {
  data: T;
}

interface ErrorPayload {
  error?: string;
  message?: string;
  details?: string;
}

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly details?: string;

  constructor(status: number, code: string, message: string, details?: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

function buildQueryString(query: Record<string, string | number | undefined>): string {
  const params = new URLSearchParams();

  Object.entries(query).forEach(([key, value]) => {
    if (value === undefined) {
      return;
    }

    params.set(key, String(value));
  });

  const serialized = params.toString();
  return serialized ? `?${serialized}` : '';
}

async function readErrorPayload(response: Response): Promise<ErrorPayload> {
  const contentType = response.headers.get('content-type') ?? '';
  if (!contentType.includes('application/json')) {
    return {};
  }

  try {
    return (await response.json()) as ErrorPayload;
  } catch {
    return {};
  }
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = await getAuthToken();
  if (!token) {
    throw new ApiError(
      401,
      'MISSING_AUTH_TOKEN',
      'Missing Clerk bearer token. Configure a token provider before calling the API.',
    );
  }

  const headers = new Headers(init.headers);
  headers.set('Authorization', `Bearer ${token}`);

  const bootstrapEmail = getBootstrapEmailHeader();
  if (bootstrapEmail) {
    headers.set('x-user-email', bootstrapEmail);
  }

  const hasBody = init.body !== undefined;
  if (hasBody && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }

  const response = await fetch(`${getApiBaseUrl()}${path}`, {
    ...init,
    headers,
  });

  if (!response.ok) {
    const payload = await readErrorPayload(response);
    throw new ApiError(
      response.status,
      payload.error ?? 'API_ERROR',
      payload.message ?? `API request failed with status ${response.status}`,
      payload.details,
    );
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}

export const apiClient = {
  async listAccounts(): Promise<AccountRow[]> {
    const response = await request<ApiEnvelope<AccountRow[]>>('/accounts');
    return response.data;
  },

  async createAccount(payload: CreateAccountRequest): Promise<AccountRow> {
    const response = await request<ApiEnvelope<AccountRow>>('/accounts', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    return response.data;
  },

  async updateAccount(accountId: string, payload: UpdateAccountRequest): Promise<AccountRow> {
    const response = await request<ApiEnvelope<AccountRow>>(`/accounts/${accountId}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    });
    return response.data;
  },

  async deleteAccount(accountId: string): Promise<void> {
    await request<void>(`/accounts/${accountId}`, {
      method: 'DELETE',
    });
  },

  async listCategories(type?: CategoryRow['type']): Promise<CategoryRow[]> {
    const query = type ? buildQueryString({ type }) : '';
    const response = await request<ApiEnvelope<CategoryRow[]>>(`/categories${query}`);
    return response.data;
  },

  async createCategory(payload: CreateCategoryRequest): Promise<CategoryRow> {
    const response = await request<ApiEnvelope<CategoryRow>>('/categories', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    return response.data;
  },

  async updateCategory(categoryId: string, payload: UpdateCategoryRequest): Promise<CategoryRow> {
    const response = await request<ApiEnvelope<CategoryRow>>(`/categories/${categoryId}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    });
    return response.data;
  },

  async deleteCategory(categoryId: string): Promise<void> {
    await request<void>(`/categories/${categoryId}`, {
      method: 'DELETE',
    });
  },

  async listTransactions(query: ListTransactionsQuery): Promise<PaginatedResponse<TransactionFeedItem>> {
    const response = await request<PaginatedResponse<TransactionFeedItem>>(
      `/transactions${buildQueryString({
        page: query.page,
        limit: query.limit,
        from: query.from,
        to: query.to,
        status: query.status,
        type: query.type,
        account_id: query.account_id,
        category_id: query.category_id,
        credit_card_id: query.credit_card_id,
      })}`,
    );

    return response;
  },

  async createManualTransaction(payload: CreateManualTransactionRequest): Promise<TransactionFeedItem> {
    const response = await request<ApiEnvelope<TransactionFeedItem>>('/transactions', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    return response.data;
  },

  async updateTransaction(
    transactionId: string,
    payload: UpdateTransactionRequest,
  ): Promise<TransactionFeedItem> {
    const response = await request<ApiEnvelope<TransactionFeedItem>>(`/transactions/${transactionId}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    });
    return response.data;
  },

  async deleteTransaction(transactionId: string): Promise<void> {
    await request<void>(`/transactions/${transactionId}`, {
      method: 'DELETE',
    });
  },
};

export function toErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message;
  }

  if (error instanceof Error) {
    return error.message;
  }

  return 'Unexpected request error';
}
