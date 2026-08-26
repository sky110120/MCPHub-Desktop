import { GroupPage, ApiResponse } from '@/types';
import { apiPost } from '../utils/fetchInterceptor';

/**
 * Paginated group search (SQL-level LIKE + LIMIT/OFFSET).
 * `page` is 0-based to match the backend command and MultiSelect loader.
 */
export const searchGroups = async (
  searchKey: string,
  page: number,
  pageSize: number,
): Promise<GroupPage> => {
  const response: ApiResponse<GroupPage> = await apiPost('/groups/search', {
    searchKey,
    page,
    pageSize,
  });
  if (!response.success) throw new Error(response.message || 'Failed to search groups');
  return response.data ?? { items: [], total: 0, page, pageSize };
};
