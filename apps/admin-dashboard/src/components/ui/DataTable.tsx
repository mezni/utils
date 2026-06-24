import { LoadingState } from './LoadingState';
import { ErrorState } from './ErrorState';
import { EmptyState } from './EmptyState';
import { Pagination } from './Pagination';
import { Button } from './Button';

export interface Column<T> {
  key: string;
  header: string;
  render?: (item: T) => React.ReactNode;
  sortable?: boolean;
  className?: string;
}

interface DataTableProps<T> {
  columns: Column<T>[];
  data?: T[];
  isLoading: boolean;
  isError: boolean;
  error?: Error | null;
  page: number;
  pages: number;
  onPageChange: (page: number) => void;
  onRowClick?: (item: T) => void;
  emptyTitle?: string;
  emptyMessage?: string;
  onCreate?: () => void;
  onRefresh?: () => void;
}

export function DataTable<T extends { id: string }>({
  columns, data, isLoading, isError, error, page, pages, onPageChange, onRowClick,
  emptyTitle, emptyMessage, onCreate, onRefresh,
}: DataTableProps<T>) {
  if (isLoading) return <LoadingState />;
  if (isError) return <ErrorState message={error?.message} onRetry={onRefresh} />;
  if (!data || data.length === 0) return <EmptyState title={emptyTitle} message={emptyMessage} action={onCreate ? { label: 'Create', onClick: onCreate } : undefined} />;

  return (
    <div className="animate-slide-up">
      <div className="overflow-x-auto rounded-xl border border-gray-800">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-surface border-b border-gray-800">
              {columns.map((col) => (
                <th key={col.key} className={`px-4 py-3.5 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider ${col.className || ''}`}>
                  {col.header}
                </th>
              ))}
              <th className="px-4 py-3.5 text-right text-xs font-semibold text-gray-500 uppercase tracking-wider">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800/50">
            {data.map((item, i) => (
              <tr
                key={item.id || i}
                className={`hover:bg-gray-800/30 transition-colors duration-100 ${onRowClick ? 'cursor-pointer' : ''}`}
                onClick={() => onRowClick?.(item)}
              >
                {columns.map((col) => (
                  <td key={col.key} className={`px-4 py-3 text-gray-300 ${col.className || ''}`}>
                    {col.render ? col.render(item) : ((item as Record<string, unknown>)[col.key] as React.ReactNode) ?? '—'}
                  </td>
                ))}
                <td className="px-4 py-3 text-right">
                  <Button variant="ghost" size="sm" onClick={(e) => { e.stopPropagation(); onRowClick?.(item); }}>View</Button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Pagination page={page} pages={pages} onPageChange={onPageChange} />
    </div>
  );
}
