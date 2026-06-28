import { ChevronUp, ChevronDown, ChevronsUpDown } from "lucide-react";

interface Column<T> {
  key: string;
  header: string;
  render?: (row: T) => React.ReactNode;
  sortable?: boolean;
  width?: string;
}

interface DataTableProps<T> {
  columns: Column<T>[];
  data: T[];
  onRowClick?: (row: T) => void;
  sortKey?: string;
  sortDir?: "asc" | "desc";
  onSort?: (key: string) => void;
  loading?: boolean;
}

export function DataTable<T extends { id?: string }>({
  columns,
  data,
  onRowClick,
  sortKey,
  sortDir,
  onSort,
  loading,
}: DataTableProps<T>) {
  const SortIcon = ({ column }: { column: string }) => {
    if (sortKey !== column) return <ChevronsUpDown size={14} className="text-surface-500" />;
    return sortDir === "asc" ? (
      <ChevronUp size={14} className="text-brand-400" />
    ) : (
      <ChevronDown size={14} className="text-brand-400" />
    );
  };

  if (loading) {
    return (
      <div className="card p-12 text-center">
        <div className="animate-pulse space-y-4">
          <div className="h-4 bg-surface-700 rounded w-1/3 mx-auto" />
          <div className="h-4 bg-surface-700 rounded w-1/4 mx-auto" />
        </div>
      </div>
    );
  }

  if (data.length === 0) {
    return (
      <div className="card p-12 text-center">
        <p className="text-surface-400 text-sm">No records found</p>
      </div>
    );
  }

  return (
    <div className="card overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-surface-700/50">
            {columns.map((col) => (
              <th
                key={col.key}
                className={`px-4 py-3 text-left text-xs font-medium text-surface-400 uppercase tracking-wider ${
                  col.sortable ? "cursor-pointer select-none hover:text-surface-50" : ""
                }`}
                style={col.width ? { width: col.width } : undefined}
                onClick={() => col.sortable && onSort?.(col.key)}
              >
                <div className="flex items-center gap-1.5">
                  {col.header}
                  {col.sortable && <SortIcon column={col.key} />}
                </div>
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-700/30">
          {data.map((row, i) => (
            <tr
              key={row.id ?? i}
              className={`transition-colors ${
                onRowClick ? "cursor-pointer hover:bg-surface-700/40" : ""
              }`}
              onClick={() => onRowClick?.(row)}
            >
              {columns.map((col) => (
                <td key={col.key} className="px-4 py-3 text-surface-50">
                  {col.render ? col.render(row) : (row as unknown as Record<string, React.ReactNode>)[col.key] ?? "-"}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
