export interface Column<T> {
  key: string;
  label: string;
  render?: (item: T) => React.ReactNode;
  className?: string;
}

interface DataTableProps<T> {
  columns: Column<T>[];
  data: T[];
  keyExtractor: (item: T) => string;
  actions?: (item: T) => React.ReactNode;
}

export function DataTable<T>({ columns, data, keyExtractor, actions }: DataTableProps<T>) {
  return (
    <div className="overflow-x-auto rounded-lg border border-default bg-card">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-default bg-neutral-50">
            {columns.map((col) => (
              <th key={col.key} className={`px-4 py-3 text-left font-medium text-muted ${col.className || ''}`}>
                {col.label}
              </th>
            ))}
            {actions && <th className="px-4 py-3 text-right font-medium text-muted">Actions</th>}
          </tr>
        </thead>
        <tbody>
          {data.map((item) => (
            <tr key={keyExtractor(item)} className="border-b border-subtle last:border-0 hover:bg-neutral-50">
              {columns.map((col) => (
                <td key={col.key} className={`px-4 py-3 text-main ${col.className || ''}`}>
                  {col.render ? col.render(item) : String((item as Record<string, unknown>)[col.key] ?? '')}
                </td>
              ))}
              {actions && (
                <td className="px-4 py-3 text-right">
                  {actions(item)}
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
