import { useState, type ReactNode } from 'react';
import { StatePill } from './StatePill';
import { TelemetryMini } from './TelemetryMini';
import { QuickActions } from './QuickActions';
import type { EntityStatus, TelemetrySnapshot } from '../../types/common';

export interface HyperColumn<T> {
  key: string;
  header: string;
  width?: string;
  render: (item: T) => ReactNode;
  hideOnMobile?: boolean;
}

export interface HyperRowActions {
  label: string;
  icon: ReactNode;
  onClick: () => void;
  variant?: 'default' | 'danger' | 'success';
}

interface HyperTableProps<T extends { id: string }> {
  columns: HyperColumn<T>[];
  data: T[];
  onRowClick?: (item: T) => void;
  rowActions?: (item: T) => HyperRowActions[];
  expandedRows?: Set<string>;
  renderExpanded?: (item: T) => ReactNode;
  emptyMessage?: string;
  compact?: boolean;
}

export function HyperTable<T extends { id: string }>({
  columns, data, onRowClick, rowActions, expandedRows, renderExpanded,
  emptyMessage, compact,
}: HyperTableProps<T>) {
  const [hoveredId, setHoveredId] = useState<string | null>(null);

  if (!data.length) {
    return (
      <div className="flex flex-col items-center justify-center py-16 gap-4 text-gray-500">
        <svg className="w-10 h-10 text-gray-700" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
        </svg>
        <p className="text-sm">{emptyMessage || 'No data'}</p>
      </div>
    );
  }

  return (
    <div className={`overflow-x-auto ${compact ? '' : 'rounded-xl border border-gray-800'}`}>
      <table className="w-full text-sm">
        <thead>
          <tr className={`${compact ? '' : 'bg-surfaceAlt border-b border-gray-800'}`}>
            {columns.map((col) => (
              <th
                key={col.key}
                className={`px-3 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider
                  ${col.hideOnMobile ? 'hidden lg:table-cell' : ''} ${col.width || ''}`}
              >
                {col.header}
              </th>
            ))}
            {rowActions && <th className="px-3 py-3 text-right w-10"><span className="sr-only">Actions</span></th>}
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-800/40">
          {data.map((item) => {
            const expanded = expandedRows?.has(item.id);
            return (
              <tr key={item.id} className="group">
                <td colSpan={columns.length + (rowActions ? 1 : 0)} className="p-0">
                  <div
                    onMouseEnter={() => setHoveredId(item.id)}
                    onMouseLeave={() => setHoveredId(null)}
                    onClick={() => onRowClick?.(item)}
                    className={`flex items-center w-full transition-all duration-100
                      ${onRowClick ? 'cursor-pointer' : ''}
                      ${hoveredId === item.id ? 'bg-gray-800/40' : ''}`}
                  >
                    {columns.map((col) => (
                      <div
                        key={col.key}
                        className={`px-3 py-2.5 flex items-center gap-2 text-gray-300
                          ${col.hideOnMobile ? 'hidden lg:flex' : ''} ${col.width || 'flex-1'}`}
                      >
                        {col.render(item)}
                      </div>
                    ))}
                    {rowActions && (
                      <div className={`px-3 py-2.5 flex items-center justify-end w-10 transition-opacity duration-100
                        ${hoveredId === item.id ? 'opacity-100' : 'opacity-0 group-hover:opacity-100'}`}
                      >
                        <QuickActions actions={rowActions(item)} />
                      </div>
                    )}
                  </div>
                  {expanded && renderExpanded && (
                    <div className="border-t border-gray-800/30 bg-gray-900/30 animate-slide-up">
                      {renderExpanded(item)}
                    </div>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/* ─── Reusable column helpers ─── */

export const colId = <T extends { id: string }>(width = 'w-32'): HyperColumn<T> => ({
  key: 'id',
  header: 'ID',
  width,
  render: (item) => <span className="font-mono text-xs text-gray-500 truncate">{item.id}</span>,
  hideOnMobile: true,
});

export const colBadge = <T extends { status: EntityStatus }>(width = 'w-28'): HyperColumn<T> => ({
  key: 'status',
  header: 'State',
  width,
  render: (item) => <StatePill status={item.status} pulse={item.status === 'ACTIVE' || item.status === 'CHARGING'} />,
});

export const colTelemetry = <T extends { telemetry: TelemetrySnapshot }>(width = 'w-64'): HyperColumn<T> => ({
  key: 'telemetry',
  header: 'Telemetry',
  width,
  render: (item) => <TelemetryMini telemetry={item.telemetry} compact />,
  hideOnMobile: true,
});
