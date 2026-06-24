import { Modal } from '../ui/Modal';
import { Button } from '../ui/Button';

interface DependencyCheckModalProps {
  open: boolean;
  onClose: () => void;
  entityType: 'Partner' | 'Station';
  entityName: string;
  dependentCount: number;
  dependentType: string;
  onReassign: () => void;
  onForceDelete: () => void;
  loading?: boolean;
}

export function DependencyCheckModal({
  open, onClose, entityType, entityName, dependentCount, dependentType,
  onReassign, onForceDelete, loading,
}: DependencyCheckModalProps) {
  return (
    <Modal open={open} onClose={onClose} title={`Cannot Delete ${entityType}`} size="lg">
      <div className="space-y-5">
        <div className="flex items-start gap-4 p-4 bg-yellow-500/5 border border-yellow-500/20 rounded-xl">
          <svg className="w-6 h-6 text-yellow-400 shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
          <div>
            <p className="text-sm font-semibold text-yellow-400">Active Dependencies Found</p>
            <p className="text-sm text-gray-400 mt-1">
              <strong className="text-white">{entityName}</strong> has <strong className="text-yellow-400">{dependentCount}</strong> active {dependentType} assigned.
              Deleting this {entityType.toLowerCase()} will orphan these assets.
            </p>
          </div>
        </div>

        <div className="space-y-3 bg-surfaceAlt rounded-xl p-4">
          <p className="text-sm font-medium text-gray-300">Recommended: Reassign Dependencies</p>
          <p className="text-xs text-gray-500">
            Transfer {dependentType} to another {entityType.toLowerCase()} before deletion.
            This preserves operational continuity and historical data lineage.
          </p>
          <Button onClick={onReassign} variant="primary" size="md" className="w-full">
            Reassign {dependentCount} {dependentType}
          </Button>
        </div>

        <div className="border-t border-gray-800 pt-4">
          <p className="text-xs text-gray-600 mb-3 font-medium">Destructive options (not recommended for production):</p>
          <Button
            onClick={onForceDelete}
            variant="danger"
            size="sm"
            loading={loading}
          >
            Force Delete — Cascade to {dependentCount} {dependentType}
          </Button>
          <p className="text-xs text-gray-600 mt-2">
            This will permanently remove this {entityType.toLowerCase()} and all associated {dependentType}.
            Billing and session history will be preserved.
          </p>
        </div>

        <div className="flex justify-end pt-2 border-t border-gray-800">
          <Button variant="ghost" onClick={onClose}>Cancel</Button>
        </div>
      </div>
    </Modal>
  );
}
