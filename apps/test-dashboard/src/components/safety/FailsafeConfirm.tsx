import { useState } from 'react';
import { Modal } from '../ui/Modal';
import { Button } from '../ui/Button';

interface FailsafeConfirmProps {
  open: boolean;
  onClose: () => void;
  title: string;
  entityName: string;
  entityId: string;
  destructiveAction: string;
  consequence: string;
  matchString: string;
  onConfirm: () => void;
  loading?: boolean;
  requireApproval?: boolean;
}

export function FailsafeConfirm({
  open, onClose, title, entityName, entityId, destructiveAction,
  consequence, matchString, onConfirm, loading, requireApproval,
}: FailsafeConfirmProps) {
  const [input, setInput] = useState('');
  const [approvedBy, setApprovedBy] = useState('');
  const matched = input === matchString;

  const handleConfirm = () => {
    if (!matched) return;
    if (requireApproval && approvedBy !== 'MANAGER_APPROVED') return;
    onConfirm();
  };

  const handleClose = () => {
    setInput('');
    setApprovedBy('');
    onClose();
  };

  return (
    <Modal open={open} onClose={handleClose} title={title} size="lg">
      <div className="space-y-5">
        {/* Warning */}
        <div className="p-4 bg-red-500/5 border border-red-500/20 rounded-xl flex items-start gap-4">
          <svg className="w-6 h-6 text-red-400 shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
          <div>
            <p className="text-sm font-semibold text-red-400">{destructiveAction}</p>
            <p className="text-sm text-gray-400 mt-1">{consequence}</p>
          </div>
        </div>

        {/* Entity info */}
        <div className="grid grid-cols-2 gap-3 bg-surfaceAlt rounded-xl p-4 text-sm">
          <div>
            <span className="text-gray-500 text-xs">Entity</span>
            <p className="text-gray-200 mt-0.5 font-medium">{entityName}</p>
          </div>
          <div>
            <span className="text-gray-500 text-xs">ID</span>
            <p className="font-mono text-gray-400 mt-0.5 text-xs">{entityId}</p>
          </div>
        </div>

        {/* String match */}
        <div className="space-y-1.5">
          <label className="block text-sm font-medium text-gray-300">
            Type <span className="font-mono text-orange-400 bg-orange-500/10 px-1.5 py-0.5 rounded">{matchString}</span> to confirm
          </label>
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder={`Type "${matchString}" to proceed`}
            className="w-full px-3 py-2.5 bg-surface border border-gray-700 rounded-lg text-foreground font-mono text-sm focus:outline-none focus:ring-2 focus:ring-red-500/30 focus:border-red-500/60"
            autoFocus
          />
          {input && !matched && (
            <p className="text-xs text-red-400">Text does not match the confirmation string</p>
          )}
        </div>

        {/* Two-step manager approval (optional) */}
        {requireApproval && (
          <div className="space-y-1.5 p-4 bg-surfaceAlt rounded-xl border border-yellow-500/20">
            <label className="block text-sm font-medium text-yellow-400 flex items-center gap-2">
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
              </svg>
              Manager Approval Required
            </label>
            <select
              value={approvedBy}
              onChange={(e) => setApprovedBy(e.target.value)}
              className="w-full px-3 py-2.5 bg-surface border border-gray-700 rounded-lg text-foreground text-sm focus:outline-none focus:ring-2 focus:ring-yellow-500/30"
            >
              <option value="">— Select approving manager —</option>
              <option value="MANAGER_APPROVED">Operations Director (on-call)</option>
              <option value="MANAGER_APPROVED">Engineering Lead</option>
              <option value="MANAGER_APPROVED">Network Security Officer</option>
            </select>
          </div>
        )}

        {/* Actions */}
        <div className="flex justify-end gap-3 pt-2 border-t border-gray-800">
          <Button variant="ghost" onClick={handleClose}>Cancel</Button>
          <Button
            onClick={handleConfirm}
            variant="danger"
            loading={loading}
            disabled={!matched || (requireApproval && !approvedBy)}
          >
            Execute {destructiveAction}
          </Button>
        </div>
      </div>
    </Modal>
  );
}
