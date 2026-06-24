import { useState } from 'react';
import { Modal } from '../ui/Modal';
import { Button } from '../ui/Button';
import { StatePill } from '../data/StatePill';
import type { ChargerState } from '../../types/common';

interface UnbindArchiveFlowProps {
  open: boolean;
  onClose: () => void;
  chargerId: string;
  chargerState: ChargerState;
  chargeBoxId: string;
  stationName: string;
  sessionActive: boolean;
  onConfirm: () => void;
  loading?: boolean;
}

export function UnbindArchiveFlow({
  open, onClose, chargerId, chargerState, chargeBoxId, stationName,
  sessionActive, onConfirm, loading,
}: UnbindArchiveFlowProps) {
  const [step, setStep] = useState<'review' | 'confirm' | 'complete'>('review');
  const [confirmText, setConfirmText] = useState('');

  const handleConfirm = () => {
    if (step === 'review') {
      if (sessionActive) {
        return; // must wait for session
      }
      setStep('confirm');
    } else if (step === 'confirm') {
      onConfirm();
      setStep('complete');
    }
  };

  const handleClose = () => {
    setStep('review');
    setConfirmText('');
    onClose();
  };

  return (
    <Modal open={open} onClose={handleClose} title="Unbind & Archive Charger" size="lg">
      {step === 'review' && (
        <div className="space-y-5">
          <div className={`flex items-start gap-4 p-4 border rounded-xl ${sessionActive ? 'bg-yellow-500/5 border-yellow-500/20' : 'bg-orange-500/5 border-orange-500/20'}`}>
            <svg className={`w-6 h-6 shrink-0 mt-0.5 ${sessionActive ? 'text-yellow-400' : 'text-orange-400'}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={sessionActive ? "M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" : "M13 10V3L4 14h7v7l9-11h-7z"} />
            </svg>
            <div>
              <p className={`text-sm font-semibold ${sessionActive ? 'text-yellow-400' : 'text-orange-400'}`}>
                {sessionActive ? 'Active Charging Session' : 'Unbind Charger'}
              </p>
              <p className="text-sm text-gray-400 mt-1">
                {sessionActive
                  ? `Charger ${chargeBoxId} has an active session. The unbind will be queued and auto-execute once the current session completes gracefully.`
                  : `This will immediately remove charger ${chargeBoxId} from all OCPI roaming maps and archive it. Historical billing data is preserved.`}
              </p>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 bg-surfaceAlt rounded-xl p-4 text-sm">
            <div><span className="text-gray-500 text-xs">Charger ID</span><p className="font-mono text-gray-200 mt-0.5">{chargerId}</p></div>
            <div><span className="text-gray-500 text-xs">State</span><div className="mt-0.5"><StatePill status={chargerState as any} size="sm" /></div></div>
            <div><span className="text-gray-500 text-xs">ChargeBox ID</span><p className="font-mono text-gray-200 mt-0.5">{chargeBoxId}</p></div>
            <div><span className="text-gray-500 text-xs">Station</span><p className="text-gray-200 mt-0.5">{stationName}</p></div>
          </div>

          {sessionActive && (
            <div className="p-3 bg-blue-500/5 border border-blue-500/20 rounded-xl flex items-start gap-2.5">
              <svg className="w-4 h-4 text-blue-400 mt-0.5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              <p className="text-xs text-blue-400">Session will be allowed to finish naturally. Unbind processes at session end.</p>
            </div>
          )}

          <div className="flex justify-end gap-3">
            <Button variant="ghost" onClick={handleClose}>Cancel</Button>
            <Button
              onClick={handleConfirm}
              variant="danger"
              disabled={sessionActive}
            >
              {sessionActive ? 'Queue Unbind (Session Active)' : 'Continue Unbind'}
            </Button>
          </div>
        </div>
      )}

      {step === 'confirm' && (
        <div className="space-y-5">
          <div className="p-4 bg-red-500/5 border border-red-500/20 rounded-xl flex items-start gap-4">
            <svg className="w-6 h-6 text-red-400 shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
            </svg>
            <div>
              <p className="text-sm font-semibold text-red-400">Final Confirmation Required</p>
              <p className="text-sm text-gray-400 mt-1">
                This will permanently remove <strong className="text-white">{chargeBoxId}</strong> from the network.
                Type <strong className="font-mono text-orange-400">{chargeBoxId}</strong> to confirm.
              </p>
            </div>
          </div>

          <div className="space-y-1.5">
            <label className="block text-sm font-medium text-gray-300">Type charger ID to confirm</label>
            <input
              value={confirmText}
              onChange={(e) => setConfirmText(e.target.value)}
              placeholder={chargeBoxId}
              className="w-full px-3 py-2.5 bg-surface border border-gray-700 rounded-lg text-foreground font-mono text-sm focus:outline-none focus:ring-2 focus:ring-red-500/30 focus:border-red-500/60"
              autoFocus
            />
          </div>

          <div className="flex justify-end gap-3 pt-2">
            <Button variant="ghost" onClick={handleClose}>Cancel</Button>
            <Button
              onClick={handleConfirm}
              variant="danger"
              disabled={confirmText !== chargeBoxId}
              loading={loading}
            >
              Confirm Unbind & Archive
            </Button>
          </div>
        </div>
      )}

      {step === 'complete' && (
        <div className="space-y-5 py-4 text-center">
          <div className="w-16 h-16 mx-auto bg-green-500/10 rounded-full flex items-center justify-center">
            <svg className="w-8 h-8 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
            </svg>
          </div>
          <p className="text-lg font-semibold text-green-400 font-mono">Charger Unbound</p>
          <p className="text-sm text-gray-400 max-w-sm mx-auto">
            {chargeBoxId} has been removed from OCPI roaming maps and archived.
            Historical billing and session data is preserved.
          </p>
          <Button onClick={handleClose} variant="primary">Done</Button>
        </div>
      )}
    </Modal>
  );
}
