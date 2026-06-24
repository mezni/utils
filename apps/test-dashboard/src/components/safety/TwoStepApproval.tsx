import { useState } from 'react';
import { Modal } from '../ui/Modal';
import { Button } from '../ui/Button';

interface TwoStepApprovalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  command: string;
  targetName: string;
  targetId: string;
  onApproved: () => void;
  onRejected: () => void;
}

export function TwoStepApproval({
  open, onClose, title, command, targetName, targetId,
  onApproved, onRejected,
}: TwoStepApprovalProps) {
  const [step, setStep] = useState<'request' | 'verify'>('request');
  const [reason, setReason] = useState('');
  const [approverPin, setApproverPin] = useState('');

  const handleRequest = () => {
    if (!reason.trim()) return;
    setStep('verify');
  };

  const handleVerify = () => {
    if (approverPin !== '424242') return; // simulated manager PIN
    onApproved();
    setStep('request');
    setReason('');
    setApproverPin('');
  };

  const handleClose = () => {
    setStep('request');
    setReason('');
    setApproverPin('');
    onClose();
  };

  return (
    <Modal open={open} onClose={handleClose} title={title} size="md">
      {step === 'request' ? (
        <div className="space-y-4">
          <div className="p-3 bg-blue-500/5 border border-blue-500/20 rounded-xl flex items-start gap-3">
            <svg className="w-5 h-5 text-blue-400 shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <p className="text-xs text-blue-400">
              Step 1 of 2: Request an authorized manager to approve this operation.
            </p>
          </div>

          <div className="bg-surfaceAlt rounded-xl p-4 space-y-2 text-sm">
            <div className="flex justify-between"><span className="text-gray-500">Command</span><span className="font-mono text-orange-400">{command}</span></div>
            <div className="flex justify-between"><span className="text-gray-500">Target</span><span className="text-gray-200">{targetName}</span></div>
            <div className="flex justify-between"><span className="text-gray-500">ID</span><span className="font-mono text-gray-400 text-xs">{targetId}</span></div>
          </div>

          <div className="space-y-1.5">
            <label className="block text-sm font-medium text-gray-300">Reason for {command}</label>
            <textarea
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              rows={3}
              placeholder="Explain why this operation is necessary..."
              className="w-full px-3 py-2.5 bg-surface border border-gray-700 rounded-lg text-foreground text-sm placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-orange-500/30 resize-none"
            />
          </div>

          <div className="flex justify-end gap-3 pt-2">
            <Button variant="ghost" onClick={handleClose}>Cancel</Button>
            <Button onClick={handleRequest} disabled={!reason.trim()}>Request Approval</Button>
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          <div className="p-3 bg-yellow-500/5 border border-yellow-500/20 rounded-xl flex items-start gap-3">
            <svg className="w-5 h-5 text-yellow-400 shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
            </svg>
            <p className="text-xs text-yellow-400">
              Step 2 of 2: Approving manager, enter your security PIN to authorize.
            </p>
          </div>

          <div className="bg-surfaceAlt rounded-xl p-4 space-y-2 text-sm">
            <div className="flex justify-between"><span className="text-gray-500">Command</span><span className="font-mono text-orange-400">{command}</span></div>
            <div className="flex justify-between"><span className="text-gray-500">Reason</span><span className="text-gray-300">{reason}</span></div>
          </div>

          <div className="space-y-1.5">
            <label className="block text-sm font-medium text-gray-300">Manager Security PIN</label>
            <input
              type="password"
              value={approverPin}
              onChange={(e) => setApproverPin(e.target.value)}
              placeholder="Enter 6-digit PIN"
              maxLength={6}
              className="w-full px-3 py-2.5 bg-surface border border-gray-700 rounded-lg text-foreground font-mono text-center text-lg tracking-widest focus:outline-none focus:ring-2 focus:ring-yellow-500/30"
              autoFocus
            />
            <p className="text-xs text-gray-600">Use PIN 424242 (simulated manager approval)</p>
          </div>

          <div className="flex justify-end gap-3 pt-2">
            <Button variant="ghost" onClick={handleClose}>Cancel</Button>
            <Button variant="secondary" onClick={() => { onRejected(); handleClose(); }}>Reject</Button>
            <Button
              onClick={handleVerify}
              disabled={approverPin !== '424242'}
              variant="danger"
            >
              Authorize & Execute
            </Button>
          </div>
        </div>
      )}
    </Modal>
  );
}
