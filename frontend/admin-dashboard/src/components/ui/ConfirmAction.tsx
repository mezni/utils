import { useState } from "react";
import { AlertTriangle, X } from "lucide-react";

interface ConfirmActionProps {
  title: string;
  message: string;
  confirmLabel?: string;
  onConfirm: () => Promise<void>;
  trigger: (open: () => void) => React.ReactNode;
}

export function ConfirmAction({
  title,
  message,
  confirmLabel = "Delete",
  onConfirm,
  trigger,
}: ConfirmActionProps) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);

  const handleConfirm = async () => {
    setLoading(true);
    try {
      await onConfirm();
      setOpen(false);
    } finally {
      setLoading(false);
    }
  };

  return (
    <>
      {trigger(() => setOpen(true))}
      {open && (
        <>
          <div className="fixed inset-0 z-50 bg-black/40 backdrop-blur-sm" onClick={() => !loading && setOpen(false)} />
          <div className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-[400px] card p-6 shadow-2xl">
            <div className="flex items-start gap-4">
              <div className="rounded-full bg-danger-500/10 p-2">
                <AlertTriangle size={20} className="text-danger-400" />
              </div>
              <div className="flex-1">
                <h3 className="text-base font-semibold text-surface-50 mb-1">{title}</h3>
                <p className="text-sm text-surface-400">{message}</p>
              </div>
              <button onClick={() => !loading && setOpen(false)} className="btn-ghost p-1 rounded-md" aria-label="Close confirmation">
                <X size={16} />
              </button>
            </div>
            <div className="flex items-center gap-3 justify-end mt-6">
              <button onClick={() => setOpen(false)} disabled={loading} className="btn-secondary text-sm">
                Cancel
              </button>
              <button onClick={handleConfirm} disabled={loading} className="btn-danger text-sm">
                {loading ? "Deleting..." : confirmLabel}
              </button>
            </div>
          </div>
        </>
      )}
    </>
  );
}
