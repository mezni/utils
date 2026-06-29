import { useEffect, useState } from "react";
import { X, CheckCircle, AlertCircle, AlertTriangle, Info } from "lucide-react";

interface Toast {
  id: string;
  type: "success" | "error" | "warning" | "info";
  title: string;
  description?: string;
  duration?: number;
}

interface ToastContainerProps {
  toasts: Toast[];
  onRemove?: (id: string) => void;
}

const iconMap = {
  success: CheckCircle,
  error: AlertCircle,
  warning: AlertTriangle,
  info: Info,
};

const styleMap = {
  success: "border-emerald-200 bg-emerald-50 text-emerald-800",
  error: "border-red-200 bg-red-50 text-red-800",
  warning: "border-amber-200 bg-amber-50 text-amber-800",
  info: "border-blue-200 bg-blue-50 text-blue-800",
};

const iconColorMap = {
  success: "text-emerald-500",
  error: "text-red-500",
  warning: "text-amber-500",
  info: "text-blue-500",
};

export function ToastContainer({ toasts, onRemove }: ToastContainerProps) {
  return (
    <div className="fixed top-4 right-4 z-50 space-y-2 w-full max-w-sm pointer-events-none">
      {toasts.map((toast) => {
        const Icon = iconMap[toast.type];
        return (
          <ToastItem
            key={toast.id}
            toast={toast}
            Icon={Icon}
            onRemove={onRemove}
            styleClass={styleMap[toast.type]}
            iconColor={iconColorMap[toast.type]}
          />
        );
      })}
    </div>
  );
}

function ToastItem({
  toast,
  Icon,
  onRemove,
  styleClass,
  iconColor,
}: {
  toast: Toast;
  Icon: React.ComponentType<{ size?: number; className?: string }>;
  onRemove?: (id: string) => void;
  styleClass: string;
  iconColor: string;
}) {
  useEffect(() => {
    if (toast.duration && toast.duration > 0) {
      const timer = setTimeout(() => onRemove?.(toast.id), toast.duration);
      return () => clearTimeout(timer);
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div
      className={`pointer-events-auto flex items-start gap-3 p-4 rounded-lg border shadow-lg animate-slide-in-right ${styleClass}`}
    >
      <Icon size={20} className={`flex-shrink-0 mt-0.5 ${iconColor}`} />
      <div className="flex-1 min-w-0">
        <p className="font-medium text-sm">{toast.title}</p>
        {toast.description && (
          <p className="text-sm mt-0.5 opacity-90">{toast.description}</p>
        )}
      </div>
      <button
        onClick={() => onRemove?.(toast.id)}
        className="flex-shrink-0 p-0.5 rounded hover:bg-black/5 transition-colors"
      >
        <X size={16} />
      </button>
    </div>
  );
}

export interface UseToastResult {
  toasts: Toast[];
  toast: (type: Toast["type"], title: string, description?: string, duration?: number) => void;
  removeToast: (id: string) => void;
}

export function useToast(): UseToastResult {
  const [toasts, setToasts] = useState<Toast[]>([]);

  const toast = (type: Toast["type"], title: string, description?: string, duration = 5000) => {
    const id = Math.random().toString(36).substr(2, 9);
    setToasts((prev) => [...prev, { id, type, title, description, duration }]);
    if (duration > 0) {
      setTimeout(() => {
        setToasts((prev) => prev.filter((t) => t.id !== id));
      }, duration);
    }
  };

  const removeToast = (id: string) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  };

  return { toasts, toast, removeToast };
}
