import { ReactNode } from "react";

interface CommandBarProps {
  children: ReactNode;
  className?: string;
}

export function CommandBar({ children, className = "" }: CommandBarProps) {
  return (
    <div className={`flex items-center justify-between mb-6 ${className}`}>
      {children}
    </div>
  );
}

interface CommandBarGroupProps {
  children: ReactNode;
  className?: string;
}

export function CommandBarGroup({ children, className = "" }: CommandBarGroupProps) {
  return (
    <div className={`flex items-center gap-2 ${className}`}>
      {children}
    </div>
  );
}

interface CommandBarTitleProps {
  children: ReactNode;
  className?: string;
}

export function CommandBarTitle({ children, className = "" }: CommandBarTitleProps) {
  return (
    <h2 className="text-xl font-semibold text-gray-900">
      {children}
    </h2>
  );
}