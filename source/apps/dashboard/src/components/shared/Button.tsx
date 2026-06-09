import type { ButtonHTMLAttributes } from 'react';

const variants = {
  primary: 'bg-brand-primary text-white hover:bg-brand-primaryDark',
  secondary: 'bg-white text-main border border-default hover:bg-neutral-50',
  danger: 'bg-status-maintenance text-white hover:bg-red-700',
  ghost: 'text-muted hover:text-main',
};

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: keyof typeof variants;
}

export function Button({ variant = 'primary', className = '', ...props }: ButtonProps) {
  return (
    <button
      className={`inline-flex items-center justify-center rounded-lg px-4 py-2 text-sm font-medium transition-colors disabled:opacity-50 ${variants[variant]} ${className}`}
      {...props}
    />
  );
}
