import type { ButtonHTMLAttributes } from 'react';
import { Button as ShadcnButton } from '@/components/ui/button';

const variantMap: Record<string, 'default' | 'secondary' | 'destructive' | 'ghost' | 'outline' | 'link'> = {
  primary: 'default',
  secondary: 'outline',
  danger: 'destructive',
  ghost: 'ghost',
};

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: keyof typeof variantMap;
}

export function Button({ variant = 'primary', className, ...props }: ButtonProps) {
  return <ShadcnButton variant={variantMap[variant]} className={className} {...props} />;
}
