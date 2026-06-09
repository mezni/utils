import type { ReactNode } from 'react';

interface PageContentProps {
  children: ReactNode;
}

export function PageContent({ children }: PageContentProps) {
  return (
    <main className="flex-1 overflow-y-auto bg-surface-background p-6">
      {children}
    </main>
  );
}
