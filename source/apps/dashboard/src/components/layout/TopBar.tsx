interface TopBarProps {
  title: string;
}

export function TopBar({ title }: TopBarProps) {
  return (
    <header className="flex h-16 items-center justify-between border-b border-default bg-card px-6">
      <h1 className="text-lg font-semibold text-main">{title}</h1>
      <div className="flex h-8 w-8 items-center justify-center rounded-full bg-neutral-200 text-sm font-medium text-muted">
        A
      </div>
    </header>
  );
}
