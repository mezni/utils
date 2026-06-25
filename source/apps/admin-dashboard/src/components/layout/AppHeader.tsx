export function AppHeader() {
  return (
    <header className="sticky top-0 z-40 flex h-14 items-center gap-4 border-b bg-background px-6">
      <div className="flex items-center gap-2 font-semibold text-lg">
        <span className="text-primary">BorneMap</span>
        <span className="text-muted-foreground">Admin</span>
      </div>
      <div className="ml-auto flex items-center gap-2">
        <div className="flex h-8 w-8 items-center justify-center rounded-full bg-muted text-xs font-medium text-muted-foreground">
          A
        </div>
      </div>
    </header>
  );
}
