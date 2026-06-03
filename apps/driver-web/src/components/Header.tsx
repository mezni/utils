interface HeaderProps {
  onSearchToggle: () => void
}

function Header({ onSearchToggle }: HeaderProps) {
  return (
    <header className="flex h-14 shrink-0 items-center gap-2 border-b border-[var(--color-border-base)] bg-[var(--color-surface-base)] px-4">
      <span className="text-lg font-bold text-[var(--color-text-base)]">BorneMap</span>
      <div className="flex-1" />
      <button
        onClick={onSearchToggle}
        className="rounded-md px-3 py-1.5 text-sm text-[var(--color-text-muted)] transition-colors hover:bg-[var(--color-surface-hover)]"
      >
        Search
      </button>
    </header>
  )
}

export default Header
