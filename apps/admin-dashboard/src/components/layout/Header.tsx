import { useAuthStore } from '@/stores/auth-store'
import { useLogout } from '@/hooks/use-auth'

export function Header() {
  const user = useAuthStore((s) => s.user)
  const { mutate: doLogout, isPending } = useLogout()

  return (
    <header className="sticky top-0 z-30 h-16 bg-background border-b border-border flex items-center justify-between px-6">
      <div>
        <h1 className="font-heading text-foreground text-lg font-semibold">
          Admin Dashboard
        </h1>
      </div>

      <div className="flex items-center gap-4">
        <div className="flex items-center gap-3">
          <div className="flex flex-col items-end">
            <span className="text-sm font-medium text-foreground">
              {user?.email}
            </span>
            <span className="text-xs text-foreground/50 capitalize">
              {user?.role}
            </span>
          </div>
          <div
            className="w-9 h-9 rounded-full bg-primary/20 flex items-center justify-center"
            aria-hidden="true"
          >
            <span className="text-sm font-semibold text-primary font-heading">
              {user?.email?.charAt(0).toUpperCase()}
            </span>
          </div>
        </div>

        <button
          onClick={() => doLogout()}
          disabled={isPending}
          className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-foreground/60 hover:text-destructive transition-colors duration-150 cursor-pointer disabled:opacity-50"
          aria-label="Logout"
        >
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
            <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
            <polyline points="16 17 21 12 16 7" />
            <line x1="21" y1="12" x2="9" y2="12" />
          </svg>
          <span className="hidden sm:inline">Logout</span>
        </button>
      </div>
    </header>
  )
}
