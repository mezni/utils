import { type ReactNode } from 'react';

interface AppShellProps {
  sidebar: ReactNode;
  contextBar: ReactNode;
  main: ReactNode;
  contextBarOpen: boolean;
  onToggleContextBar: () => void;
}

export function AppShell({ sidebar, contextBar, main, contextBarOpen, onToggleContextBar }: AppShellProps) {
  return (
    <div className="min-h-screen bg-background text-foreground flex flex-col">
      {/* Top bar */}
      <header className="h-14 border-b border-gray-800 flex items-center justify-between px-4 bg-background/80 backdrop-blur-md sticky top-0 z-20 shrink-0">
        <div className="flex items-center gap-3">
          <div className="w-7 h-7 bg-gradient-to-br from-orange-500 to-orange-600 rounded-lg flex items-center justify-center shadow-lg shadow-orange-500/30">
            <svg className="w-4 h-4 text-slate-900" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
          </div>
          <span className="text-base font-bold font-mono text-white tracking-tight">BorneMap</span>
          <span className="hidden sm:inline-flex items-center gap-1.5 px-2 py-0.5 bg-gray-800 rounded-md text-[10px] text-gray-500 font-mono border border-gray-700">
            CRUD Engine
          </span>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2 text-xs text-gray-600">
            <span className="w-1.5 h-1.5 rounded-full bg-green-500 animate-pulse-dot" />
            <span className="hidden sm:inline">Connected</span>
          </div>
          <button
            onClick={onToggleContextBar}
            className={`p-1.5 rounded-lg transition-all ${contextBarOpen ? 'text-orange-400 bg-orange-500/10' : 'text-gray-500 hover:text-gray-300 hover:bg-gray-800'}`}
            title="Toggle context panel"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h7" />
            </svg>
          </button>
        </div>
      </header>

      {/* Body */}
      <div className="flex flex-1 overflow-hidden">
        {/* Left sidebar */}
        <div className="hidden lg:flex w-60 shrink-0 border-r border-gray-800 flex-col bg-gray-900/30 overflow-y-auto">
          {sidebar}
        </div>

        {/* Context preservation bar */}
        {contextBarOpen && (
          <div className="hidden lg:flex w-72 shrink-0 border-r border-gray-800 flex-col bg-gray-900/20 overflow-y-auto animate-slide-in-left">
            {contextBar}
          </div>
        )}

        {/* Main content */}
        <main className="flex-1 overflow-y-auto p-5 lg:p-6 bg-grid-subtle">
          <div className="max-w-7xl mx-auto">
            {main}
          </div>
        </main>
      </div>
    </div>
  );
}
