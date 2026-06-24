export function LoadingState() {
  return (
    <div className="space-y-4 p-8">
      <div className="flex items-center justify-center gap-3 text-gray-500 mb-6">
        <svg className="animate-spin h-5 w-5 text-orange-500" viewBox="0 0 24 24" fill="none">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
        <span className="text-sm text-gray-500">Loading data…</span>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 lg:gap-6">
        {[1, 2, 3].map((i) => (
          <div key={i} className="bg-surface border border-gray-800 rounded-xl p-6 space-y-3">
            <div className="h-3 w-20 bg-gray-800 rounded animate-pulse" />
            <div className="h-8 w-24 bg-gray-800 rounded animate-pulse" />
            <div className="h-3 w-16 bg-gray-800/50 rounded animate-pulse" />
          </div>
        ))}
      </div>
      <div className="bg-surface border border-gray-800 rounded-xl p-6 space-y-3">
        <div className="h-3 w-32 bg-gray-800 rounded animate-pulse" />
        <div className="space-y-2">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="h-10 bg-gray-800/50 rounded animate-pulse" />
          ))}
        </div>
      </div>
    </div>
  );
}
