import { useSandbox } from "../../context/sandbox-context"

export function Header() {
  const { isSandboxActive, setSandboxActive } = useSandbox()

  return (
    <header className="flex h-16 items-center justify-between border-b border-gray-200 bg-white px-6">
      <div>
        <h2 className="text-lg font-semibold text-gray-900">Admin Portal</h2>
      </div>
      <div className="flex items-center gap-4">
        <label className="flex cursor-pointer items-center gap-2">
          <span className="text-sm text-gray-600">Sandbox</span>
          <button
            role="switch"
            aria-checked={isSandboxActive}
            onClick={() => setSandboxActive(!isSandboxActive)}
            className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
              isSandboxActive ? "bg-sky-500" : "bg-gray-300"
            }`}
          >
            <span
              className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                isSandboxActive ? "translate-x-6" : "translate-x-1"
              }`}
            />
          </button>
        </label>
      </div>
    </header>
  )
}
