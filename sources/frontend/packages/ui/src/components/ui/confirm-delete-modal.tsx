import React, { useState } from "react"

interface ConfirmDeleteModalProps {
  isOpen: boolean
  resourceId: string
  resourceLabel: string
  onConfirm: () => void
  onCancel: () => void
}

export function ConfirmDeleteModal({
  isOpen,
  resourceId,
  resourceLabel,
  onConfirm,
  onCancel,
}: ConfirmDeleteModalProps) {
  const [input, setInput] = useState("")

  if (!isOpen) return null

  const isMatch = input === resourceId

  const handleConfirm = () => {
    if (isMatch) {
      onConfirm()
      setInput("")
    }
  }

  const handleCancel = () => {
    setInput("")
    onCancel()
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="w-full max-w-md rounded-2xl bg-white p-6 shadow-float">
        <h3 className="text-lg font-semibold text-gray-900">Confirm Delete</h3>
        <p className="mt-2 text-sm text-gray-600">
          This action is irreversible. Type{" "}
          <code className="rounded bg-gray-100 px-1 py-0.5 font-mono text-sm text-gray-800">
            {resourceId}
          </code>{" "}
          to confirm deletion of <strong>{resourceLabel}</strong>.
        </p>
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder={`Type ${resourceId} to confirm`}
          className="mt-4 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900 placeholder-gray-400 shadow-sm focus:border-red-500 focus:outline-none focus:ring-1 focus:ring-red-500"
        />
        <div className="mt-6 flex justify-end gap-3">
          <button
            onClick={handleCancel}
            className="rounded-lg border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
          >
            Cancel
          </button>
          <button
            onClick={handleConfirm}
            disabled={!isMatch}
            className="rounded-lg bg-red-600 px-4 py-2 text-sm font-medium text-white transition disabled:cursor-not-allowed disabled:bg-red-300"
          >
            Confirm Delete
          </button>
        </div>
      </div>
    </div>
  )
}
