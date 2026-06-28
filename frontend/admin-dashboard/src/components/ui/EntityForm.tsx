import { useState } from "react";

interface Field {
  name: string;
  label: string;
  type?: "text" | "number" | "email" | "select";
  required?: boolean;
  placeholder?: string;
  options?: { value: string; label: string }[];
  min?: number;
  step?: string;
}

interface EntityFormProps {
  fields: Field[];
  onSubmit: (values: Record<string, string | number>) => Promise<void>;
  onCancel: () => void;
  submitLabel?: string;
  loading?: boolean;
}

export function EntityForm({
  fields,
  onSubmit,
  onCancel,
  submitLabel = "Save",
  loading,
}: EntityFormProps) {
  const [values, setValues] = useState<Record<string, string | number>>(() => {
    const initial: Record<string, string | number> = {};
    for (const f of fields) {
      initial[f.name] = f.type === "number" ? 0 : "";
    }
    return initial;
  });
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      await onSubmit(values);
    } catch (err) {
      setError(err instanceof Error ? err.message : "An error occurred");
    }
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      {fields.map((field) => (
        <div key={field.name}>
          <label className="label">
            {field.label}
            {field.required && <span className="text-danger-400 ml-1">*</span>}
          </label>
          {field.type === "select" && field.options ? (
            <select
              value={values[field.name] as string}
              onChange={(e) => setValues((v) => ({ ...v, [field.name]: e.target.value }))}
              className="input"
              required={field.required}
            >
              <option value="">Select...</option>
              {field.options.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          ) : (
            <input
              type={field.type || "text"}
              value={values[field.name] as string}
              onChange={(e) =>
                setValues((v) => ({
                  ...v,
                  [field.name]: field.type === "number" ? parseFloat(e.target.value) || 0 : e.target.value,
                }))
              }
              placeholder={field.placeholder}
              className="input"
              required={field.required}
              min={field.min}
              step={field.step}
            />
          )}
        </div>
      ))}

      {error && (
        <div className="rounded-lg bg-danger-500/10 border border-danger-500/20 px-4 py-3 text-sm text-danger-400">
          {error}
        </div>
      )}

      <div className="flex items-center gap-3 pt-2">
        <button type="submit" disabled={loading} className="btn-primary flex-1 justify-center">
          {loading ? "Saving..." : submitLabel}
        </button>
        <button type="button" onClick={onCancel} className="btn-secondary">
          Cancel
        </button>
      </div>
    </form>
  );
}
