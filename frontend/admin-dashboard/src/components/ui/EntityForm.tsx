import { ReactNode } from "react";

interface FormField {
  name: string;
  label: string;
  type: "text" | "email" | "number" | "password" | "textarea" | "select";
  required?: boolean;
  placeholder?: string;
  options?: Array<{ value: string; label: string }>;
  className?: string;
}

interface EntityFormProps {
  fields: FormField[];
  initialData?: Record<string, any>;
  onSubmit: (values: Record<string, any>) => void;
  onCancel: () => void;
  submitText?: string;
  cancelText?: string;
  className?: string;
}

export function EntityForm({
  fields,
  initialData = {},
  onSubmit,
  onCancel,
  submitText = "Submit",
  cancelText = "Cancel",
  className = "",
}: EntityFormProps) {
  const [formData, setFormData] = useState<Record<string, any>>(initialData);
  const [errors, setErrors] = useState<Record<string, string>>({});

  const validateForm = () => {
    const newErrors: Record<string, string> = {};
    
    fields.forEach(field => {
      if (field.required && !formData[field.name]) {
        newErrors[field.name] = `${field.label} is required`;
      }
    });
    
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (validateForm()) {
      onSubmit(formData);
    }
  };

  const handleChange = (name: string, value: any) => {
    setFormData(prev => ({ ...prev, [name]: value }));
    // Clear error when user starts typing
    if (errors[name]) {
      setErrors(prev => ({ ...prev, [name]: "" }));
    }
  };

  return (
    <form onSubmit={handleSubmit} className={`space-y-6 ${className}`}>
      {fields.map((field) => (
        <div key={field.name} className="space-y-2">
          <label
            htmlFor={field.name}
            className={`block text-sm font-medium ${
              field.required ? 'text-gray-900' : 'text-gray-700'
            }`}
          >
            {field.label}
            {field.required && <span className="text-red-500">*</span>}
          </label>
          
          {field.type === "textarea" ? (
            <textarea
              id={field.name}
              className={`w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                errors[field.name] ? 'border-red-300' : ''
              }`}
              placeholder={field.placeholder}
              value={formData[field.name] || ""}
              onChange={(e) => handleChange(field.name, e.target.value)}
              rows={4}
            />
          ) : field.type === "select" ? (
            <select
              id={field.name}
              className={`w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                errors[field.name] ? 'border-red-300' : ''
              }`}
              value={formData[field.name] || ""}
              onChange={(e) => handleChange(field.name, e.target.value)}
            >
              <option value="">{field.placeholder}</option>
              {field.options?.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          ) : (
            <input
              type={field.type}
              id={field.name}
              className={`w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                errors[field.name] ? 'border-red-300' : ''
              }`}
              placeholder={field.placeholder}
              value={formData[field.name] || ""}
              onChange={(e) => handleChange(field.name, e.target.value)}
            />
          )}
          
          {errors[field.name] && (
            <p className="text-sm text-red-600">{errors[field.name]}</p>
          )}
        </div>
      ))}
      
      <div className="flex justify-end gap-3 pt-6 border-t border-gray-200">
        <button
          type="button"
          className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-gray-500"
          onClick={onCancel}
        >
          {cancelText}
        </button>
        <button
          type="submit"
          className="px-4 py-2 text-sm font-medium text-white bg-blue-600 border border-transparent rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {submitText}
        </button>
      </div>
    </form>
  );
}