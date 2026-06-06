import { useTranslation } from 'react-i18next'

interface SearchBarProps {
  value: string
  onChange: (value: string) => void
  onSubmit: (value: string) => void
  placeholder?: string
  autoFocus?: boolean
}

export default function SearchBar({ value, onChange, onSubmit, placeholder, autoFocus }: SearchBarProps) {
  const { t } = useTranslation()

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      onSubmit(value)
    }
  }

  return (
    <div className="mx-4 my-2 flex items-center rounded-lg bg-white px-3 py-2 shadow-md">
      <svg className="mr-2 h-5 w-5 text-neutral-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
      </svg>
      <input
        type="text"
        value={value}
        onChange={e => onChange(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder={placeholder || t('home.searchPlaceholder')}
        autoFocus={autoFocus}
        className="w-full bg-transparent text-sm focus:outline-none"
        aria-label="Search stations"
      />
    </div>
  )
}
