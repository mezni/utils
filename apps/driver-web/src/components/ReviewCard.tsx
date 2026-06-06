interface ReviewCardProps {
  review: {
    id: string
    authorName: string
    rating: number
    text: string
    date: string
    language: 'ar' | 'fr' | 'en'
  }
  maxRating?: number
}

function formatDate(dateStr: string, lang: string): string {
  const d = new Date(dateStr)
  const now = new Date()
  const diffMs = now.getTime() - d.getTime()
  const diffDays = Math.floor(diffMs / 86400000)

  if (diffDays === 0) return lang === 'ar' ? 'اليوم' : "Aujourd'hui"
  if (diffDays === 1) return lang === 'ar' ? 'أمس' : 'Hier'
  if (diffDays < 7) return lang === 'ar' ? `منذ ${diffDays} أيام` : `Il y a ${diffDays} jours`
  return d.toLocaleDateString(lang === 'ar' ? 'ar-TN' : 'fr-TN', { day: 'numeric', month: 'short', year: 'numeric' })
}

export default function ReviewCard({ review, maxRating = 5 }: ReviewCardProps) {
  return (
    <div
      className="rounded-lg border border-neutral-200 bg-white p-4"
      dir={review.language === 'ar' ? 'rtl' : 'ltr'}
    >
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium text-neutral-700">{review.authorName}</span>
        <span className="text-xs text-neutral-400">{formatDate(review.date, review.language)}</span>
      </div>
      <div className="mt-1 flex items-center gap-0.5">
        {Array.from({ length: maxRating }, (_, i) => (
          <svg
            key={i}
            className={`h-4 w-4 ${i < review.rating ? 'text-yellow-400' : 'text-neutral-200'}`}
            fill="currentColor"
            viewBox="0 0 20 20"
          >
            <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
          </svg>
        ))}
        <span className="ml-1 text-xs text-neutral-400">({review.rating}/{maxRating})</span>
      </div>
      <p className="mt-2 text-sm leading-relaxed text-neutral-600">{review.text}</p>
    </div>
  )
}
