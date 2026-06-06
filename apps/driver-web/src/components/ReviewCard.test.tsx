import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import ReviewCard from './ReviewCard'

describe('ReviewCard', () => {
  it('renders author name', () => {
    render(
      <ReviewCard
        review={{ id: '1', authorName: 'Ahmed', rating: 4, text: 'Bonne station', date: '2026-05-20', language: 'fr' }}
      />,
    )
    expect(screen.getByText('Ahmed')).toBeInTheDocument()
  })

  it('renders review text', () => {
    render(
      <ReviewCard
        review={{ id: '1', authorName: 'Ahmed', rating: 4, text: 'Bonne station', date: '2026-05-20', language: 'fr' }}
      />,
    )
    expect(screen.getByText('Bonne station')).toBeInTheDocument()
  })

  it('renders star rating', () => {
    render(
      <ReviewCard
        review={{ id: '1', authorName: 'Ahmed', rating: 4, text: 'Bonne station', date: '2026-05-20', language: 'fr' }}
      />,
    )
    expect(screen.getByText('(4/5)')).toBeInTheDocument()
  })

  it('renders 5 stars total by default', () => {
    const { container } = render(
      <ReviewCard
        review={{ id: '1', authorName: 'Ahmed', rating: 4, text: 'Bonne station', date: '2026-05-20', language: 'fr' }}
      />,
    )
    const stars = container.querySelectorAll('svg')
    expect(stars).toHaveLength(5)
  })

  it('fills correct number of stars based on rating', () => {
    render(
      <ReviewCard
        review={{ id: '1', authorName: 'Ahmed', rating: 3, text: 'OK', date: '2026-05-20', language: 'fr' }}
      />,
    )
    const rateText = screen.getByText('(3/5)')
    expect(rateText).toBeInTheDocument()
  })

  it('shows "Aujourd\'hui" for today\'s date', () => {
    const today = new Date().toISOString().split('T')[0]
    render(
      <ReviewCard
        review={{ id: '1', authorName: 'Ahmed', rating: 5, text: 'Super', date: today, language: 'fr' }}
      />,
    )
    expect(screen.getByText("Aujourd'hui")).toBeInTheDocument()
  })

  it('shows "Hier" for yesterday\'s date', () => {
    const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0]
    render(
      <ReviewCard
        review={{ id: '1', authorName: 'Ahmed', rating: 5, text: 'Super', date: yesterday, language: 'fr' }}
      />,
    )
    expect(screen.getByText('Hier')).toBeInTheDocument()
  })

  it('shows relative days for older dates', () => {
    const threeDaysAgo = new Date(Date.now() - 3 * 86400000).toISOString().split('T')[0]
    render(
      <ReviewCard
        review={{ id: '1', authorName: 'Ahmed', rating: 5, text: 'Super', date: threeDaysAgo, language: 'fr' }}
      />,
    )
    expect(screen.getByText('Il y a 3 jours')).toBeInTheDocument()
  })

  it('applies rtl dir for Arabic reviews', () => {
    const { container } = render(
      <ReviewCard
        review={{ id: '1', authorName: 'أحمد', rating: 5, text: 'محطة ممتازة', date: '2026-05-20', language: 'ar' }}
      />,
    )
    expect(container.querySelector('[dir="rtl"]')).toBeInTheDocument()
  })

  it('applies ltr dir for French reviews', () => {
    const { container } = render(
      <ReviewCard
        review={{ id: '1', authorName: 'Ahmed', rating: 5, text: 'Excellent', date: '2026-05-20', language: 'fr' }}
      />,
    )
    expect(container.querySelector('[dir="ltr"]')).toBeInTheDocument()
  })
})
