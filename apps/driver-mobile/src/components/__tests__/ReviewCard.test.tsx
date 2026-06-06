import React from 'react'
import { render } from '@testing-library/react-native'
import ReviewCard from '../ReviewCard'

const mockReview = {
  id: '1',
  stationId: '1',
  userId: 'user1',
  userName: 'John Doe',
  rating: 5,
  text: 'Great charging experience!',
  timestamp: new Date('2024-01-15').toISOString(),
}

describe('ReviewCard', () => {
  it('renders reviewer name', () => {
    const { getByText } = render(<ReviewCard review={mockReview} />)
    expect(getByText('John Doe')).toBeTruthy()
  })

  it('displays rating', () => {
    const { getByText } = render(<ReviewCard review={mockReview} />)
    expect(getByText('5')).toBeTruthy()
  })

  it('shows review text', () => {
    const { getByText } = render(<ReviewCard review={mockReview} />)
    expect(getByText('Great charging experience!')).toBeTruthy()
  })

  it('displays timestamp', () => {
    const { getByText } = render(<ReviewCard review={mockReview} />)
    expect(getByText(/2024/)).toBeTruthy()
  })

  it('renders different ratings', () => {
    const { getByText } = render(
      <ReviewCard review={{ ...mockReview, rating: 3 }} />
    )
    expect(getByText('3')).toBeTruthy()
  })
})
