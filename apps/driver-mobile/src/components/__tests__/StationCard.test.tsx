import React from 'react'
import { render, screen, fireEvent } from '@testing-library/react-native'
import StationCard from '../StationCard'

const mockStation = {
  id: '1',
  name: 'Test Station',
  address: '123 Test St',
  distance: 2.5,
  chargerCount: 10,
  availableCount: 5,
  availability: 'available' as const,
  rating: 4.5,
  reviewCount: 120,
}

describe('StationCard', () => {
  it('renders station information', () => {
    const { getByText } = render(
      <StationCard station={mockStation} onClick={() => {}} />
    )
    expect(getByText('Test Station')).toBeTruthy()
    expect(getByText('123 Test St')).toBeTruthy()
  })

  it('displays charger count', () => {
    const { getByText } = render(
      <StationCard station={mockStation} onClick={() => {}} />
    )
    expect(getByText(/5\/10/)).toBeTruthy()
  })

  it('shows rating', () => {
    const { getByText } = render(
      <StationCard station={mockStation} onClick={() => {}} />
    )
    expect(getByText(/4\.5/)).toBeTruthy()
  })

  it('calls onClick when pressed', () => {
    const onClick = jest.fn()
    const { getByRole } = render(
      <StationCard station={mockStation} onClick={onClick} />
    )
    fireEvent.press(getByRole('button'))
    expect(onClick).toHaveBeenCalledWith('1')
  })

  it('displays availability status', () => {
    const { getByText } = render(
      <StationCard station={mockStation} onClick={() => {}} />
    )
    expect(getByText(/available/i)).toBeTruthy()
  })

  it('shows unavailable status', () => {
    const unavailableStation = { ...mockStation, availability: 'unavailable' as const }
    const { getByText } = render(
      <StationCard station={unavailableStation} onClick={() => {}} />
    )
    expect(getByText(/unavailable/i)).toBeTruthy()
  })
})
