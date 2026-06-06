import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import FilterPills from './FilterPills'

describe('FilterPills', () => {
  it('renders all charger type buttons', () => {
    render(
      <FilterPills
        selectedChargerType="all"
        onChargerTypeChange={() => {}}
        selectedAvailability="all"
        onAvailabilityChange={() => {}}
      />,
    )
    expect(screen.getAllByText('Tous')).toHaveLength(2)
    expect(screen.getByText('Type 2')).toBeInTheDocument()
    expect(screen.getByText('CCS')).toBeInTheDocument()
    expect(screen.getByText('CHAdeMO')).toBeInTheDocument()
  })

  it('renders availability filter buttons', () => {
    render(
      <FilterPills
        selectedChargerType="all"
        onChargerTypeChange={() => {}}
        selectedAvailability="all"
        onAvailabilityChange={() => {}}
      />,
    )
    const buttons = screen.getAllByText('Tous')
    expect(buttons.length).toBeGreaterThanOrEqual(2)
    expect(screen.getByText('Disponible uniquement')).toBeInTheDocument()
  })

  it('highlights selected charger type', () => {
    render(
      <FilterPills
        selectedChargerType="CCS"
        onChargerTypeChange={() => {}}
        selectedAvailability="all"
        onAvailabilityChange={() => {}}
      />,
    )
    const ccsButton = screen.getByText('CCS')
    expect(ccsButton).toHaveClass('bg-brand-primary')
  })

  it('calls onChargerTypeChange on click', () => {
    const onChange = vi.fn()
    render(
      <FilterPills
        selectedChargerType="all"
        onChargerTypeChange={onChange}
        selectedAvailability="all"
        onAvailabilityChange={() => {}}
      />,
    )
    fireEvent.click(screen.getByText('CCS'))
    expect(onChange).toHaveBeenCalledWith('CCS')
  })

  it('calls onAvailabilityChange on click', () => {
    const onChange = vi.fn()
    render(
      <FilterPills
        selectedChargerType="all"
        onChargerTypeChange={() => {}}
        selectedAvailability="all"
        onAvailabilityChange={onChange}
      />,
    )
    fireEvent.click(screen.getByText('Disponible uniquement'))
    expect(onChange).toHaveBeenCalledWith('available')
  })

  it('sets aria-pressed correctly on selected charger type', () => {
    render(
      <FilterPills
        selectedChargerType="CCS"
        onChargerTypeChange={() => {}}
        selectedAvailability="all"
        onAvailabilityChange={() => {}}
      />,
    )
    const ccsButton = screen.getByText('CCS')
    expect(ccsButton).toHaveAttribute('aria-pressed', 'true')
    const type2Button = screen.getByText('Type 2')
    expect(type2Button).toHaveAttribute('aria-pressed', 'false')
  })
})
