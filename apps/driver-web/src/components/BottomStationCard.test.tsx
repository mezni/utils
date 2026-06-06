import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import BottomStationCard from './BottomStationCard'

const mockStation = {
  id: 'STN-001',
  name: 'Station Test',
  address: '15 Avenue Test',
  availability: 'available' as const,
  distance: 2.5,
  chargerCount: 4,
  availableCount: 2,
  rating: 4.2,
}

describe('BottomStationCard', () => {
  it('renders station name', () => {
    render(<BottomStationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByText('Station Test')).toBeInTheDocument()
  })

  it('renders address', () => {
    render(<BottomStationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByText('15 Avenue Test')).toBeInTheDocument()
  })

  it('renders distance', () => {
    render(<BottomStationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByText('2.5 km')).toBeInTheDocument()
  })

  it('renders charger count', () => {
    render(<BottomStationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByText('2/4 Chargeurs')).toBeInTheDocument()
  })

  it('renders rating', () => {
    render(<BottomStationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByText('4.2')).toBeInTheDocument()
  })

  it('shows available badge', () => {
    render(<BottomStationCard station={mockStation} onClick={() => {}} />)
    expect(screen.getByText('Disponible')).toBeInTheDocument()
  })

  it('shows unavailable badge', () => {
    render(<BottomStationCard station={{ ...mockStation, availability: 'unavailable' }} onClick={() => {}} />)
    expect(screen.getByText('Indisponible')).toBeInTheDocument()
  })

  it('calls onClick with station id on card click', () => {
    const onClick = vi.fn()
    render(<BottomStationCard station={mockStation} onClick={onClick} />)
    fireEvent.click(screen.getByText('Station Test').closest('[role="button"]')!)
    expect(onClick).toHaveBeenCalledWith('STN-001')
  })

  it('renders specs when provided', () => {
    render(
      <BottomStationCard
        station={mockStation}
        onClick={() => {}}
        specs={[{ label: 'Type', value: 'CCS' }, { label: 'Puissance', value: '50 kW' }]}
      />,
    )
    expect(screen.getByText('Type')).toBeInTheDocument()
    expect(screen.getByText('CCS')).toBeInTheDocument()
    expect(screen.getByText('Puissance')).toBeInTheDocument()
    expect(screen.getByText('50 kW')).toBeInTheDocument()
  })

  it('does not render specs section when specs is empty', () => {
    const { container } = render(<BottomStationCard station={mockStation} onClick={() => {}} />)
    expect(container.querySelector('.border-t')).not.toBeInTheDocument()
  })

  it('renders directions button when onNavigate provided', () => {
    render(<BottomStationCard station={mockStation} onClick={() => {}} onNavigate={() => {}} />)
    expect(screen.getByText('Itinéraire')).toBeInTheDocument()
  })

  it('does not render directions button when onNavigate not provided', () => {
    render(<BottomStationCard station={mockStation} onClick={() => {}} />)
    expect(screen.queryByText('Itinéraire')).not.toBeInTheDocument()
  })

  it('calls onNavigate with station id on directions click', () => {
    const onNavigate = vi.fn()
    const onClick = vi.fn()
    render(<BottomStationCard station={mockStation} onClick={onClick} onNavigate={onNavigate} />)
    const directionsBtn = screen.getByText('Itinéraire')
    fireEvent.click(directionsBtn)
    expect(onNavigate).toHaveBeenCalledWith('STN-001')
    expect(onClick).not.toHaveBeenCalled()
  })
})
