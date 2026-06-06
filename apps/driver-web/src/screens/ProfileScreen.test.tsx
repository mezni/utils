import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { MemoryRouter } from 'react-router-dom'
import ProfileScreen from './ProfileScreen'

describe('ProfileScreen', () => {
  it('renders profile title', () => {
    render(
      <MemoryRouter>
        <ProfileScreen />
      </MemoryRouter>,
    )
    expect(screen.getByText('Profil')).toBeInTheDocument()
  })

  it('renders avatar initial', () => {
    render(
      <MemoryRouter>
        <ProfileScreen />
      </MemoryRouter>,
    )
    expect(screen.getByText('A')).toBeInTheDocument()
  })

  it('renders name input with default value', () => {
    render(
      <MemoryRouter>
        <ProfileScreen />
      </MemoryRouter>,
    )
    expect(screen.getByDisplayValue('Ahmed Ben Salem')).toBeInTheDocument()
  })

  it('renders email input with default value', () => {
    render(
      <MemoryRouter>
        <ProfileScreen />
      </MemoryRouter>,
    )
    expect(screen.getByDisplayValue('ahmed.bensalem@example.tn')).toBeInTheDocument()
  })

  it('renders phone input with default value', () => {
    render(
      <MemoryRouter>
        <ProfileScreen />
      </MemoryRouter>,
    )
    expect(screen.getByDisplayValue('+216 52 123 456')).toBeInTheDocument()
  })

  it('renders save button', () => {
    render(
      <MemoryRouter>
        <ProfileScreen />
      </MemoryRouter>,
    )
    expect(screen.getByText('Enregistrer')).toBeInTheDocument()
  })

  it('renders input labels', () => {
    render(
      <MemoryRouter>
        <ProfileScreen />
      </MemoryRouter>,
    )
    expect(screen.getByText('Nom')).toBeInTheDocument()
    expect(screen.getByText('Email')).toBeInTheDocument()
    expect(screen.getByText('Téléphone')).toBeInTheDocument()
  })
})
