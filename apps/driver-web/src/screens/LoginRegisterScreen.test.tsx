import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { MemoryRouter } from 'react-router-dom'
import LoginRegisterScreen from './LoginRegisterScreen'

function renderScreen() {
  return render(
    <MemoryRouter>
      <LoginRegisterScreen />
    </MemoryRouter>,
  )
}

describe('LoginRegisterScreen', () => {
  it('renders login tab as active by default', () => {
    renderScreen()
    const loginTab = screen.getByText('Connexion')
    expect(loginTab).toHaveClass('border-brand-primary')
  })

  it('renders register tab', () => {
    renderScreen()
    expect(screen.getByText('Inscription')).toBeInTheDocument()
  })

  it('renders email input', () => {
    renderScreen()
    expect(screen.getByPlaceholderText('email@example.com')).toBeInTheDocument()
  })

  it('renders password input', () => {
    renderScreen()
    expect(screen.getByPlaceholderText('••••••••')).toBeInTheDocument()
  })

  it('renders login button', () => {
    renderScreen()
    expect(screen.getByText('Se connecter')).toBeInTheDocument()
  })

  it('switches to register form on tab click', () => {
    renderScreen()
    fireEvent.click(screen.getByText('Inscription'))
    expect(screen.getByText("S'inscrire")).toBeInTheDocument()
  })

  it('renders social login buttons', () => {
    renderScreen()
    expect(screen.getByText('Se connecter avec Google')).toBeInTheDocument()
    expect(screen.getByText('Se connecter avec Apple')).toBeInTheDocument()
    expect(screen.getByText('Se connecter avec Facebook')).toBeInTheDocument()
  })

  it('renders toggle text', () => {
    renderScreen()
    expect(screen.getByText('Pas encore de compte ?')).toBeInTheDocument()
  })

  it('shows "Déjà un compte ?" when in register mode', () => {
    renderScreen()
    fireEvent.click(screen.getByText('Inscription'))
    expect(screen.getByText('Déjà un compte ?')).toBeInTheDocument()
  })
})
