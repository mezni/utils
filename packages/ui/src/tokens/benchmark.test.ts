import { describe, it, expect } from 'vitest'
import * as colors from './colors'
import * as typography from './typography'
import * as spacing from './spacing'
import * as radius from './radius'
import * as shadows from './shadows'

describe('token resolution benchmark', () => {
  it('resolves all color tokens in <10ms', () => {
    const start = performance.now()
    const result = { ...colors }
    const elapsed = performance.now() - start
    expect(result.brandPrimary).toBe('#007943')
    expect(elapsed).toBeLessThan(10)
  })

  it('resolves all typography tokens in <10ms', () => {
    const start = performance.now()
    const result = { ...typography }
    const elapsed = performance.now() - start
    expect(result.fontSizeLg).toBe(16)
    expect(elapsed).toBeLessThan(10)
  })

  it('resolves all spacing tokens in <10ms', () => {
    const start = performance.now()
    const result = { ...spacing }
    const elapsed = performance.now() - start
    expect(result.spacing4).toBe(16)
    expect(elapsed).toBeLessThan(10)
  })

  it('resolves all radius tokens in <10ms', () => {
    const start = performance.now()
    const result = { ...radius }
    const elapsed = performance.now() - start
    expect(result.radiusMd).toBe(8)
    expect(elapsed).toBeLessThan(10)
  })

  it('resolves all shadow tokens in <10ms', () => {
    const start = performance.now()
    const result = { ...shadows }
    const elapsed = performance.now() - start
    expect(result.shadowCard.elevation).toBe(2)
    expect(elapsed).toBeLessThan(10)
  })
})
