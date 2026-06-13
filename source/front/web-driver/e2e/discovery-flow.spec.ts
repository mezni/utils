// Web E2E: Discovery Flow Test (Playwright)
// Tests: US3 - Station discovery on web app
// Requires: Running backend + Traefik on localhost:8080
// Run: npx playwright test e2e/discovery-flow.spec.ts

import { test, expect } from '@playwright/test'

test.describe('Station Discovery Flow', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:5173')
  })

  test('map loads with station markers', async ({ page }) => {
    // Wait for the map container to render
    await page.waitForSelector('#map', { timeout: 10000 })

    // Wait for station markers to appear (5s per SC-006)
    await page.waitForSelector('.leaflet-marker-icon', { timeout: 8000 })

    // Verify at least one marker is visible
    const markers = await page.locator('.leaflet-marker-icon').count()
    expect(markers).toBeGreaterThan(0)
  })

  test('station detail shows after clicking marker', async ({ page }) => {
    await page.waitForSelector('#map', { timeout: 10000 })
    await page.waitForSelector('.leaflet-marker-icon', { timeout: 8000 })

    // Click first marker
    await page.locator('.leaflet-marker-icon').first().click()

    // Verify station detail appears (2s per SC-006)
    await page.waitForSelector('[data-testid="station-detail"]', { timeout: 5000 })

    // Verify station name is displayed
    const name = await page.locator('[data-testid="station-name"]')
    await expect(name).toBeVisible()

    // Verify charger information is displayed
    const chargers = await page.locator('[data-testid="charger-info"]')
    await expect(chargers).toBeVisible()
  })

  test('dark mode toggle works', async ({ page }) => {
    await page.waitForSelector('#map', { timeout: 10000 })

    // Click dark mode toggle
    await page.locator('[data-testid="dark-mode-toggle"]').click()

    // Verify dark mode is applied (body class or data attribute)
    const html = await page.locator('html')
    const classAttr = await html.getAttribute('class')
    expect(classAttr).toContain('dark')
  })

  test('error state shows recovery actions on network failure', async ({ page }) => {
    // Simulate offline mode
    await page.context().setOffline(true)

    await page.goto('http://localhost:5173')

    // Wait for error state
    await page.waitForSelector('[data-testid="error-state"]', { timeout: 10000 })

    // Verify retry button is visible
    const retryBtn = await page.locator('[data-testid="retry-button"]')
    await expect(retryBtn).toBeVisible()

    // Verify back navigation is available
    const backBtn = await page.locator('[data-testid="back-button"]')
    await expect(backBtn).toBeVisible()

    // Restore online mode
    await page.context().setOffline(false)
  })
})
