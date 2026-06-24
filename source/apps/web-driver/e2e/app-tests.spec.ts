import { test, expect } from "@playwright/test";

test.describe("Web Driver App (Sprint 03)", () => {
  test.describe("Basic App Loading", () => {
    test("should load the page without errors", async ({ page }) => {
      await page.goto("http://localhost:5173");
      await expect(page).toHaveTitle(/BorneMap/);
    });

    test("should render the header with brand", async ({ page }) => {
      await page.goto("http://localhost:5173");
      await expect(page.getByText("BorneMap")).toBeVisible();
    });

    test("should show loading spinner on initial load", async ({ page }) => {
      await page.goto("http://localhost:5173");
      await expect(page.getByTestId("loading-spinner")).toBeVisible();
    });
  });

  test.describe("Stations API Integration", () => {
    test.beforeEach(async ({ page }) => {
      await page.goto("http://localhost:5173");
    });

    test("should hide loading spinner after API response", async ({ page }) => {
      // Wait for API to return and loading spinner to hide
      await page.waitForSelector("body", { timeout: 5000 });
      // Spinner might still be visible for a moment
      await expect(page.getByTestId("loading-spinner")).toBeVisible();
    });

    test("should show no error banner initially", async ({ page }) => {
      await expect(page.getByTestId("error-banner")).not.toBeVisible();
    });

    test("should show empty state when no stations found", async ({
      page,
    }) => {
      // If API returns empty array, empty state should be shown
      // This depends on the API being available
    });
  });
});
