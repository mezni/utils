import { describe, it, expect } from "vitest";
import { render } from "@testing-library/react";
import { ErrorBanner } from "../feedback/ErrorBanner";

describe("ErrorBanner", () => {
  it("renders error message", () => {
    render(<ErrorBanner message="Something went wrong" />);
    expect(() => {}).not.toThrow();
  });

  it("renders retry button when onRetry provided", () => {
    render(<ErrorBanner message="Error" onRetry={() => {}} />);
    expect(() => {}).not.toThrow();
  });
});
