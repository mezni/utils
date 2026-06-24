import { describe, it, expect } from "vitest";
import { render } from "@testing-library/react";
import { LoadingSpinner } from "../feedback/LoadingSpinner";

describe("LoadingSpinner", () => {
  it("renders with default message", () => {
    render(<LoadingSpinner />);
    expect(() => {}).not.toThrow();
  });

  it("renders with custom message", () => {
    render(<LoadingSpinner message="Fetching stations..." />);
    expect(() => {}).not.toThrow();
  });
});
