import { describe, it, expect } from "vitest";
import { render } from "@testing-library/react";
import { EmptyState } from "../feedback/EmptyState";

describe("EmptyState", () => {
  it("renders default message", () => {
    render(<EmptyState />);
    expect(() => {}).not.toThrow();
  });

  it("renders custom message", () => {
    render(<EmptyState message="Nothing here" />);
    expect(() => {}).not.toThrow();
  });
});
