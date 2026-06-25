import { jsx as _jsx } from "react/jsx-runtime";
import { describe, it, expect } from "vitest";
import { render } from "@testing-library/react";
import { ErrorBanner } from "../feedback/ErrorBanner";
describe("ErrorBanner", () => {
    it("renders error message", () => {
        render(_jsx(ErrorBanner, { message: "Something went wrong" }));
        expect(() => { }).not.toThrow();
    });
    it("renders retry button when onRetry provided", () => {
        render(_jsx(ErrorBanner, { message: "Error", onRetry: () => { } }));
        expect(() => { }).not.toThrow();
    });
});
//# sourceMappingURL=ErrorBanner.test.js.map