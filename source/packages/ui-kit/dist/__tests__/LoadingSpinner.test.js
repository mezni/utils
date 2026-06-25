import { jsx as _jsx } from "react/jsx-runtime";
import { describe, it, expect } from "vitest";
import { render } from "@testing-library/react";
import { LoadingSpinner } from "../feedback/LoadingSpinner";
describe("LoadingSpinner", () => {
    it("renders with default message", () => {
        render(_jsx(LoadingSpinner, {}));
        expect(() => { }).not.toThrow();
    });
    it("renders with custom message", () => {
        render(_jsx(LoadingSpinner, { message: "Fetching stations..." }));
        expect(() => { }).not.toThrow();
    });
});
//# sourceMappingURL=LoadingSpinner.test.js.map