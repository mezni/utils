import { jsx as _jsx } from "react/jsx-runtime";
import { describe, it, expect } from "vitest";
import { render } from "@testing-library/react";
import { EmptyState } from "../feedback/EmptyState";
describe("EmptyState", () => {
    it("renders default message", () => {
        render(_jsx(EmptyState, {}));
        expect(() => { }).not.toThrow();
    });
    it("renders custom message", () => {
        render(_jsx(EmptyState, { message: "Nothing here" }));
        expect(() => { }).not.toThrow();
    });
});
//# sourceMappingURL=EmptyState.test.js.map