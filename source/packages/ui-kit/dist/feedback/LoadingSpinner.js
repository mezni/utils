import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import styles from "./LoadingSpinner.module.css";
export function LoadingSpinner({ message = "Loading..." }) {
    return (_jsxs("div", { className: styles.spinner, role: "status", "data-testid": "loading-spinner", children: [_jsx("div", { className: styles.animation }), _jsx("span", { children: message })] }));
}
//# sourceMappingURL=LoadingSpinner.js.map