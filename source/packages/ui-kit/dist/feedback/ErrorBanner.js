import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import styles from "./ErrorBanner.module.css";
export function ErrorBanner({ message, onRetry }) {
    return (_jsxs("div", { className: styles.banner, role: "alert", "data-testid": "error-banner", children: [_jsx("span", { className: styles.message, children: message }), onRetry && (_jsx("button", { className: styles.retryBtn, onClick: onRetry, "data-testid": "retry-btn", children: "Retry" }))] }));
}
//# sourceMappingURL=ErrorBanner.js.map