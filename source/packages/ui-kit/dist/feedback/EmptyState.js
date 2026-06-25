import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import styles from "./EmptyState.module.css";
export function EmptyState({ message = "No charging stations found in this area." }) {
    return (_jsxs("div", { className: styles.container, "data-testid": "empty-state", children: [_jsx("div", { className: styles.icon, children: "\u25C7" }), _jsx("span", { children: message })] }));
}
//# sourceMappingURL=EmptyState.js.map