import { Link } from "react-router-dom";

export default function NotFound() {
  return (
    <div style={styles.container}>
      <h1 style={styles.code}>404</h1>
      <p style={styles.text}>Page not found</p>
      <Link to="/" style={styles.link}>Go to Home</Link>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    justifyContent: "center",
    minHeight: "100vh",
  },
  code: { fontSize: 72, margin: 0, color: "#333" },
  text: { fontSize: 18, color: "#666", margin: "8px 0 24px" },
  link: { color: "#0070f3", textDecoration: "none", fontSize: 14 },
};
