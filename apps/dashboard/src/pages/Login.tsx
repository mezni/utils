export default function Login() {
  return (
    <div style={styles.container}>
      <div style={styles.card}>
        <h1 style={styles.title}>BorneMap</h1>
        <p style={styles.subtitle}>Partner Dashboard</p>
        <div style={styles.form}>
          <input
            type="email"
            placeholder="Email"
            style={styles.input}
            disabled
          />
          <input
            type="password"
            placeholder="Password"
            style={styles.input}
            disabled
          />
          <button style={styles.button} disabled>
            Sign In
          </button>
        </div>
        <p style={styles.hint}>Keycloak SSO will be integrated in a future sprint.</p>
      </div>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    display: "flex",
    justifyContent: "center",
    alignItems: "center",
    minHeight: "100vh",
    backgroundColor: "#f5f5f5",
  },
  card: {
    background: "white",
    padding: 40,
    borderRadius: 8,
    boxShadow: "0 2px 8px rgba(0,0,0,0.1)",
    textAlign: "center",
    maxWidth: 400,
    width: "100%",
  },
  title: { margin: 0, fontSize: 28, fontWeight: 700 },
  subtitle: { color: "#666", marginTop: 4, marginBottom: 24 },
  form: { display: "flex", flexDirection: "column", gap: 12 },
  input: {
    padding: "10px 12px",
    border: "1px solid #ddd",
    borderRadius: 4,
    fontSize: 14,
  },
  button: {
    padding: "10px 12px",
    backgroundColor: "#0070f3",
    color: "white",
    border: "none",
    borderRadius: 4,
    fontSize: 14,
    cursor: "not-allowed",
    opacity: 0.6,
  },
  hint: { fontSize: 12, color: "#999", marginTop: 16 },
};
