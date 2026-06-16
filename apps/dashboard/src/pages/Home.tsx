export default function Home() {
  return (
    <div style={styles.layout}>
      <aside style={styles.sidebar}>
        <h2 style={styles.logo}>BorneMap</h2>
        <nav style={styles.nav}>
          <a href="/" style={styles.navItem}>Dashboard</a>
          <a href="/stations" style={styles.navItem}>Stations</a>
          <a href="/partners" style={styles.navItem}>Partners</a>
        </nav>
      </aside>
      <main style={styles.main}>
        <header style={styles.header}>
          <h1 style={styles.heading}>Dashboard</h1>
        </header>
        <div style={styles.content}>
          <p>Welcome to the BorneMap partner dashboard.</p>
        </div>
      </main>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  layout: { display: "flex", minHeight: "100vh" },
  sidebar: {
    width: 240,
    backgroundColor: "#1a1a2e",
    color: "white",
    padding: 20,
    display: "flex",
    flexDirection: "column",
  },
  logo: { fontSize: 20, margin: "0 0 24px 0" },
  nav: { display: "flex", flexDirection: "column", gap: 4 },
  navItem: {
    color: "#ccc",
    textDecoration: "none",
    padding: "8px 12px",
    borderRadius: 4,
    fontSize: 14,
  },
  main: { flex: 1, display: "flex", flexDirection: "column" },
  header: {
    borderBottom: "1px solid #eee",
    padding: "16px 24px",
  },
  heading: { margin: 0, fontSize: 20 },
  content: { padding: 24, flex: 1 },
};
