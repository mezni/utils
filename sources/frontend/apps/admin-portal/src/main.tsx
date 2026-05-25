import React from "react"
import ReactDOM from "react-dom/client"
import { BrowserRouter, Routes, Route } from "react-router-dom"
import "./index.css"

function App() {
  return (
    <div className="min-h-screen bg-surface">
      <h1 className="text-2xl font-bold p-6 text-accent">BorneMap Admin</h1>
    </div>
  )
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/*" element={<App />} />
      </Routes>
    </BrowserRouter>
  </React.StrictMode>
)
