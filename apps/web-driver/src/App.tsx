import { MapContainer, TileLayer } from "react-leaflet";
import "leaflet/dist/leaflet.css";

const TUNISIA_CENTER: [number, number] = [34.0, 9.0];

function App() {
  return (
    <MapContainer
      center={TUNISIA_CENTER}
      zoom={7}
      style={{ width: "100%", height: "100vh" }}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
    </MapContainer>
  );
}

export default App;
