import { formatId } from "@bornemap/shared-types";

const APP_NAME = "admin-dashboard";
const sampleId = formatId("USR", "01JAN1234567890");

function App() {
  return <h1>{APP_NAME} | {sampleId}</h1>;
}

export default App;
