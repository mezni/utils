import { formatId } from "@bornemap/shared-types";

const APP_NAME = "partner-dashboard";
const sampleId = formatId("PRT", "01JAN1234567890");

function App() {
  return <h1>{APP_NAME} | {sampleId}</h1>;
}

export default App;
