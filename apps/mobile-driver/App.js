import { registerRootComponent } from "expo";
import { View, StyleSheet, Text } from "react-native";

const TUNISIA_COORDS = {
  latitude: 34.0,
  longitude: 9.0,
};

function App() {
  return (
    <View style={styles.container}>
      <Text style={styles.title}>BorneMap Driver</Text>
      <Text style={styles.subtitle}>Tunisia Map View</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#f0f0f0",
    alignItems: "center",
    justifyContent: "center",
  },
  title: {
    fontSize: 24,
    fontWeight: "bold",
    color: "#333",
    marginBottom: 8,
  },
  subtitle: {
    fontSize: 16,
    color: "#666",
  },
});

registerRootComponent(App);
