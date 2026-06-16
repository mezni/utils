import { registerRootComponent } from "expo";
import { View, StyleSheet } from "react-native";

function App() {
  return (
    <View style={styles.container}>
      <View style={styles.placeholder}>
        <View style={styles.mapPlaceholder} />
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  placeholder: { flex: 1, justifyContent: "center", alignItems: "center" },
  mapPlaceholder: {
    width: "100%",
    height: "100%",
    backgroundColor: "#e8e8e8",
  },
});

registerRootComponent(App);
