import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";

export default defineConfig({
  resolve: {
    alias: {
      "cloudflare-worker-kvdb": fileURLToPath(new URL("./src/index.ts", import.meta.url)),
    },
  },
  test: {
    globals: true,
    environment: "node",
  },
});

