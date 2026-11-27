import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// En développement, /api est redirigé vers FastAPI (en production, c'est Nginx qui le fait) :
// le navigateur ne parle qu'à une seule adresse, pas besoin de CORS.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: { "/api": { target: "http://127.0.0.1:8000", rewrite: (chemin) => chemin.replace(/^\/api/, "") } },
  },
});
