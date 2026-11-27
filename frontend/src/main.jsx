import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
// Polices embarquées dans l'application (pas de Google Fonts) : la démo fonctionne sans Internet.
// Geist pour le texte et les grands chiffres, Geist Mono pour les petits sous-titres (style Prism).
import "@fontsource/geist-sans/400.css";
import "@fontsource/geist-sans/500.css";
import "@fontsource/geist-sans/600.css";
import "@fontsource/geist-sans/700.css";
import "@fontsource/geist-mono/400.css";
import "@fontsource/geist-mono/500.css";
import "./styles.css";
import App from "./App.jsx";

createRoot(document.getElementById("racine")).render(<StrictMode><App /></StrictMode>);
