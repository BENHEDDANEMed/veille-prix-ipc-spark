import { useEffect, useState } from "react";
import FluxDirect from "./pages/FluxDirect.jsx";
import Groupes from "./pages/Groupes.jsx";
import Previsions from "./pages/Previsions.jsx";

// 3 pages, adressées par le # de l'URL (pas besoin d'un routeur pour si peu)
const PAGES = [
  { id: "flux", titre: "Flux en direct", Page: FluxDirect },
  { id: "previsions", titre: "Prévisions", Page: Previsions },
  { id: "groupes", titre: "Groupes de villes", Page: Groupes },
];
const pageDepuisUrl = () => PAGES.find((p) => p.id === location.hash.slice(1)) ?? PAGES[0];

// Thème : sombre par défaut (style Prism), clair pour le vidéoprojecteur. Le choix est retenu par le navigateur.
function themeRetenu() {
  try { return localStorage.getItem("theme") === "clair" ? "clair" : "sombre"; } catch { return "sombre"; }
}

const formatHeure = new Intl.DateTimeFormat("fr-FR", { hour: "2-digit", minute: "2-digit", second: "2-digit" });

/** Horloge isolée dans son composant : seule elle se redessine chaque seconde, pas toute la page. */
function Horloge({ enDirect }) {
  const [maintenant, setMaintenant] = useState(() => new Date());
  useEffect(() => {
    const minuterie = setInterval(() => setMaintenant(new Date()), 1000);
    return () => clearInterval(minuterie);
  }, []);
  return (
    <span className={`horloge ${enDirect ? "en-direct" : ""}`}>
      <span className="pastille" aria-hidden="true" />
      <time>{formatHeure.format(maintenant)}</time>
    </span>
  );
}

/** Logo : trois barres qui montent dans un cadre en équerre (rappel des coins des panneaux). */
const Logo = () => (
  <svg className="logo" viewBox="0 0 28 28" aria-hidden="true">
    <path d="M2 8V2h6M20 2h6v6M26 20v6h-6M8 26H2v-6" fill="none" stroke="var(--accent)" strokeWidth="2" />
    <rect x="8" y="15" width="3" height="6" rx="1" fill="var(--s3)" />
    <rect x="12.5" y="11" width="3" height="10" rx="1" fill="var(--s2)" />
    <rect x="17" y="7" width="3" height="14" rx="1" fill="var(--s1)" />
  </svg>
);

export default function App() {
  const [page, setPage] = useState(pageDepuisUrl);
  const [enDirect, setEnDirect] = useState(true);
  const [theme, setTheme] = useState(themeRetenu);

  useEffect(() => {
    const suivre = () => setPage(pageDepuisUrl());
    addEventListener("hashchange", suivre);
    return () => removeEventListener("hashchange", suivre);
  }, []);
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    try { localStorage.setItem("theme", theme); } catch { /* navigation privée : le choix n'est pas retenu */ }
  }, [theme]);

  const { Page } = page;
  const clair = theme === "clair";
  return (
    <>
      <header className="entete">
        <a className="marque" href="#flux">
          <Logo />
          <span className="marque-nom">Veille des prix</span>
          <span className="marque-source">IPC HCP</span>
        </a>
        <nav className="segments onglets" aria-label="Pages">
          {PAGES.map((p) => (
            <a key={p.id} href={`#${p.id}`} aria-current={p === page ? "page" : undefined}>{p.titre}</a>
          ))}
        </nav>
        <div className="commandes">
          <Horloge enDirect={enDirect} />
          <button type="button" className="bouton" onClick={() => setEnDirect((v) => !v)} aria-pressed={!enDirect}>
            {enDirect
              ? <><svg viewBox="0 0 16 16" aria-hidden="true"><path d="M5 3v10M11 3v10" stroke="currentColor" strokeWidth="2" /></svg>Pause</>
              : <><svg viewBox="0 0 16 16" aria-hidden="true"><path d="M4 2.5v11l9-5.5z" fill="currentColor" /></svg>Live</>}
          </button>
          <button type="button" className="bouton bouton-icone" onClick={() => setTheme(clair ? "sombre" : "clair")}
                  aria-label={clair ? "Passer en mode sombre" : "Passer en mode clair"} title={clair ? "Mode sombre" : "Mode clair"}>
            {clair
              ? <svg viewBox="0 0 16 16" aria-hidden="true"><path d="M13.5 9.5A6 6 0 0 1 6.5 2.5a6 6 0 1 0 7 7z" fill="currentColor" /></svg>
              : <svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="3" fill="currentColor" /><path d="M8 1v2M8 13v2M1 8h2M13 8h2M3 3l1.4 1.4M11.6 11.6 13 13M3 13l1.4-1.4M11.6 4.4 13 3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>}
          </button>
        </div>
      </header>
      <main className="page">
        <Page enDirect={enDirect} />
      </main>
      <footer className="pied">
        Données : <a href="https://data.gov.ma/data/fr/dataset/data_i4166">HCP, IPC mensuel base 100 2017, sur data.gov.ma</a>.
        Traitement : Apache Spark (Structured Streaming, Spark SQL, MLlib).
      </footer>
    </>
  );
}
