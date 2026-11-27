import { useEffect, useId, useRef, useState } from "react";
import { Area, AreaChart, ResponsiveContainer, YAxis } from "recharts";
import { DIVISIONS, decimal } from "./outils.js";

/** Panneau Prism : titre, sous-titre en police mono, actions à droite, 4 coins en équerre (dessinés en CSS). */
export function Panneau({ titre, sousTitre, actions, className = "", style, children }) {
  const id = useId();
  return (
    <section className={`panneau ${className}`} style={style} aria-labelledby={id}>
      <header className="panneau-tete">
        <div>
          <h2 id={id}>{titre}</h2>
          {sousTitre && <p className="mono">{sousTitre}</p>}
        </div>
        {actions}
      </header>
      {children}
    </section>
  );
}

/** Sélecteur segmenté (comme « Live / 24H / 7D / 30D ») : le choix actif est rempli en turquoise. */
export function Segments({ etiquette, options, valeur, onChange }) {
  return (
    <div className="segments" role="radiogroup" aria-label={etiquette}>
      {options.map((o) => (
        <button key={o.valeur} type="button" role="radio" aria-checked={o.valeur === valeur} onClick={() => onChange(o.valeur)}>
          {o.texte}
        </button>
      ))}
    </div>
  );
}

const mouvementReduit = () => matchMedia("(prefers-reduced-motion: reduce)").matches;

/** Chiffre qui défile de sa valeur précédente (0 au premier affichage) jusqu'à la nouvelle.
 *  Le lecteur d'écran n'entend que la valeur finale. */
export function Compteur({ valeur, format = (v) => decimal(v, 0) }) {
  const [affichee, setAffichee] = useState(0);
  const courante = useRef(0); // valeur à l'écran à l'instant t (point de départ de la prochaine animation)
  useEffect(() => {
    if (valeur == null) return;
    const depart = courante.current;
    if (mouvementReduit() || depart === valeur) { courante.current = valeur; setAffichee(valeur); return; }
    const debut = performance.now();
    let image;
    const avancer = (t) => {
      const p = Math.min(1, (t - debut) / 900);
      courante.current = depart + (valeur - depart) * (1 - (1 - p) ** 3); // ralentit en arrivant
      setAffichee(courante.current);
      if (p < 1) image = requestAnimationFrame(avancer);
    };
    image = requestAnimationFrame(avancer);
    return () => cancelAnimationFrame(image);
  }, [valeur]);
  if (valeur == null) return "n/d";
  return <><span aria-hidden="true">{format(affichee)}</span><span className="lecteur">{format(valeur)}</span></>;
}

/** Hausse en vert (flèche vers le haut), baisse en rouge (flèche vers le bas) : jamais la couleur seule. */
export function Variation({ valeur, format, neutre = false }) {
  if (valeur == null) return null;
  const sens = valeur > 0 ? "hausse" : valeur < 0 ? "baisse" : "stable";
  return (
    <span className={`variation ${neutre ? "neutre" : sens}`}>
      <span aria-hidden="true">{sens === "hausse" ? "↑" : sens === "baisse" ? "↓" : "→"}</span> {format(valeur)}
    </span>
  );
}

/** Barres horizontales (comme « Campaign ROAS ») avec un repère vertical (seuil ou référence).
 *  Chaque ligne : { cle, nom, valeur, texte } et en option rang, detail (sous le nom), sousTexte (sous la valeur, une ou plusieurs lignes),
 *  marge (trait fin ± erreur autour du bout de la barre), classe (couleur propre à la ligne). */
export function BarresHorizontales({ lignes, max, repere, etiquetteRepere, classeBarre = "" }) {
  const echelle = Math.max(max, repere ?? 0) * 1.08;
  const position = (v) => (Math.min(echelle, Math.max(0, v)) / echelle) * 100;
  const classee = lignes.some((l) => l.rang != null);
  const Liste = classee ? "ol" : "ul";
  return (
    <>
      {repere != null && <p className="repere-legende mono"><span aria-hidden="true" className="repere-trait" />{etiquetteRepere}</p>}
      <Liste className={`barres-h ${classee ? "avec-rang" : ""}`}>
        {lignes.map((l) => (
          <li key={l.cle}>
            {classee && <span className="barres-h-rang mono">{String(l.rang).padStart(2, "0")}</span>}
            <span className="barres-h-nom">{l.nom}{l.detail && <span className="mono">{l.detail}</span>}</span>
            <span className="barres-h-piste">
              <span className={`barres-h-valeur ${l.classe ?? classeBarre}`} style={{ width: `${position(l.valeur)}%` }} />
              {l.marge != null && (
                <span className="barres-h-marge" style={{ left: `${position(l.valeur - l.marge)}%`,
                  width: `${position(l.valeur + l.marge) - position(l.valeur - l.marge)}%` }} />
              )}
              {repere != null && <span className="barres-h-repere" style={{ left: `${position(repere)}%` }} />}
            </span>
            <span className="barres-h-texte">
              {l.texte}
              {[].concat(l.sousTexte ?? []).map((ligne) => <span key={ligne} className="mono">{ligne}</span>)}
            </span>
          </li>
        ))}
      </Liste>
    </>
  );
}

/** Mini-courbe d'une carte KPI (pas d'axe : la valeur exacte est dans le grand chiffre au-dessus). */
export function Etincelle({ donnees, cle, couleur }) {
  return (
    <div className="etincelle" aria-hidden="true">
      <ResponsiveContainer width="100%" height={48}>
        <AreaChart data={donnees} margin={{ top: 4, right: 4, bottom: 0, left: 4 }}>
          <YAxis hide domain={["dataMin", "dataMax"]} />
          <Area dataKey={cle} type="monotone" stroke={couleur} strokeWidth={2} fill={couleur} fillOpacity={0.14}
                dot={false} isAnimationActive={false} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

/** Avant la première réponse : message d'attente, ou ce qui ne va pas et comment le corriger. */
export function Attente({ erreur }) {
  return (
    <div className="attente" role="status">
      {erreur ? <><strong>Les données ne sont pas encore là.</strong><p>{erreur}</p></> : <p>Chargement des données…</p>}
    </div>
  );
}

/** Erreur survenue alors que des données sont déjà affichées : on les garde et on prévient. */
export function Bandeau({ erreur }) {
  return erreur ? <p className="bandeau" role="alert">Mise à jour impossible : {erreur}</p> : null;
}

export function Choix({ etiquette, valeur, onChange, options, tous }) {
  return (
    <label className="choix">
      <span>{etiquette}</span>
      <select value={valeur} onChange={(e) => onChange(e.target.value)}>
        {tous && <option value="">{tous}</option>}
        {options.map((o) => <option key={o.valeur} value={o.valeur}>{o.texte}</option>)}
      </select>
    </label>
  );
}

export const optionsVilles = (villes) => villes.map((v) => ({ valeur: v, texte: v }));
export const optionsDivisions = (codes) => codes.map((c) => ({ valeur: c, texte: DIVISIONS[c].nom }));

/** Chaque graphique a son équivalent en tableau (lisible au clavier et par un lecteur d'écran). */
export function VueTableau({ titre, colonnes, lignes }) {
  return (
    <details className="vue-tableau">
      <summary>{titre}</summary>
      <div className="defilement">
        <table>
          <thead><tr>{colonnes.map((c) => <th key={c} scope="col">{c}</th>)}</tr></thead>
          <tbody>{lignes.map((l, i) => <tr key={i}>{l.map((v, j) => <td key={j}>{v}</td>)}</tr>)}</tbody>
        </table>
      </div>
    </details>
  );
}

/** Icône d'alerte : la couleur rouge n'est jamais le seul signal (forme + texte). */
export const IconeAlerte = () => (
  <svg className="icone-alerte" viewBox="0 0 16 16" aria-hidden="true">
    <path d="M8 1.5 15 14.5H1z" fill="currentColor" />
    <path d="M8 6v4M8 11.6v.9" stroke="var(--panneau)" strokeWidth="1.6" strokeLinecap="round" />
  </svg>
);
