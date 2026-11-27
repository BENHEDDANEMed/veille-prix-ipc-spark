import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Attente, Bandeau, Compteur, Panneau, VueTableau } from "../composants.jsx";
import { DIVISIONS, decimal, pourcent, useApi } from "../outils.js";

// Couleur d'un groupe : ordre fixe des séries (le groupe 1 est toujours turquoise, le groupe 2 toujours rose…)
const COULEURS_GROUPES = ["var(--s1)", "var(--s2)", "var(--s3)", "var(--s4)", "var(--s5)", "var(--gris)"];
const nomGroupe = (g) => `Groupe ${g + 1}`;
const moyenne = (valeurs) => valeurs.reduce((a, b) => a + b, 0) / valeurs.length;
const axe = { stroke: "var(--axe)", tick: { fill: "var(--texte-3)", fontSize: 11, fontFamily: "var(--mono)" }, tickLine: false };

// Carte de chaleur : échelle divergente en 7 classes de 0,5 point (bleu = moins de hausse que la moyenne des villes,
// orange = plus), milieu neutre
const PAS = 0.5;
const CLASSES = [-3, -2, -1, 0, 1, 2, 3];
const classe = (ecart) => Math.max(-3, Math.min(3, Math.round(ecart / PAS)));
const couleur = (c) => c === 0 ? "var(--milieu)"
  : `color-mix(in oklab, var(${c > 0 ? "--pole-haut" : "--pole-bas"}) ${(Math.abs(c) / 3) * 100}%, var(--milieu))`;
const etiquetteClasse = (c) => c === 0 ? "moyenne"
  : `${c > 0 ? "+" : "−"}${decimal(Math.abs(c) * PAS, 1)}${Math.abs(c) === 3 ? " et +" : ""}`;

function InfoBulle({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="info-bulle">
      <strong>{label}</strong>
      {payload.map((p) => <span key={p.dataKey}>{p.name} : {pourcent(p.value, { decimales: 2 })} par an</span>)}
    </div>
  );
}

export default function Groupes() {
  const { donnees, erreur } = useApi("/groupes");
  if (!donnees) return <><h1 className="lecteur">Groupes de villes</h1><Attente erreur={erreur} /></>;

  const { villes, profils, silhouette } = donnees;
  const colonnes = Object.keys(villes[0]).filter((c) => /^d\d\d$/.test(c));
  const nomDivision = (colonne) => DIVISIONS[colonne.slice(1)].nom;
  const moyennes = Object.fromEntries(colonnes.map((c) => [c, moyenne(villes.map((v) => v[c]))]));
  const retenu = silhouette.find((s) => s.retenu);
  const nettete = retenu.silhouette < 0.25 ? "qui se recouvrent largement" : retenu.silhouette < 0.5 ? "assez distincts" : "bien distincts";
  const largeurGroupe = Math.max(4, Math.floor(12 / profils.length)); // en colonnes de la grille

  // Ce qui distingue chaque groupe : les 3 produits où il dépasse le plus la moyenne des 18 villes
  const signatures = profils.map((p) => ({
    ...p,
    villes: villes.filter((v) => v.groupe === p.groupe).map((v) => v.ville),
    marques: colonnes.map((c) => ({ colonne: c, ecart: p[c] - moyennes[c] }))
      .sort((a, b) => b.ecart - a.ecart).slice(0, 3).filter((m) => m.ecart > 0),
  }));
  // Barres groupées : une ligne par produit, une barre par groupe
  const profilParProduit = colonnes.map((c) => ({
    produit: nomDivision(c), ...Object.fromEntries(profils.map((p) => [`g${p.groupe}`, p[c]])),
  }));

  return (
    <>
      <h1 className="lecteur">Les 18 villes forment {profils.length} groupes {nettete}</h1>
      <Bandeau erreur={erreur} />
      <div className="grille">
        {signatures.map((g) => (
          <Panneau key={g.groupe} style={{ gridColumn: `span ${largeurGroupe}` }} titre={<><span className="carre" style={{ background: COULEURS_GROUPES[g.groupe] }} />{nomGroupe(g.groupe)}</>}
                   sousTitre={`KMeans · ${g.nb_villes} villes · hausse annuelle moyenne depuis 2018`}>
            <ul className="puces" aria-label={`Villes du ${nomGroupe(g.groupe).toLowerCase()}`}>
              {g.villes.map((v) => <li key={v}>{v}</li>)}
            </ul>
            <div>
              <p className="mono">Prix qui montent plus vite qu'ailleurs</p>
              <dl className="signature">
                {g.marques.map((m) => (
                  <div key={m.colonne}>
                    <dt>{nomDivision(m.colonne)}</dt>
                    <dd><span className="variation neutre"><span aria-hidden="true">↑</span> {decimal(m.ecart)} pt par an</span>
                      <span className="mono"> au-dessus de la moyenne des villes</span></dd>
                  </div>
                ))}
              </dl>
            </div>
          </Panneau>
        ))}

        <Panneau className="zone-profil" titre="Profil d'inflation moyen de chaque groupe"
                 sousTitre="Hausse annuelle moyenne par produit depuis 2018 · moyenne des villes du groupe"
                 actions={<ul className="legende" aria-label="Légende">
                   {profils.map((p) => <li key={p.groupe}><span className="carre" style={{ background: COULEURS_GROUPES[p.groupe] }} />{nomGroupe(p.groupe)}</li>)}
                 </ul>}>
          <div className="graphique">
            <ResponsiveContainer width="100%" height={colonnes.length * (11 * profils.length + 14) + 36}>
              <BarChart data={profilParProduit} layout="vertical" margin={{ top: 4, right: 16, bottom: 0, left: 0 }} barGap={2} barCategoryGap={6}>
                <CartesianGrid horizontal={false} stroke="var(--grille)" />
                <XAxis type="number" {...axe} tickFormatter={(v) => `${v} %`} />
                <YAxis type="category" dataKey="produit" width={150} {...axe} axisLine={false} tick={{ ...axe.tick, fontFamily: "var(--police)", fontSize: 12, fill: "var(--texte-2)" }} />
                <Tooltip cursor={{ fill: "var(--survol)" }} content={<InfoBulle />} />
                {profils.map((p) => (
                  <Bar key={p.groupe} dataKey={`g${p.groupe}`} name={nomGroupe(p.groupe)} fill={COULEURS_GROUPES[p.groupe]}
                       radius={[0, 3, 3, 0]} barSize={9} animationDuration={800} />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>
          <VueTableau titre="Tableau des valeurs" colonnes={["Produit", ...profils.map((p) => nomGroupe(p.groupe))]}
                      lignes={colonnes.map((c) => [nomDivision(c), ...profils.map((p) => pourcent(p[c], { decimales: 2 }))])} />
        </Panneau>

        <Panneau className="zone-silhouette" titre="Choix du nombre de groupes"
                 sousTitre="KMeans · k de 2 à 6 · 10 départs par k · score silhouette">
          <div className="kpi-ligne">
            <p className="grand-chiffre">k = <Compteur valeur={retenu.k} /></p>
            <p className="kpi-comparaison"><span>silhouette {decimal(retenu.silhouette, 3)}, la meilleure</span></p>
          </div>
          <div className="graphique">
            <ResponsiveContainer width="100%" height={230}>
              <BarChart data={silhouette} margin={{ top: 22, right: 8, bottom: 0, left: 0 }} barCategoryGap="22%">
                <CartesianGrid vertical={false} stroke="var(--grille)" />
                <XAxis dataKey="k" {...axe} tickFormatter={(k) => `k = ${k}`} />
                <YAxis {...axe} axisLine={false} width={40} tickFormatter={(v) => decimal(v, 2)} />
                <Bar dataKey="silhouette" radius={[3, 3, 0, 0]} animationDuration={800}
                     label={{ position: "top", fill: "var(--texte)", fontSize: 12, fontFamily: "var(--mono)", formatter: (v) => decimal(v, 2) }}>
                  {silhouette.map((s) => <Cell key={s.k} fill={s.retenu ? "var(--accent)" : "var(--gris)"} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
          <p className="mono">
            Score de 0 à 1 : {decimal(retenu.silhouette, 2)} veut dire des groupes {nettete}. À lire comme une tendance,
            pas comme une frontière nette (seulement 18 villes).
          </p>
          <VueTableau titre="Tableau des valeurs" colonnes={["Nombre de groupes", "Silhouette", ""]}
                      lignes={silhouette.map((s) => [s.k, decimal(s.silhouette, 3), s.retenu ? "retenu" : ""])} />
        </Panneau>

        <Panneau className="zone-carte" titre="Écart de chaque ville à la moyenne des 18 villes"
                 sousTitre="Hausse annuelle moyenne depuis 2018 · en points de pourcentage par an · par produit"
                 actions={<ul className="legende-classes" aria-label="Légende des couleurs">
                   {CLASSES.map((c) => <li key={c}><span style={{ background: couleur(c) }} />{etiquetteClasse(c)}</li>)}
                 </ul>}>
          <div className="carte-defilement">
            <table className="carte">
              <thead>
                <tr>
                  <th scope="col">Ville</th>
                  {colonnes.map((c) => <th key={c} scope="col">{nomDivision(c)}</th>)}
                </tr>
              </thead>
              {profils.map((p) => (
                <tbody key={p.groupe}>
                  <tr className="carte-groupe">
                    <th colSpan={colonnes.length + 1} scope="rowgroup">
                      <span className="carre" style={{ background: COULEURS_GROUPES[p.groupe] }} />{nomGroupe(p.groupe)}
                    </th>
                  </tr>
                  {villes.filter((v) => v.groupe === p.groupe).map((v) => (
                    <tr key={v.ville}>
                      <th scope="row">{v.ville}</th>
                      {colonnes.map((c) => {
                        const ecart = v[c] - moyennes[c];
                        const texte = `${v.ville}, ${nomDivision(c).toLowerCase()} : ${pourcent(v[c], { decimales: 2 })} par an, `
                          + `${decimal(Math.abs(ecart))} point ${ecart >= 0 ? "au-dessus" : "au-dessous"} de la moyenne`;
                        return <td key={c} style={{ background: couleur(classe(ecart)) }} title={texte} aria-label={texte} tabIndex={0} />;
                      })}
                    </tr>
                  ))}
                </tbody>
              ))}
            </table>
          </div>
          <VueTableau titre="Tableau des valeurs" colonnes={["Ville", "Groupe", ...colonnes.map(nomDivision)]}
                      lignes={villes.map((v) => [v.ville, nomGroupe(v.groupe), ...colonnes.map((c) => pourcent(v[c], { decimales: 2 }))])} />
        </Panneau>
      </div>
    </>
  );
}
