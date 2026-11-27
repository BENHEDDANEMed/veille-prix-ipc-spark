import { useState } from "react";
import { Attente, Bandeau, BarresHorizontales, Choix, Compteur, Panneau, Segments, VueTableau, optionsDivisions, optionsVilles } from "../composants.jsx";
import { DIVISIONS, annee, decimal, enumerer, mois, points, pourcent, useApi } from "../outils.js";

const SEUIL_ALERTE = 4; // % sur 12 mois, comme dans le flux

// Deux classements : les plus fortes hausses prévues, ou les hausses inattendues (au-delà de celle du même mois
// l'an dernier : ce que la règle simple ne voit pas, par exemple autre chose que la rentrée scolaire en septembre)
const MODES = [
  { valeur: "prevision", texte: "Plus fortes hausses" },
  { valeur: "surprise", texte: "Hausses inattendues" },
];
const ecartPoints = (v) => `${v > 0 ? "+" : ""}${decimal(v)} pt`;

/** « En septembre 2026, contrôlez d'abord l'enseignement à Tétouan, Rabat et Kénitra. » */
function phraseDecision(risques, moisCible, surprise) {
  const tete = risques.slice(0, 3);
  const villes = [...new Set(tete.map((r) => r.ville))];
  const codes = [...new Set(tete.map((r) => r.code))];
  const cibles = villes.length === 1
    ? `${enumerer(codes.map((c) => DIVISIONS[c].article))} à ${villes[0]}`
    : enumerer(codes.map((c) => `${DIVISIONS[c].article} à ${enumerer(tete.filter((r) => r.code === c).map((r) => r.ville))}`));
  return surprise
    ? `En ${moisCible}, au-delà des hausses habituelles, surveillez ${cibles}.`
    : `En ${moisCible}, contrôlez d'abord ${cibles}.`;
}

/** Même mois, un an plus tôt (date ISO) : septembre 2025 pour une prévision de septembre 2026. */
function unAnAvant(iso) {
  const jour = new Date(iso);
  jour.setUTCFullYear(jour.getUTCFullYear() - 1);
  return jour.toISOString();
}

// Le modèle et les deux règles simples, dans l'ordre fixe des couleurs (turquoise, rose, citron)
const METHODES = [
  { cle: "mae_modele", nom: "Modèle", couleur: "var(--s1)" },
  { cle: "mae_naif_an_dernier", nom: "Même mois l'an dernier", couleur: "var(--s2)" },
  { cle: "mae_naif_mois_courant", nom: "Même hausse que ce mois-ci", couleur: "var(--s3)" },
];
const nomCourt = (modele) => modele.replace("Naïf : ", "Règle : ");
const pourcentEntier = (v) => pourcent(v * 100, { signe: false, decimales: 0 });

export default function Previsions() {
  const [ville, setVille] = useState("");
  const [division, setDivision] = useState("");
  const [tri, setTri] = useState("prevision");
  const parametres = new URLSearchParams({ limite: 10, tri, ...(ville && { ville }), ...(division && { division }) });
  const { donnees, erreur, chargement } = useApi(`/previsions?${parametres}`);
  if (!donnees) return <><h1 className="lecteur">Prévisions</h1><Attente erreur={erreur} /></>;

  const { risques, modeles_test: modeles, fiabilite_par_division: fiabilite, villes, divisions } = donnees;
  const retenu = modeles.find((m) => m.retenu);
  const naifs = modeles.filter((m) => m.modele.startsWith("Naïf")).sort((a, b) => a.mae - b.mae);
  const gain = 1 - retenu.mae / naifs[0].mae; // erreur en moins par rapport à la meilleure règle simple
  const fiabiliteDe = Object.fromEntries(fiabilite.map((f) => [f.code, f]));
  const moisCible = risques.length ? mois(risques[0].date_cible) : "";
  const surprise = tri === "surprise";
  const isoAnDernier = risques.length ? unAnAvant(risques[0].date_cible) : null;
  const anDernier = isoAnDernier ? mois(isoAnDernier) : "l'an dernier";       // « septembre 2025 »
  const anneeAvant = isoAnDernier ? annee(isoAnDernier) : "l'an dernier";     // « 2025 »
  const regleCourant = modeles.find((m) => m.modele.includes("mois courant"));
  // Produits du classement pour lesquels la règle « même mois l'an dernier » prévoit mieux que le modèle
  const moinsFiables = [...new Set(risques.map((r) => r.code))]
    .filter((c) => fiabiliteDe[c].mae_naif_an_dernier < fiabiliteDe[c].mae_modele);
  const tableauComparaison = [retenu, ...naifs];
  const meilleur = { mae: Math.min(...tableauComparaison.map((m) => m.mae)), rmse: Math.min(...tableauComparaison.map((m) => m.rmse)),
    r2: Math.max(...tableauComparaison.map((m) => m.r2)), part_top10: Math.max(...tableauComparaison.map((m) => m.part_top10)) };

  const filtres = (
    <div className="filtres">
      <Segments etiquette="Classement" options={MODES} valeur={tri} onChange={setTri} />
      <Choix etiquette="Ville" valeur={ville} onChange={setVille} options={optionsVilles(villes)} tous="Toutes les villes" />
      <Choix etiquette="Produit" valeur={division} onChange={setDivision}
             options={optionsDivisions(divisions.map((d) => d.code))} tous="Tous les produits" />
      <button type="button" className="bouton" onClick={() => print()}>Imprimer</button>
    </div>
  );

  return (
    <>
      <Bandeau erreur={erreur} />
      <div className="grille">
        <Panneau className="zone-decision" titre="Décision du mois prochain"
                 sousTitre={`Prévision ${retenu.modele} · variation des prix sur le mois · ${moisCible}`}>
          <div className="decision">
            <h1 className="decision-phrase">
              {risques.length ? phraseDecision(risques, moisCible, surprise) : "Aucune hausse prévue pour ce choix de ville et de produit."}
            </h1>
            <dl className="decision-chiffres">
              <div><dt className="mono">{surprise ? `Plus forte hausse au-delà de ${anDernier}` : "Plus forte hausse prévue"}</dt>
                <dd>{risques.length
                  ? <Compteur valeur={surprise ? risques[0].surprise : risques[0].prediction}
                              format={(v) => (surprise ? ecartPoints(v) : pourcent(v, { decimales: 2 }))} />
                  : "n/d"}</dd></div>
              <div><dt className="mono">Erreur moyenne du modèle</dt><dd>± {decimal(retenu.mae)} pt</dd></div>
              <div><dt className="mono">Erreur en moins qu'une règle simple</dt><dd><Compteur valeur={gain * 100} format={(v) => `${decimal(v, 0)} %`} /></dd></div>
            </dl>
          </div>
        </Panneau>

        <Panneau className={`zone-top ${chargement ? "rafraichit" : ""}`}
                 titre={surprise ? `Top ${risques.length} des hausses inattendues en ${moisCible}` : `Top ${risques.length} à surveiller en ${moisCible}`}
                 sousTitre={surprise
                   ? `Ville × produit · hausse prévue moins hausse de ${anDernier} · ± erreur moyenne du modèle`
                   : "Ville × produit · hausse prévue sur le mois · ± erreur moyenne du modèle pour ce produit"}
                 actions={filtres}>
          <h2 className="titre-impression">Liste de contrôle des prix, {moisCible}{surprise ? " (hausses inattendues)" : ""}</h2>
          {risques.length ? (
            <BarresHorizontales max={Math.max(0.5, ...risques.map((r) => (surprise ? r.surprise : r.prediction) + fiabiliteDe[r.code].mae_modele))}
                                lignes={risques.map((r, i) => ({
                                  cle: `${r.ville}-${r.code}`, rang: i + 1, nom: r.ville, detail: DIVISIONS[r.code].nom,
                                  valeur: surprise ? r.surprise : r.prediction, marge: fiabiliteDe[r.code].mae_modele,
                                  texte: surprise ? ecartPoints(r.surprise) : pourcent(r.prediction, { decimales: 2 }),
                                  sousTexte: surprise
                                    ? [`prévue ${pourcent(r.prediction, { decimales: 2 })}`,
                                       `en ${anneeAvant} : ${pourcent(r.variation_meme_mois_an_dernier, { decimales: 2 })}`]
                                    : `${pourcent(r.glissement_annuel)} / 12 mois`,
                                  classe: r.glissement_annuel > SEUIL_ALERTE ? "deja-alerte" : undefined }))} />
          ) : <p className="vide mono">Aucune hausse prévue pour ce filtre.</p>}
          <ul className="legende legende-bas" aria-label="Légende">
            <li><span className="carre s1" />{surprise ? `Écart avec ${anDernier}, en points` : "Hausse prévue"}</li>
            <li><span className="carre alerte" />Déjà en alerte (plus de {SEUIL_ALERTE} % sur 12 mois)</li>
            <li><span className="moustache" />± erreur moyenne du modèle</li>
          </ul>
          {surprise && (
            <p className="mono">
              Classement des hausses plus fortes que l'an dernier ({moisCible} comparé à {anDernier}).
              Seules les hausses prévues sont affichées.
            </p>
          )}
          {moinsFiables.length > 0 && (
            <p className="nuance">
              Pour {enumerer(moinsFiables.map((c) => `${DIVISIONS[c].article} (${decimal(fiabiliteDe[c].mae_naif_an_dernier)} contre ${decimal(fiabiliteDe[c].mae_modele)} point d'erreur)`))},
              la hausse du même mois l'an dernier prévoit mieux que le modèle : vérifiez-la aussi.
            </p>
          )}
          <VueTableau titre="Tableau des valeurs"
                      colonnes={["Rang", "Ville", "Produit", "Hausse prévue", `Hausse en ${anDernier}`, "Écart", "Erreur moyenne", "Sur 12 mois"]}
                      lignes={risques.map((r, i) => [i + 1, r.ville, DIVISIONS[r.code].nom, pourcent(r.prediction, { decimales: 2 }),
                                                     pourcent(r.variation_meme_mois_an_dernier, { decimales: 2 }), ecartPoints(r.surprise),
                                                     `± ${decimal(fiabiliteDe[r.code].mae_modele)} pt`, pourcent(r.glissement_annuel)])} />
        </Panneau>

        <Panneau className="zone-metriques" titre="Métriques du modèle"
                 sousTitre="Test · prévisions de févr. 2025 à août 2026 · jamais vues à l'entraînement">
          <div className="kpi-ligne">
            <p className="grand-chiffre"><Compteur valeur={retenu.mae} format={(v) => decimal(v, 3)} /></p>
            <p className="kpi-comparaison"><span>MAE en points · {decimal(naifs[0].mae, 3)} pour la meilleure règle simple</span></p>
          </div>
          <BarresHorizontales max={Math.max(...modeles.map((m) => m.mae))}
                              lignes={modeles.map((m) => ({ cle: m.modele, nom: nomCourt(m.modele), valeur: m.mae, texte: decimal(m.mae, 3),
                                                            classe: m.retenu ? undefined : m.modele.startsWith("Naïf") ? "reference" : "autre" }))} />
          <p className="mono">Erreur moyenne (MAE) : plus la barre est courte, mieux c'est.</p>
          <div className="defilement">
            <table className="comparaison">
              <thead><tr><th scope="col" /><th scope="col">MAE</th><th scope="col">RMSE</th><th scope="col">R²</th><th scope="col">Top 10</th></tr></thead>
              <tbody>
                {tableauComparaison.map((m) => (
                  <tr key={m.modele} className={m.retenu ? "retenu" : undefined}>
                    <th scope="row">{nomCourt(m.modele)}</th>
                    {[["mae", decimal(m.mae, 3)], ["rmse", decimal(m.rmse, 3)], ["r2", decimal(m.r2, 2)], ["part_top10", pourcentEntier(m.part_top10)]].map(([cle, texte]) => (
                      <td key={cle} className={m[cle] === meilleur[cle] ? "meilleur" : undefined}>{texte}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="mono">Top 10 : part des 10 hausses prévues qui sont parmi les 10 plus fortes hausses réelles (hasard : 5 %). En gras : le meilleur.</p>

          <div className="bloc-metrique">
            <h3>Hausses inattendues retrouvées</h3>
            <p className="mono">Part des 10 plus fortes surprises prévues parmi les 10 plus fortes surprises réelles, chaque mois</p>
            <BarresHorizontales max={retenu.part_top10_surprise * 100}
                                lignes={[
                                  { cle: "modele", nom: retenu.modele, valeur: retenu.part_top10_surprise * 100, texte: pourcentEntier(retenu.part_top10_surprise) },
                                  { cle: "regle", nom: "Règle : mois suivant = mois courant", valeur: regleCourant.part_top10_surprise * 100,
                                    texte: pourcentEntier(regleCourant.part_top10_surprise), classe: "reference" },
                                  { cle: "hasard", nom: "Hasard (10 sur 216)", valeur: (10 / 216) * 100, texte: pourcentEntier(10 / 216), classe: "autre" },
                                ]} />
            <p className="nuance">
              Le modèle fait mieux que la règle simple ({pourcentEntier(retenu.part_top10_surprise)} contre {pourcentEntier(regleCourant.part_top10_surprise)}),
              mais le gain est modeste. Une partie de ces surprises est mécanique : après une chute exceptionnelle l'an dernier,
              presque toute valeur la dépasse.
            </p>
          </div>
        </Panneau>

        <Fiabilite fiabilite={fiabilite} />
      </div>
    </>
  );
}

/** Graphique en points : erreur moyenne de chaque méthode, par produit (plus à gauche = meilleur). */
function Fiabilite({ fiabilite }) {
  const max = Math.max(...fiabilite.flatMap((f) => METHODES.map((s) => f[s.cle])));
  const lignes = [...fiabilite.filter((f) => f.code !== "GR"), ...fiabilite.filter((f) => f.code === "GR")];
  const meilleure = (f) => METHODES.reduce((a, b) => (f[b.cle] < f[a.cle] ? b : a));
  const gagnes = lignes.filter((f) => meilleure(f).cle === "mae_modele").length;

  return (
    <Panneau className="zone-fiabilite" titre="Le modèle fait-il mieux qu'une règle simple ?"
             sousTitre={`Erreur moyenne par produit · le modèle gagne pour ${gagnes} produits sur ${lignes.length}`}
             actions={<ul className="legende" aria-label="Légende">
               {METHODES.map((s) => <li key={s.cle}><span className="point" style={{ background: s.couleur }} />{s.nom}</li>)}
             </ul>}>
      <div className="points-graphique" role="img"
           aria-label="Erreur moyenne par produit pour le modèle et les deux règles simples ; détail dans le tableau">
        {lignes.map((f) => (
          <div className="points-ligne" key={f.code}>
            <span className="points-nom">{DIVISIONS[f.code].nom}</span>
            <div className="points-piste">
              {METHODES.map((s) => (
                <span key={s.cle} className="point" style={{ left: `${(f[s.cle] / max) * 100}%`, background: s.couleur }}
                      title={`${s.nom} : ${points(f[s.cle])}`} />
              ))}
            </div>
            <span className="points-gagnant mono">{meilleure(f).nom}</span>
          </div>
        ))}
        <div className="points-axe mono"><span>0</span><span>{decimal(max / 2)} pt</span><span>{decimal(max)} pt</span></div>
      </div>
      <p className="mono">Pour les hausses fixées à date connue (tabac, communications, rentrée scolaire), reprendre la hausse de l'an dernier reste plus sûr.</p>
      <VueTableau titre="Tableau des valeurs" colonnes={["Produit", ...METHODES.map((s) => s.nom)]}
                  lignes={lignes.map((f) => [DIVISIONS[f.code].nom, ...METHODES.map((s) => decimal(f[s.cle], 3))])} />
    </Panneau>
  );
}
