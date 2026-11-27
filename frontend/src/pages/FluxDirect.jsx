import { useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, Pie, PieChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Attente, Bandeau, BarresHorizontales, Compteur, Etincelle, Panneau, Segments, Variation, VueTableau } from "../composants.jsx";
import { DIVISIONS, annee, decimal, majuscule, mois, pourcent, useApi, useApparition } from "../outils.js";

const RAFRAICHISSEMENT = 4000; // ms : le producteur publie un mois toutes les quelques secondes
const ACTIF_SI_MOINS_DE = 15;  // s : le flux « reçoit » si le dernier lot date de moins de 15 s

// Sélecteur de période de l'histogramme. Live = les 6 derniers mois reçus, la fenêtre avance avec le flux.
const PERIODES = [
  { valeur: 6, texte: "Live" },
  { valeur: 12, texte: "12 mois" },
  { valeur: 60, texte: "5 ans" },
  { valeur: 0, texte: "Tout" },
];
const NB_VILLES_ANNEAU = 4; // 4 villes + « autres villes » : au-delà de 5 parts un anneau ne se lit plus
const COULEURS_ANNEAU = ["var(--s1)", "var(--s2)", "var(--s3)", "var(--s4)", "var(--gris)"];
const NB_ALERTES_VISIBLES = 8;

const moisCourt = new Intl.DateTimeFormat("fr-FR", { month: "short", year: "2-digit", timeZone: "UTC" });
const formatMoisCourt = (iso) => moisCourt.format(new Date(iso));
const nomZone = (ville) => (ville === "National" ? "Maroc" : ville);
const secondes = (s) => `${decimal(s, s < 10 ? 2 : 1)} s`;
const axe = { stroke: "var(--axe)", tick: { fill: "var(--texte-3)", fontSize: 11, fontFamily: "var(--mono)" }, tickLine: false };

function InfoBulle({ active, payload, seuil }) {
  if (!active || !payload?.length) return null;
  const l = payload[0].payload;
  return (
    <div className="info-bulle">
      <strong>{majuscule(mois(l.date))}</strong>
      <span>{pourcent(l.glissement_national, { decimales: 2 })} sur 12 mois</span>
      {l.glissement_national > seuil && <span className="texte-alerte">au-dessus du seuil d'alerte</span>}
    </div>
  );
}

/** Jauge 3/4 de cercle : mois traités par Spark sur mois préparés par la collecte. */
function Jauge({ part, valeur, total }) {
  const arc = "M 22 98 A 56 56 0 1 1 118 98"; // 270° ouvert vers le bas
  return (
    <div className="jauge">
      <svg viewBox="0 0 140 120" role="img" aria-label={`${valeur} mois traités sur ${total}`}>
        <path d={arc} className="jauge-piste" pathLength="100" />
        <path d={arc} className="jauge-valeur" pathLength="100" strokeDasharray={`${part * 100} 100`} />
      </svg>
      <div className="jauge-texte">
        <strong><Compteur valeur={valeur} /><span className="jauge-total">/{total}</span></strong>
        <span className="mono">mois traités</span>
      </div>
    </div>
  );
}

export default function FluxDirect({ enDirect }) {
  const [periode, setPeriode] = useState(6);
  const { donnees, erreur } = useApi("/flux", enDirect ? RAFRAICHISSEMENT : 0);
  const barresApparaissent = useApparition(`${periode}-${Boolean(donnees)}`); // à l'arrivée des données et au changement de période
  const anneauApparait = useApparition(Boolean(donnees));
  if (!donnees) return <><h1 className="lecteur">Flux en direct</h1><Attente erreur={erreur} /></>;

  const { dernier_mois, nb_mois, nb_series, seuil_alerte, evolution, alertes_par_mois, alertes, journal, mois_publiables } = donnees;

  // Inflation nationale : glissement sur 12 mois (vide les 12 premiers mois, donc filtré)
  const connus = evolution.filter((l) => l.glissement_national != null);
  const dernier = evolution.at(-1);
  const avantDernier = evolution.at(-2);
  const ecart = dernier?.glissement_national != null && avantDernier?.glissement_national != null
    ? dernier.glissement_national - avantDernier.glissement_national : null;
  const histogramme = periode ? connus.slice(-periode) : connus;
  const parAnnee = histogramme.length > 24;

  // Alertes du dernier mois par ville (le Maroc entier n'est pas une ville : exclu de l'anneau).
  // Les couleurs suivent le rang du mois : la légende nomme chaque part.
  const parVille = Object.entries(Object.groupBy(alertes.filter((a) => a.ville !== "National"), (a) => a.ville))
    .map(([ville, liste]) => ({ nom: ville, n: liste.length }))
    .sort((a, b) => b.n - a.n || a.nom.localeCompare(b.nom));
  const totalVilles = parVille.reduce((s, v) => s + v.n, 0);
  const autres = parVille.slice(NB_VILLES_ANNEAU).reduce((s, v) => s + v.n, 0);
  const parts = [...parVille.slice(0, NB_VILLES_ANNEAU), ...(autres ? [{ nom: `${parVille.length - NB_VILLES_ANNEAU} autres villes`, n: autres }] : [])];

  // Indicateurs et santé du pipeline, à partir du journal des micro-batches (le plus récent en premier)
  const nbAlertes = alertes_par_mois.at(-1).nb_alertes;
  const ecartAlertes = alertes_par_mois.length > 1 ? nbAlertes - alertes_par_mois.at(-2).nb_alertes : null;
  const lignesIngerees = journal.reduce((s, l) => s + l.lignes, 0);
  let cumul = 0;
  const lignesCumulees = [...journal].reverse().map((l) => ({ lignes: (cumul += l.lignes) }));
  const dernierLot = journal[0];
  const ilYa = dernierLot ? Math.max(0, Date.now() / 1000 - dernierLot.fin) : null;
  const fluxActif = ilYa != null && ilYa < ACTIF_SI_MOINS_DE;
  const cadence = journal.length > 2 ? (journal[0].fin - journal[Math.min(5, journal.length - 1)].fin) / Math.min(5, journal.length - 1) : null;
  const dureeMoyenne = journal.length ? journal.reduce((s, l) => s + l.duree_s, 0) / journal.length : null;
  const statut = erreur ? { classe: "panne", texte: "Interrompu" } : enDirect ? { classe: "ok", texte: "Nominal" } : { classe: "pause", texte: "En pause" };

  return (
    <>
      <h1 className="lecteur">Flux en direct : {mois(dernier_mois)}</h1>
      <Bandeau erreur={erreur} />
      <div className="grille">
        <Panneau className="zone-inflation" titre="Inflation nationale"
                 sousTitre={`Dernier mois reçu · glissement sur 12 mois · ${mois(dernier_mois)}`}
                 actions={<Segments etiquette="Période" options={PERIODES} valeur={periode} onChange={setPeriode} />}>
          <div className="kpi-ligne">
            <p className="grand-chiffre"><Compteur valeur={dernier?.glissement_national} format={(v) => pourcent(v, { decimales: 2 })} /></p>
            {ecart != null && (
              <p className="kpi-comparaison">
                <Variation valeur={ecart} format={(v) => `${decimal(Math.abs(v), 2)} pt`} />
                <span>vs {mois(avantDernier.date)}</span>
              </p>
            )}
            <ul className="legende" aria-label="Légende">
              <li><span className="carre s1" />Glissement sur 12 mois</li>
              <li><span className="carre alerte" />Mois en alerte</li>
              <li><span className="tiret" />Seuil {decimal(seuil_alerte, 0)} %</li>
            </ul>
          </div>
          <div className={`graphique ${barresApparaissent ? "apparition" : ""}`}>
            {histogramme.length === 0 ? (
              <p className="vide mono">Le glissement sur 12 mois apparaît au 13e mois reçu ({nb_mois} mois pour l'instant).</p>
            ) : (
              <ResponsiveContainer width="100%" height={220}>
                <BarChart key={periode} data={histogramme} margin={{ top: 22, right: 8, bottom: 0, left: 0 }} barCategoryGap={histogramme.length > 60 ? 1 : "18%"}>
                  <CartesianGrid vertical={false} stroke="var(--grille)" />
                  <XAxis dataKey="date" {...axe} interval={parAnnee ? 0 : "preserveStartEnd"} minTickGap={12}
                         ticks={parAnnee ? histogramme.map((l) => l.date).filter((d) => d.endsWith("-01-01")) : undefined}
                         tickFormatter={parAnnee ? annee : formatMoisCourt} />
                  <YAxis {...axe} axisLine={false} width={44} tickFormatter={(v) => `${v} %`}
                         padding={{ bottom: periode === 6 ? 20 : 0 }} /> {/* place pour l'étiquette sous une barre négative */}
                  <ReferenceLine y={0} stroke="var(--axe)" />
                  <ReferenceLine y={seuil_alerte} stroke="var(--s2)" strokeDasharray="4 4" />
                  <Tooltip cursor={{ fill: "var(--survol)" }} content={<InfoBulle seuil={seuil_alerte} />} />
                  <Bar dataKey="glissement_national" radius={[3, 3, 0, 0]} isAnimationActive={false}
                       label={periode === 6 && { position: "top", fill: "var(--texte)", formatter: (v) => pourcent(v, { decimales: 2 }) }}>
                    {histogramme.map((l) => <Cell key={l.date} className={l.glissement_national < 0 ? "barre-negative" : undefined}
                                                      fill={l.glissement_national > seuil_alerte ? "var(--alerte)" : "var(--s1)"} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>
          <VueTableau titre="Tableau des valeurs" colonnes={["Mois", "Glissement sur 12 mois", "Variation sur le mois"]}
                      lignes={[...histogramme].reverse().map((l) => [majuscule(mois(l.date)), pourcent(l.glissement_national, { decimales: 2 }), pourcent(l.variation_ville, { decimales: 2 })])} />
        </Panneau>

        <Panneau className="zone-villes" titre="Alertes par ville" sousTitre={`Part des séries en alerte · ${mois(dernier_mois)}`}>
          <div className="anneau">
            <div className={`anneau-graphique ${anneauApparait ? "apparition" : ""}`}>
              <ResponsiveContainer width="100%" height={180}>
                <PieChart>
                  <Pie data={totalVilles ? parts : [{ nom: "aucune", n: 1 }]} dataKey="n" nameKey="nom" innerRadius="72%" outerRadius="100%"
                       paddingAngle={totalVilles ? 2 : 0} stroke="none" startAngle={90} endAngle={-270} isAnimationActive={false}>
                    {(totalVilles ? parts : [0]).map((_, i) => <Cell key={i} fill={totalVilles ? COULEURS_ANNEAU[i] : "var(--piste)"} />)}
                  </Pie>
                </PieChart>
              </ResponsiveContainer>
              <p className="anneau-centre"><strong><Compteur valeur={totalVilles} /></strong><span className="mono">alertes<br />en ville</span></p>
            </div>
            {totalVilles ? (
              <ul className="anneau-legende">
                {parts.map((p, i) => (
                  <li key={p.nom}>
                    <span className="carre" style={{ background: COULEURS_ANNEAU[i] }} />
                    <span className="anneau-nom">{p.nom}</span>
                    <span className="mono">{decimal((p.n / totalVilles) * 100, 1)} %</span>
                    <span className="mono discret">{p.n}</span>
                  </li>
                ))}
              </ul>
            ) : <p className="vide mono">Aucune ville au-dessus de {decimal(seuil_alerte, 0)} % ce mois-ci.</p>}
          </div>
          <VueTableau titre="Tableau des valeurs" colonnes={["Ville", "Séries en alerte"]}
                      lignes={parVille.map((v) => [v.nom, v.n])} />
        </Panneau>

        <Panneau className="zone-kpi" titre="Indicateurs clés" sousTitre={`Mois par mois · ${nb_series} séries suivies`}>
          <div className="cartes-kpi">
            <div className="carte-kpi">
              <p className="mono">Dernier mois reçu</p>
              <p className="kpi-valeur">{mois(dernier_mois)}</p>
              <Variation valeur={dernier?.variation_ville} format={(v) => `${pourcent(Math.abs(v), { signe: false, decimales: 2 })} sur le mois`} />
              <Etincelle donnees={evolution.slice(-12)} cle="variation_ville" couleur="var(--s1)" />
            </div>
            <div className="carte-kpi">
              <p className="mono">Séries en alerte</p>
              <p className="kpi-valeur"><Compteur valeur={nbAlertes} /></p>
              <Variation valeur={ecartAlertes} neutre format={(v) => `${Math.abs(v)} vs mois précédent`} />
              <Etincelle donnees={alertes_par_mois.slice(-12)} cle="nb_alertes" couleur="var(--alerte)" />
            </div>
            <div className="carte-kpi">
              <p className="mono">Lignes ingérées</p>
              <p className="kpi-valeur"><Compteur valeur={lignesIngerees} /></p>
              <Variation valeur={dernierLot?.lignes} neutre format={(v) => `${decimal(v, 0)} au dernier lot`} />
              <Etincelle donnees={lignesCumulees} cle="lignes" couleur="var(--s3)" />
            </div>
          </div>
        </Panneau>

        <Panneau className="zone-sante" titre="Santé du pipeline Spark"
                 sousTitre={fluxActif ? "Structured Streaming · reçoit des mois" : "Structured Streaming · à jour, en attente"}
                 actions={<span className={`statut ${statut.classe}`}><span aria-hidden="true">{statut.classe === "panne" ? "✕" : statut.classe === "pause" ? "❚❚" : "✓"}</span>{statut.texte}</span>}>
          <div className="sante">
            <Jauge part={mois_publiables ? nb_mois / mois_publiables : 0} valeur={nb_mois} total={mois_publiables} />
            <dl className="sante-chiffres">
              <div><dt className="mono">Micro-batches</dt><dd><Compteur valeur={journal.length} /></dd></div>
              <div><dt className="mono">Lignes ingérées</dt><dd><Compteur valeur={lignesIngerees} /></dd></div>
              <div><dt className="mono">Durée du dernier lot</dt><dd>{dernierLot ? secondes(dernierLot.duree_s) : "n/d"}</dd></div>
              <div><dt className="mono">Moyenne par lot</dt><dd>{dureeMoyenne != null ? secondes(dureeMoyenne) : "n/d"}</dd></div>
            </dl>
          </div>
        </Panneau>

        <Panneau className="zone-alertes" titre={`Séries en alerte en ${mois(dernier_mois)}`}
                 sousTitre={`Glissement sur 12 mois · ${Math.min(NB_ALERTES_VISIBLES, alertes.length)} plus fortes sur ${alertes.length}`}>
          {alertes.length ? (
            <BarresHorizontales classeBarre="alerte" repere={seuil_alerte} etiquetteRepere={`Seuil d'alerte ${decimal(seuil_alerte, 0)} %`}
                                max={alertes[0].glissement_annuel}
                                lignes={alertes.slice(0, NB_ALERTES_VISIBLES).map((a) => ({
                                  cle: `${a.ville}-${a.code}`, nom: `${nomZone(a.ville)} · ${DIVISIONS[a.code].nom}`,
                                  valeur: a.glissement_annuel, texte: pourcent(a.glissement_annuel) }))} />
          ) : <p className="vide mono">Aucune série au-dessus de {decimal(seuil_alerte, 0)} % ce mois-ci.</p>}
          <VueTableau titre={`Les ${alertes.length} alertes`} colonnes={["Ville", "Produit", "Sur 12 mois", "Sur le mois"]}
                      lignes={alertes.map((a) => [nomZone(a.ville), DIVISIONS[a.code].nom, pourcent(a.glissement_annuel), pourcent(a.variation_mensuelle)])} />
        </Panneau>

        <Panneau className="zone-journal" titre="Journal du flux"
                 sousTitre={fluxActif && cadence ? `Streaming · 1 mois toutes les ${decimal(cadence, 0)} s` : `${journal.length} micro-batches · 1 fichier CSV par lot`}>
          <ol className="journal" aria-live="polite">
            {journal.map((l) => (
              <li key={l.numero_lot}>
                <strong>{majuscule(mois(l.mois))}</strong>
                <span className="mono discret">{decimal(l.lignes, 0)} lignes · {l.alertes} alertes</span>
                <span className="mono discret journal-lot">lot {l.numero_lot} · {secondes(l.duree_s)}</span>
              </li>
            ))}
          </ol>
        </Panneau>
      </div>
    </>
  );
}
