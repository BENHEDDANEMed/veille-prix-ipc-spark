import { useEffect, useRef, useState } from "react";

// Noms courts des 12 divisions du HCP (les libellés officiels sont longs et en majuscules)
export const DIVISIONS = {
  GR: { nom: "Indice général", article: "l'ensemble des prix" },
  "01": { nom: "Alimentation", article: "l'alimentation" },
  "02": { nom: "Alcool et tabac", article: "l'alcool et le tabac" },
  "03": { nom: "Habillement", article: "l'habillement" },
  "04": { nom: "Logement et énergie", article: "le logement et l'énergie" },
  "05": { nom: "Meubles et ménage", article: "les meubles et l'entretien" },
  "06": { nom: "Santé", article: "la santé" },
  "07": { nom: "Transports", article: "les transports" },
  "08": { nom: "Communications", article: "les communications" },
  "09": { nom: "Loisirs et culture", article: "les loisirs et la culture" },
  "10": { nom: "Enseignement", article: "l'enseignement" },
  "11": { nom: "Restaurants et hôtels", article: "les restaurants et hôtels" },
  "12": { nom: "Biens et services divers", article: "les biens et services divers" },
};

const ESPACE_FINE = " "; // espace fine insécable avant %, comme en typographie française
const formats = {};
const nombre = (decimales) =>
  (formats[decimales] ??= new Intl.NumberFormat("fr-FR", { minimumFractionDigits: decimales, maximumFractionDigits: decimales }));

export function pourcent(valeur, { signe = true, decimales = 1 } = {}) {
  if (valeur == null) return "n/d";
  return `${signe && valeur > 0 ? "+" : ""}${nombre(decimales).format(valeur)}${ESPACE_FINE}%`;
}
export const points = (valeur) => `${nombre(2).format(valeur)} point`;
export const decimal = (valeur, decimales = 2) => nombre(decimales).format(valeur);

const formatMois = new Intl.DateTimeFormat("fr-FR", { month: "long", year: "numeric", timeZone: "UTC" });
export const mois = (iso) => formatMois.format(new Date(iso));
export const majuscule = (texte) => texte.charAt(0).toUpperCase() + texte.slice(1);
export const annee = (iso) => iso.slice(0, 4);

// ["a", "b", "c"] -> "a, b et c"
export function enumerer(liste) {
  return liste.length < 2 ? liste.join("") : `${liste.slice(0, -1).join(", ")} et ${liste.at(-1)}`;
}

/** Vrai pendant 1,2 s après l'apparition, puis à chaque changement de `cle` (ex. la période affichée).
 *  Sert à n'animer les graphiques qu'à ce moment-là : Recharts recrée toutes ses formes à chaque nouveau mois,
 *  une animation permanente ferait repousser les 100 barres toutes les 3 secondes pendant le flux. */
export function useApparition(cle) {
  const [active, setActive] = useState(true);
  useEffect(() => {
    setActive(true);
    const minuterie = setTimeout(() => setActive(false), 1200);
    return () => clearTimeout(minuterie);
  }, [cle]);
  return active;
}

const NOUVEL_ESSAI = 5000; // ms : l'API (Spark) met quelques secondes à démarrer, ou les tables arrivent

/** Appelle l'API ; avec un intervalle, la rappelle en boucle (après chaque réponse, jamais en parallèle).
 *  En cas d'échec, réessaie toutes les 5 s : la page se remplit seule dès que l'API est prête.
 *  Les données précédentes restent affichées pendant le rechargement et en cas d'erreur.
 *  Une réponse identique à la précédente ne change rien à l'écran (les graphiques ne rejouent pas leur animation). */
export function useApi(chemin, intervalle = 0) {
  const [etat, setEtat] = useState({ donnees: null, erreur: null, chargement: true });
  const derniereReponse = useRef(null);
  useEffect(() => {
    let actif = true;
    let minuterie;
    async function charger() {
      if (!intervalle) setEtat((e) => ({ ...e, chargement: true })); // en boucle, pas d'état « chargement » : rien ne bouge à l'écran
      let reussi = false;
      try {
        const reponse = await fetch(`/api${chemin}`);
        const texte = await reponse.text();
        if (!actif) return;
        reussi = reponse.ok;
        const inchangee = reponse.ok && texte === derniereReponse.current;
        derniereReponse.current = texte;
        const corps = inchangee ? null : JSON.parse(texte);
        // Réponse identique : on renvoie le même état, React ne redessine rien (sinon Recharts relance ses animations)
        setEtat((e) => inchangee ? (e.erreur === null && !e.chargement ? e : { ...e, erreur: null, chargement: false }) : reponse.ok
          ? { donnees: corps, erreur: null, chargement: false }
          : { ...e, erreur: corps.detail ?? `Erreur ${reponse.status}`, chargement: false });
      } catch {
        if (actif) setEtat((e) => ({ ...e, erreur: "L'API ne répond pas encore (démarrage de Spark ?). Nouvel essai dans 5 secondes.", chargement: false }));
      }
      const delai = reussi ? intervalle : (intervalle || NOUVEL_ESSAI);
      if (actif && delai) minuterie = setTimeout(charger, delai);
    }
    charger();
    return () => { actif = false; clearTimeout(minuterie); };
  }, [chemin, intervalle]);
  return etat;
}
