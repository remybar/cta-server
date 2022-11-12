// From where CTA items come from ('imx' for IMX blockchain or 'local' for test)
const ASSETS_SOURCE = process.env.ASSETS_SOURCE || "imx";

// Address of the CTA collection on the IMX blockchain
const CTA_COLLECTION_ADDRESS =
  process.env.CTA_COLLECTION_ADDRESS ||
  "0xa04bcac09a3ca810796c9e3deee8fdc8c9807166";

// Number of items to get per page with the IMX API
const CTA_PAGE_SIZE = process.env.CTA_PAGE_SIZE || 1000;

// Maximum number of assets retrieved per update to avoid reaching the API limit
const MAX_ASSETS_PER_UPDATE = process.env.MAX_ASSETS_PER_UPDATE || 2000;

// rarities for which we know the total supply
const KNOWN_SUPPLY_RARITIES = [
  "MYTHIC",
  "ULTRA_RARE",
  "SPECIAL_RARE",
  "RARE",
  "UNCOMMON",
  "COMMON",
];

// Standard card supply
const CARD_SUPPLY = {
  normal: {
    MYTHIC: {
      STANDARD: 20000,
      ALTERNATIVE: 10000,
      ALTERNATIVE_COMBO: 5000,
    },
    ULTRA_RARE: {
      STANDARD: 60000,
      ALTERNATIVE: 30000,
      ALTERNATIVE_COMBO: 15000,
    },
    SPECIAL_RARE: {
      STANDARD: 100000,
      ALTERNATIVE: 40000,
      ALTERNATIVE_COMBO: 20000,
    },
    RARE: {
      STANDARD: 150000,
      ALTERNATIVE: 50000,
      ALTERNATIVE_COMBO: 25000,
    },
    UNCOMMON: {
      STANDARD: 400000,
      ALTERNATIVE: 100000,
      ALTERNATIVE_COMBO: 50000,
    },
    COMMON: {
      STANDARD: 800000,
      ALTERNATIVE: 300000,
      ALTERNATIVE_COMBO: 150000,
    },
  },
  foil: {
    MYTHIC: {
      STANDARD: 4000,
      ALTERNATIVE: 2000,
      ALTERNATIVE_COMBO: 1000,
    },
    ULTRA_RARE: {
      STANDARD: 12000,
      ALTERNATIVE: 6000,
      ALTERNATIVE_COMBO: 3000,
    },
    SPECIAL_RARE: {
      STANDARD: 20000,
      ALTERNATIVE: 8000,
      ALTERNATIVE_COMBO: 4000,
    },
    RARE: {
      STANDARD: 30000,
      ALTERNATIVE: 10000,
      ALTERNATIVE_COMBO: 5000,
    },
    UNCOMMON: {
      STANDARD: 80000,
      ALTERNATIVE: 20000,
      ALTERNATIVE_COMBO: 10000,
    },
    COMMON: {
      STANDARD: 160000,
      ALTERNATIVE: 60000,
      ALTERNATIVE_COMBO: 30000,
    },
  },
};

const percent = (v) => Math.round((v + Number.EPSILON) * 10000) / 100;

const getSupply = (card) => {
  // ignore for special rarities such as 'exclusive'
  if (!KNOWN_SUPPLY_RARITIES.includes(card.rarity)) return null;

  let total_supply_foil, total_supply_non_foil;
  if (card.card_name === "Hannibal & Honora") {
    total_supply_non_foil = 10000;
    total_supply_foil = 1000;
  } else {
    total_supply_non_foil = CARD_SUPPLY.normal[card.rarity][card.advancement];
    total_supply_foil = CARD_SUPPLY.foil[card.rarity][card.advancement];
  }

  return {
    non_foil: total_supply_non_foil,
    foil: total_supply_foil,
  };
};

/**
 * Compute the percentage of supply already minted.
 * @param card the card.
 */
const getSupplyPercent = (card) => {
  const supply = getSupply(card);

  return (
    supply && {
      foil: percent(card.total_count_foil / supply.foil),
      non_foil: percent(card.total_count_non_foil / supply.non_foil),
    }
  );
};

module.exports = {
  CTA_COLLECTION_ADDRESS,
  CTA_PAGE_SIZE,
  ASSETS_SOURCE,
  CARD_SUPPLY,
  KNOWN_SUPPLY_RARITIES,
  MAX_ASSETS_PER_UPDATE,
  getSupply,
  getSupplyPercent,
};
