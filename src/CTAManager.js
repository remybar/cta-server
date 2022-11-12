const { ImmutableX, Config } = require("@imtbl/core-sdk");
const fs = require("fs");
const debug = require("debug")("cta");

const { Repository } = require("./Repository");

const {
  CTA_COLLECTION_ADDRESS,
  CTA_PAGE_SIZE,
  ASSETS_SOURCE,
  MAX_ASSETS_PER_UPDATE,
  getSupplyPercent,
  getSupply,
} = require("./constants");

/**
 * Get the mint pass type from the metadata image filename
 * @param asset the asset retrieved from IMX.
 */
const getMintPassType = (asset) => {
  const url = asset.metadata.image;
  const filename = url.substring(url.lastIndexOf("/") + 1);
  return filename.replace(".png", "");
};

/**
 * Get the card ID from the metadata image filename.
 * @param asset the asset retrieved from IMX.
 */
const getMetaCardId = (asset) => {
  const url = asset.metadata.image;
  const filename = url.substring(url.lastIndexOf("/") + 1);
  return filename.split("-")[0];
};

/**
 * Main manager of CTA assets got from the IMX blockchain
 */
class CTAManager {
  constructor() {
    this.lastCursor = undefined;
    this.repository = new Repository();
  }

  /**
   * Log retrieved assets in a file in debug mode.
   * @param assets assets to log.
   */
  logAssets(assets) {
    if (debug.enabled) {
      // be sure that the assets folder exists
      fs.mkdirSync("debug/assets", { recursive: true });

      fs.writeFileSync(
        `debug/assets/${new Date().toISOString()}.json`,
        JSON.stringify(assets, null, 2)
      );
    }
  }

  /**
   * Get assets from a file (used for debugging)
   * @returns list of assets.
   */
  _getAssetsFromFile() {
    const assets = JSON.parse(fs.readFileSync("assets.json"));
    return assets;
  }

  /**
   * Get a new assets page from IMX blockchain.
   * @returns the assets list from the requested page.
   */
  async _getNewAssetsPage() {
    let params = {
      collection: CTA_COLLECTION_ADDRESS,
      pageSize: CTA_PAGE_SIZE,
      metadata: encodeURI(JSON.stringify({ tokenType: ["CARD", "MINT_PASS"] })),
      orderBy: "updated_at",
      direction: "asc",
      cursor: this.lastCursor,
    };

    const { cursor, result } = await this.client.listAssets(params);

    this.lastCursor = cursor;
    return result;
  }

  /**
   * Get next assets from IMX blockchain or from a file.
   * @returns new list of assets.
   */
  async getNextAssets() {
    if (ASSETS_SOURCE === "imx") return await this._getNewAssetsPage();

    if (this.assetsFileAlreadyRead) return [];

    this.assetsFileAlreadyRead = true;
    return this._getAssetsFromFile();
  }

  /**
   * Initialize the IMX client and the CTA database.
   */
  async initialize() {
    await this.repository.initialize();

    const config = Config.PRODUCTION;
    this.client = new ImmutableX(config);
  }

  /**
   * Update CTA database from IMX blockchain data.
   */
  async update() {
    let lastAssetTs = null;
    let assets;
    let assetsCount = 0;

    try {
      assets = await this.getNextAssets();
      assetsCount = assets.length;
      this.logAssets(assets);
    } catch (error) {
      debug(
        `update ABORTED cause of the following error: ${error}. Will continue at the next update`
      );
      return;
    }

    while (assets.length > 0 && assetsCount < MAX_ASSETS_PER_UPDATE) {
      const updateTs = new Date().toISOString();
      let metaCards = new Map();
      let mintPassTypes = new Map();
      let users = new Set();
      let rarities = new Set();
      let elements = new Set();
      let families = new Set();
      let cards = new Map();
      let mintPasses = new Map();

      // process new/updated assets which are not burned
      assets
        .filter((a) => a.status !== "burned")
        .forEach((asset) => {
          if (asset.metadata.tokenType === "MINT_PASS") {
            const {
              user: user,
              token_id: tokenId,
              metadata: {
                name: name,
                description: description,
                numbering: numbering,
                image: imageUrl,
              },
              created_at: created_at,
              updated_at: updated_at,
            } = asset;
            const passType = getMintPassType(asset);

            if (!mintPassTypes.has(passType)) {
              mintPassTypes.set(passType, {
                passType,
                name,
                description,
                imageUrl,
              });
            }
            if (!mintPasses.has(tokenId)) {
              mintPasses.set(tokenId, {
                id: parseInt(tokenId),
                passType,
                user,
                numbering,
                created_at,
                updated_at,
              });
            }
          }

          if (asset.metadata.tokenType === "CARD") {
            const {
              user: user,
              status: status,
              token_id: tokenId,
              metadata: {
                name: name,
                description: description,
                rarity: rarity,
                set: family,
                element: element,
                power: power,
                foil: foil,
                rank: rank,
                grade: grade,
                potential: potential,
                numbering: numbering,
                image: imageUrl,
                advancement: advancement,
                cardType: cardType,
                animationLevel: animationLevel,
              },
              created_at: created_at,
              updated_at: updated_at,
            } = asset;
            const metaCardId = getMetaCardId(asset);

            elements.add(element);
            rarities.add(rarity);
            families.add(family);
            users.add(user);

            // meta cards list
            if (!metaCards.has(metaCardId)) {
              metaCards.set(metaCardId, {
                id: metaCardId,
                name,
                description,
                imageUrl,
                element,
                rarity,
                family,
                advancement,
                cardType,
              });
            }

            // cards
            if (!cards.has(tokenId)) {
              cards.set(tokenId, {
                id: parseInt(tokenId),
                card_meta_id: metaCardId,
                user,
                status,
                foil,
                rank,
                grade,
                power,
                potential,
                numbering,
                animationLevel,
                created_at,
                updated_at,
              });
            }
          }
        });

      await this.repository.update({
        arkomes: [...elements],
        rarities: [...rarities],
        families: [...families],
        users: [...users],
        metaCards: Array.from(metaCards.values()),
        cards: Array.from(cards.values()),
        mintPassTypes: Array.from(mintPassTypes.values()),
        mintPasses: Array.from(mintPasses.values()),
      });

      // process burned assets
      let burnedPassIds = [];
      let burnedCardIds = [];
      assets
        .filter((a) => a.status === "burned")
        .forEach((asset) => {
          if (asset.metadata.tokenType === "MINT_PASS") {
            burnedPassIds = [...burnedPassIds, parseInt(asset.token_id)];
          }

          if (asset.metadata.tokenType === "CARD") {
            burnedCardIds = [...burnedCardIds, parseInt(asset.token_id)];
          }
        });

      await this.repository.burn({
        passIds: burnedPassIds,
        cardIds: burnedCardIds,
      });

      if (assets.length > 0) {
        lastAssetTs = assets[0].updated_at;

        await this.repository.recordUpdate(
          updateTs,
          lastAssetTs,
          assets.length
        );
      }

      try {
        assets = await this.getNextAssets();
        assetsCount = assets.length;
        this.logAssets(assets);
      } catch (error) {
        debug(
          `update ABORTED cause of the following error: ${error}. Will continue at the next update`
        );
        return;
      }
    }

    debug(`update DONE (assets retrieved: ${assetsCount})`);
  }

  /**
   * get CTA statistics from the database.
   * @returns the statistics.
   */
  async getCollectionStats() {
    let stdCollection = new Map();
    let altCollection = new Map();

    const { stdCards, altCards, mintPasses } =
      await this.repository.getCollectionStats();

    // process standard cards
    stdCards.forEach((c) => {
      const cardCount = parseInt(c.count);
      const values = stdCollection.get(c.id) || {
        id: c.id,
        foil: Boolean(c.foil),
        card_name: c.card_name,
        card_type: c.card_type,
        advancement: c.advancement,
        element: c.element,
        rarity: c.rarity,
        total_count: 0,
        total_count_non_foil: 0,
        total_count_foil: 0,
        supply_percent: 0,
        supply: getSupply(c),
        ranks: {
          standard: { r1: 0, r2: 0, r3: 0, r4: 0, r5: 0 },
          foil: { r1: 0, r2: 0, r3: 0, r4: 0, r5: 0 },
        },
      };

      values.ranks[c.foil ? "foil" : "standard"][`r${c.rank}`] = cardCount;
      values.total_count += cardCount;
      values.total_count_non_foil += c.foil ? 0 : cardCount;
      values.total_count_foil += c.foil ? cardCount : 0;

      stdCollection.set(c.id, values);
    });

    // process alternative cards
    altCards.forEach((c) => {
      const cardCount = parseInt(c.count);
      const values = altCollection.get(c.id) || {
        id: c.id,
        foil: Boolean(c.foil),
        card_name: c.card_name,
        card_type: c.card_type,
        advancement: c.advancement,
        element: c.element,
        rarity: c.rarity,
        total_count: 0,
        total_count_non_foil: 0,
        total_count_foil: 0,
        supply_percent: 0,
        supply: getSupply(c),
        grades: {
          foil: { C: 0, B: 0, A: 0, S: 0 },
          standard: { C: 0, B: 0, A: 0, S: 0 },
        },
      };

      values.grades[c.foil ? "foil" : "standard"][c.grade] = cardCount;
      values.total_count += cardCount;
      values.total_count_non_foil += c.foil ? 0 : cardCount;
      values.total_count_foil += c.foil ? cardCount : 0;

      altCollection.set(c.id, values);
    });

    // compute the percentage of supply already minted
    stdCollection.forEach((c) => {
      c.supply_percent = getSupplyPercent(c);
    });
    altCollection.forEach((c) => {
      c.supply_percent = getSupplyPercent(c);
    });

    return {
      std_cards: Array.from(stdCollection.values()),
      alt_cards: Array.from(altCollection.values()),
      mint_passes: mintPasses,
      last_update: await this.repository.get_last_update(),
    };
  }

  /**
   * Get user collection from its IMX wallet address
   * @param address IMX wallet address of an user.
   * @returns the user card collection.
   */
  async getUserCollection(address) {
    const collection = await this.repository.getUserCollection(address);
    const userInfo = await this.repository.getUserInfo(address);
    return {
      info: userInfo,
      collection,
    };
  }

  /**
   * Get card details (i.e card information + who owns cards of this ID)
   */
  async getCardDetail(id) {
    const cardDetail = await this.repository.getCardDetail(id);
    const cardList = await this.repository.getCardList(id);
    return {
      detail: cardDetail,
      cardList: cardList,
    };
  }

  /**
   * Get the full card collection.
   */
  async getCardCollection() {
    const collection = await this.repository.getCardCollection();
    return collection;
  }

  /**
   * Get the list of users.
   */
  async getUsers(pageIndex = 0, pageSize = 50) {
    const users = await this.repository.getUsers(pageIndex, pageSize);
    return users;
  }
}

module.exports = { CTAManager };
