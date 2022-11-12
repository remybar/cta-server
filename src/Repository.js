const debug = require("debug")("repository");
const { DatabaseService } = require("./DatabaseService");

/**
 * Manage CTA repository.
 */
class Repository {
  constructor() {
    this.dbService = new DatabaseService();
  }

  /**
   * Create the database schema.
   * @note: use 'IF NOT EXISTS' clause to avoid errors while trying to create the schema.
   */
  async _createSchema() {
    // UPDATE_HISTORY Table
    await this.dbService.query(
      `
            CREATE TABLE IF NOT EXISTS UPDATE_HISTORY (
                id                   SERIAL PRIMARY KEY,
                update_timestamp     TEXT NOT NULL,
                last_asset_timestamp TEXT NOT NULL,
                assets_count         INT
            )
            `
    );

    // ELEMENT Table
    await this.dbService.query(
      `
            CREATE TABLE IF NOT EXISTS ELEMENT (
                id   SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )
            `
    );

    // RARITY Table
    await this.dbService.query(
      `
            CREATE TABLE IF NOT EXISTS RARITY (
                id   SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )
            `
    );

    // FAMILY Table
    await this.dbService.query(
      `
            CREATE TABLE IF NOT EXISTS FAMILY (
                id   SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )
            `
    );

    // CTA_USER Table
    await this.dbService.query(
      `
            CREATE TABLE IF NOT EXISTS CTA_USER (
                id      SERIAL PRIMARY KEY,
                address TEXT NOT NULL UNIQUE
            )
            `
    );

    // CARD_META Table
    await this.dbService.query(
      `
            CREATE TABLE IF NOT EXISTS CARD_META (
                id          INT PRIMARY KEY,
                name        TEXT,
                description TEXT,
                image_url   TEXT,
                advancement TEXT,
                card_type   TEXT,

                element_id INT REFERENCES ELEMENT(id),
                rarity_id  INT REFERENCES RARITY(id),
                family_id  INT REFERENCES FAMILY(id)
            )
            `
    );

    // CARD Table
    await this.dbService.query(
      `
            CREATE TABLE IF NOT EXISTS CARD (
                id         INT PRIMARY KEY,
                foil       INT,
                rank       INT,
                grade      TEXT,
                animationLevel INT,
                numbering  INT,
                power      INT,
                created_at TEXT,
                updated_at TEXT,

                card_meta_id INT REFERENCES CARD_META(id),
                user_id      INT REFERENCES CTA_USER(id) 
            )
            `
    );

    // MINT PASS TYPE Table
    await this.dbService.query(
      `
            CREATE TABLE IF NOT EXISTS MINT_PASS_TYPE (
                id          SERIAL PRIMARY KEY,
                pass_type   TEXT,
                name        TEXT,
                description TEXT,
                image_url   TEXT
            )
            `
    );

    // MINT PASS Table
    await this.dbService.query(
      `
            CREATE TABLE IF NOT EXISTS MINT_PASS (
                id         INT PRIMARY KEY,
                numbering  INT,
                created_at TEXT,
                updated_at TEXT,

                mint_pass_type_id INT REFERENCES MINT_PASS_TYPE(id),
                user_id           INT REFERENCES CTA_USER(id) 
            )
            `
    );
  }

  /**
   * Read a 'Enum' table from the database.
   * @param tableName name of the table to read.
   * @param fieldName name of the field containing the enum value.
   * @returns a Map (enum value => row id).
   */
  async _get_enum_table(tableName, fieldName) {
    const rows = await this.dbService.query(
      `SELECT id, ${fieldName} FROM ${tableName}`
    );
    return new Map(rows.map((r) => [r[fieldName], r.id]));
  }

  /**
   * Update 'Enum' table (i.e a table that contains enum fields) by inserting new values.
   * @param tableName the table name.
   * @param fieldName the field to update ('name', ...)
   * @param items the list of items to create/update.
   */
  async _insertNewValuesInEnumTable(
    tableName,
    fieldName,
    items,
    readExistingValues
  ) {
    const existingValues = await readExistingValues();
    const newValues = items.filter((v) => !existingValues.has(v));

    if (newValues.length > 0) {
      await this.dbService.insert(tableName, fieldName, "id", newValues);
    } else {
      if (items.length > 0) {
        debug(
          `SKIP ${tableName} values: ${items.filter((e) =>
            existingValues.has(e)
          )}`
        );
      }
    }
  }

  /**
   * Get element values.
   */
  async _read_elements() {
    return await this._get_enum_table("ELEMENT", "name");
  }

  /**
   * Get rarity values.
   */
  async _read_rarities() {
    return await this._get_enum_table("RARITY", "name");
  }

  /**
   * Get families values.
   */
  async _read_families() {
    return await this._get_enum_table("FAMILY", "name");
  }

  /**
   * Get users values.
   */
  async _read_users() {
    return await this._get_enum_table("CTA_USER", "address");
  }

  /**
   * Get the list of mint pass types.
   */
  async _read_mint_pass_types() {
    const rows = await this.dbService.query(
      "SELECT id, pass_type FROM MINT_PASS_TYPE"
    );
    return new Map(rows.map((r) => [r.pass_type, r.id]));
  }

  /**
   * Get the timestamp of the last asset stored in the database.
   */
  async readLastAssetTimestamp() {
    const rows = await this.dbService.query(`
            SELECT   last_asset_timestamp
            FROM     UPDATE_HISTORY
            ORDER BY update_timestamp DESC
            LIMIT    1
        `);
    return rows.length > 0 ? rows[0].last_asset_timestamp : undefined;
  }

  /**
   *
   */
  async get_last_update() {
    const rows = await this.dbService.query(
      `
              SELECT update_timestamp
                FROM UPDATE_HISTORY
            ORDER BY id DESC
               LIMIT 1
            `
    );
    return rows[0].update_timestamp;
  }

  /**
   * Update arkomes.
   * @param elements list of candidates for new elements.
   */
  async updateElements(elements) {
    await this._insertNewValuesInEnumTable(
      "ELEMENT",
      "name",
      elements,
      this._read_elements.bind(this)
    );
  }

  /**
   * Update rarities.
   * @param rarities list of candidates for new rarities.
   */
  async updateRarities(rarities) {
    await this._insertNewValuesInEnumTable(
      "RARITY",
      "name",
      rarities,
      this._read_rarities.bind(this)
    );
  }

  /**
   * Update families.
   * @param families list of candidates for new families.
   */
  async updateFamilies(families) {
    await this._insertNewValuesInEnumTable(
      "FAMILY",
      "name",
      families,
      this._read_families.bind(this)
    );
  }

  /**
   * Update users.
   * @param users list of candidates for new users.
   */
  async updateUsers(users) {
    await this._insertNewValuesInEnumTable(
      "CTA_USER",
      "address",
      users,
      this._read_users.bind(this)
    );
  }

  /**
   * Update meta cards.
   * @param metaCards list of candidates for new meta cards.
   */
  async updateMetaCards(metaCards) {
    if (metaCards.length <= 0) return;

    const elements = await this._read_elements();
    const rarities = await this._read_rarities();
    const families = await this._read_families();

    const rows = metaCards.map((r) => {
      const mc = {
        ...r,
        image_url: r.imageUrl,
        card_type: r.cardType,
        element_id: elements.get(r.element),
        rarity_id: rarities.get(r.rarity),
        family_id: families.get(r.family),
      };
      return mc;
    });

    await this.dbService.upsert(
      "CARD_META",
      [
        "id",
        "name",
        "description",
        "image_url",
        "advancement",
        "card_type",
        "element_id",
        "rarity_id",
        "family_id",
      ],
      "id",
      rows,
      true
    );
  }

  /**
   * Update cards.
   * @param cards list of candidates for new cards.
   */
  async updateCards(cards) {
    if (cards.length <= 0) return;

    const users = await this._read_users();

    const rows = cards.map((c) => ({
      ...c,
      foil: c.foil ? 1 : 0,
      power: c.power,
      card_meta_id: c.card_meta_id,
      user_id: users.get(c.user),
    }));

    await this.dbService.upsert(
      "CARD",
      [
        "id",
        "foil",
        "rank",
        "grade",
        "animationLevel",
        "numbering",
        "power",
        "created_at",
        "updated_at",
        "card_meta_id",
        "user_id",
      ],
      "id",
      rows,
      true
    );
  }

  /**
   * Update mint pass types.
   * @param {*} mintPassTypes list of candidates for new mint pass types.
   */
  async updateMintPassTypes(mintPassTypes) {
    if (mintPassTypes.length <= 0) return;

    const existingPassTypes = await this._read_mint_pass_types();
    const existingPassTypeNames = Array.from(existingPassTypes.keys());

    const rows = mintPassTypes
      .filter((p) => !existingPassTypeNames.includes(p.passType))
      .map((p) => ({
        ...p,
        pass_type: p.passType,
        image_url: p.imageUrl,
      }));

    if (rows.length > 0) {
      await this.dbService.insert(
        "MINT_PASS_TYPE",
        ["pass_type", "name", "description", "image_url"],
        "id",
        rows
      );
    }
  }

  /**
   * Update mint passes.
   * @param {*} mintPasses list of candidates for new mint passes.
   */
  async updateMintPasses(mintPasses) {
    if (mintPasses.length <= 0) return;

    const users = await this._read_users();
    const existingPassTypes = await this._read_mint_pass_types();

    const rows = mintPasses.map((r) => ({
      ...r,
      mint_pass_type_id: existingPassTypes.get(r.passType),
      user_id: users.get(r.user),
    }));

    await this.dbService.upsert(
      "MINT_PASS",
      [
        "id",
        "numbering",
        "created_at",
        "updated_at",
        "mint_pass_type_id",
        "user_id",
      ],
      "id",
      rows,
      true
    );
  }

  /**
   *
   */
  async burnPasses(passIds) {
    if (passIds.length > 0) {
      debug(`mint pass BURNED: [${passIds.join(", ")}]`);
      await this.dbService.delete("MINT_PASS", passIds);
    }
  }

  /**
   *
   */
  async burnCards(cardIds) {
    if (cardIds.length > 0) {
      debug(`cards BURNED: [${cardIds.join(", ")}]`);
      await this.dbService.delete("CARD", cardIds);
    }
  }

  /**
   * Store an update point information.
   * @param {*} updateTimestamp timestamp of the update
   * @param {*} lastAssetTimestamp update timestamp of the last stored asset
   * @param {*} assetsCount number of assets retrieved for this update.
   */
  async recordUpdate(updateTimestamp, lastAssetTimestamp, assetsCount) {
    await this.dbService.insert(
      "UPDATE_HISTORY",
      ["update_timestamp", "last_asset_timestamp", "assets_count"],
      "id",
      [
        {
          update_timestamp: updateTimestamp,
          last_asset_timestamp: lastAssetTimestamp,
          assets_count: assetsCount,
        },
      ]
    );
    debug(
      "update",
      `up ts: ${updateTimestamp}, asset ts: ${lastAssetTimestamp}, assets count: ${assetsCount}`
    );
  }

  /**
   * Initialize the database.
   */
  async initialize() {
    await this.dbService.initialize();
    await this._createSchema();
  }

  /**
   * Update the database.
   * @param elements new arkomes candidates.
   * @param rarities new rarities candidates.
   * @param families new families candidates.
   * @param users new users candidates.
   * @param metaCards new metaCards candidates.
   * @param cards new cards candidates.
   * @param mintPassTypes new mint pass types candidates.
   * @param mintPasses new mintPasses candidates.
   */
  async update({
    arkomes,
    rarities,
    families,
    users,
    metaCards,
    cards,
    mintPassTypes,
    mintPasses,
  }) {
    await this.updateElements(arkomes);
    await this.updateRarities(rarities);
    await this.updateFamilies(families);
    await this.updateUsers(users);
    await this.updateMetaCards(metaCards);
    await this.updateCards(cards);
    await this.updateMintPassTypes(mintPassTypes);
    await this.updateMintPasses(mintPasses);
  }

  async burn({ passIds, cardIds }) {
    await this.burnPasses(passIds);
    await this.burnCards(cardIds);
  }

  /**
   * Extract some statistics of the CTA collection.
   * @param separateFoil indicates if foil cards have to be counted separately.
   * @returns the statistics.
   */
  async getCollectionStats() {
    const _query = (_rankOrGrade, where) =>
      `
            SELECT mc.id, 
                   c.foil,
                   mc.name                 AS card_name,
                   mc.card_type,
                   mc.advancement,
                   a.name                  AS element,
                   r.name                  AS rarity,
                   ${_rankOrGrade},
                   COUNT(c.id)             AS count,
                   MIN(c.numbering)        AS min_number,
                   MAX(c.numbering)        AS max_number,
                   mc.card_type
            FROM CARD c
            JOIN CARD_META mc ON mc.id = c.card_meta_id
            JOIN ELEMENT a ON a.id = mc.element_id
            JOIN RARITY r ON r.id = mc.rarity_id
            WHERE ${where}
            GROUP BY mc.id, c.foil, ${_rankOrGrade}, a.name, r.name, mc.card_type
            ORDER BY mc.id, c.foil, ${_rankOrGrade}
            `;

    const stdCards = await this.dbService.query(
      _query("c.rank", "mc.advancement = 'STANDARD'")
    );
    const altCards = await this.dbService.query(
      _query("c.grade", "mc.advancement != 'STANDARD'")
    );
    const mintPasses = await this.dbService.query(
      `
            SELECT mt.id, 
                   mt.name,
                   COUNT(p.id) AS count,
                   MIN(p.numbering) AS min_number,
                   MAX(p.numbering) AS max_number
            FROM MINT_PASS p
            JOIN MINT_PASS_TYPE mt ON mt.id = p.mint_pass_type_id
            GROUP BY mt.id
            ORDER BY mt.id
            `
    );

    return { stdCards, altCards, mintPasses };
  }

  /**
   * Get the full user card collection.
   * @param address IMX wallet address of the user.
   * @returns the full user collection.
   */
  async getUserCollection(address) {
    const rows = this.dbService.query(
      `
            SELECT c.id, c.foil, c.rank, c.numbering, c.power, c.card_meta_id
              FROM CARD c
              JOIN CTA_USER u ON u.id = c.user_id
             WHERE u.address = $1
             `,
      [address]
    );
    return rows;
  }

  async getUserInfo(address) {
    const rows = await this.dbService.query(
      `
      SELECT r.name, m.advancement, c.foil, count(*)
        FROM CARD c
        JOIN CARD_META m ON m.id = c.card_meta_id
        JOIN RARITY r ON r.id = m.rarity_id
        JOIN CTA_USER u ON u.id = c.user_id
        WHERE u.address = $1
        GROUP BY r.name, m.advancement, c.foil
      `,
      [address]
    );

    const res = {};
    rows.forEach((r) => {
      const name = r.name.toLowerCase();
      const advancement = r.advancement.toLowerCase();

      if (!res[name]) {
        res[name] = {
          standard: { normal: 0, foil: 0 },
          alternative: { normal: 0, foil: 0 },
          alternative_combo: { normal: 0, foil: 0 },
        };
      }
      res[name][advancement][r.foil ? "foil" : "normal"] = parseInt(r.count);
    });

    return res;
  }

  /**
   * Get the full card collection.
   * @returns the full card collection.
   */
  async getCardCollection() {
    const rows = this.dbService.query(
      `
            SELECT m.id,
                   m.name,
                   m.description,
                   m.image_url,
                   m.advancement,
                   m.card_type,
                   a.name AS element,
                   r.name AS rarity,
                   f.name AS family
              FROM CARD_META m
              JOIN ELEMENT a ON a.id = m.element_id
              JOIN RARITY r ON r.id = m.rarity_id
              JOIN FAMILY f ON f.id = m.family_id
              ORDER BY m.id
            `
    );
    return rows;
  }

  /**
   * Get details about a card using its ID.
   */
  async getCardDetail(id) {
    const rows = await this.dbService.query(
      `
      SELECT m.id,
             m.name,
             m.description,
             m.image_url,
             m.advancement,
             m.card_type,
             a.name AS element,
             r.name AS rarity,
             f.name AS family
        FROM CARD_META m
        JOIN ELEMENT a ON a.id = m.element_id
        JOIN RARITY r ON r.id = m.rarity_id
        JOIN FAMILY f ON f.id = m.family_id
       WHERE m.id = $1
      `,
      [id]
    );
    return rows[0];
  }

  /**
   * Get the list of cards related to an ID.
   */
  async getCardList(id) {
    const rows = this.dbService.query(
      `
        SELECT c.foil,
               c.rank,
               c.numbering,
               c.power,
               u.address
          FROM CARD c
          JOIN CTA_USER u ON u.id = c.user_id 
         WHERE c.card_meta_id = $1
      `,
      [id]
    );

    return rows;
  }

  /**
   * Get the list of users.
   */
  async getUsers(pageIndex, pageSize) {
    const rows = this.dbService.query(
      `
          SELECT u.address, count(c.id) AS card_count
            FROM CARD c,
                 CTA_USER u
           WHERE u.id = c.user_id
        GROUP BY u.address
        ORDER BY count(c.id) DESC
          OFFSET $1
           LIMIT $2
      `,
      [pageIndex * pageSize, pageSize]
    );
    return rows;
  }
}

module.exports = { Repository };
