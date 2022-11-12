const { Pool } = require("pg");
const fs = require("fs");
const format = require("pg-format");
const debug = require("debug")("database");

/**
 * convert a list of fields in a SQL field names string.
 */
const _fieldString = (fieldNames) => {
  return typeof fieldNames === "string" ? fieldNames : fieldNames.join(",");
};

/**
 * generate a list of values based on a list of fields to extract from a list of
 * objects.
 */
const _fieldValues = (fieldNames, o) => {
  return typeof fieldNames === "string"
    ? typeof o === "object"
      ? o[fieldNames]
      : o
    : fieldNames.map((f) => o[f]);
};

/**
 * Service to access to a database.
 */
class DatabaseService {
  constructor() {
    this.pool = new Pool();
  }

  /**
   * Initialize the service.
   */
  async initialize() {
    debug("initializing...");
  }

  /**
   * Terminate the service.
   */
  async finalize() {
    debug("finalizing...");
    await this.pool.end();
  }

  /**
   * query the database (could be a select, a create table, ...)
   * @param {*} query the SQL query.
   * @param {*} params the list of params.
   */
  async query(query, params) {
    try {
      const { rows } = await this.pool.query(query, params);
      return rows;
    } catch (error) {
      /*       fs.writeFileSync(
        `debug/databases/${new Date().toISOString()}.json`,
        JSON.stringify({ query: query.split("\n"), params, error }, null, 2)
      );
 */ return [];
    }
  }

  /**
   *
   */
  async delete(table, ids) {
    await this.pool.query(format(`DELETE FROM ${table} WHERE id IN (%L)`, ids));
  }

  /**
   * Insert new rows.
   * @param {*} tableName the table name.
   * @param {*} fieldNames the list of field names to insert.
   * @param {*} pkName the name of the primary key.
   * @param {*} values the field values as an array of objects where keys = fieldNames.
   * @returns the list of primary key values of inserted rows.
   */
  async insert(tableName, fieldNames, pkName, values) {
    debug(`insert: ${tableName} (count: ${values.length})`);

    const fieldString = _fieldString(fieldNames);
    const newRows = values.map((v) => [_fieldValues(fieldNames, v)]);

    const pkValues = await this.query(
      format(
        `
            INSERT INTO ${tableName}(${fieldString})
            VALUES %L
            ON CONFLICT DO NOTHING
            RETURNING ${pkName}
            `,
        newRows
      )
    );

    return pkValues.map((o) => o[pkName]);
  }

  /**
   * Update existing rows.
   * @param {*} tableName the table name.
   * @param {*} fieldNames the list of field names to update.
   * @param {*} values the field values as an array of objects where keys = fieldNames.
   */
  async update(tableName, fieldNames, pkName, values) {
    debug(`update: ${tableName} (count: ${values.length})`);

    const fieldString = _fieldString(fieldNames);

    for (const value of values) {
      const query = format(
        `
                UPDATE ${tableName}
                   SET (${fieldString}) = %L
                 WHERE ${pkName} = ${value[pkName]}
                `,
        [_fieldValues(fieldNames, value)]
      );
      const res = await this.query(query);
    }
  }

  /**
   * Insert or update rows.
   * @param {*} tableName the table name.
   * @param {*} fieldNames the list of field names to insert/update.
   * @param {*} pkName the name of the primary key.
   * @param {*} values the field values as an array of objects where keys = fieldNames.
   * @param {*} insertPk insert the pk value or let the database manager to generate it.
   */
  async upsert(tableName, fieldNames, pkName, values, insertPk = false) {
    debug(`upsert: ${tableName} (count: ${values.length})`);

    const pkValues = await this.insert(
      tableName,
      insertPk ? fieldNames : fieldNames.filter((f) => f !== pkName),
      pkName,
      insertPk
        ? values
        : values.map((v) => {
            const { [pkName]: unused, ...rest } = v;
            return rest;
          })
    );

    const updatedRows =
      pkValues.length > 0
        ? values.filter((v) => pkName in v && !pkValues.includes(v[pkName]))
        : values;

    if (updatedRows.length > 0)
      await this.update(tableName, fieldNames, pkName, updatedRows);
  }
}

module.exports = { DatabaseService };
