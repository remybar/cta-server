const { assert, expect } = require("chai");
const { Client } = require("pg");
const format = require("pg-format");

const { DatabaseService } = require("../src/DatabaseService");

describe("DatabaseService", () => {
  const client = new Client();
  let dbService;

  const persons = [
    { firstname: "john", lastname: "doe" },
    { firstname: "mike", lastname: "horn" },
  ];

  /**
   * Return an object o without its property p.
   */
  const withoutProp = (o, p) => {
    const { [p]: unused, ...res } = o;
    return res;
  };

  const withoutIds = (rows) => rows.map((r) => withoutProp(r, "id"));

  const createSchema = async () => {
    await client.query(
      `
            CREATE TABLE IF NOT EXISTS Person (
                id        SERIAL PRIMARY KEY,
                firstname TEXT,
                lastname  TEXT
            )
            `
    );
  };

  const deleteSchema = async () => {
    await client.query(`DROP TABLE IF EXISTS Person`);
  };

  const fillTables = async () => {
    await client.query(
      format(
        `INSERT INTO Person(firstname, lastname) VALUES %L`,
        persons.map((p) => [p.firstname, p.lastname])
      )
    );
  };

  const cleanTables = async () => {
    await client.query("DELETE FROM Person");
  };

  const getPersons = async () => {
    const { rows } = await client.query(
      "select id, firstname, lastname from Person"
    );
    return rows;
  };
  const getPersonsWithoutIds = async () => {
    const { rows } = await client.query(
      "select firstname, lastname from Person"
    );
    return rows;
  };

  before(async () => {
    await client.connect();
    await createSchema();
  });

  beforeEach(async () => {
    dbService = new DatabaseService();
    await dbService.initialize();
    await cleanTables();
    await fillTables();
  });

  after(async () => {
    await deleteSchema();
    await client.end();
  });

  afterEach(async () => {
    await dbService.finalize();
  });

  describe("query", async () => {
    it("should select all persons", async () => {
      const rows = await dbService.query(
        "SELECT firstname, lastname FROM Person"
      );
      expect(rows).to.have.deep.members(persons);
    });
  });

  describe("insert", () => {
    it("should insert 2 new persons", async () => {
      // arrange
      const newPersons = [
        { firstname: "marilyn", lastname: "monroe" },
        { firstname: "john", lastname: "wayne" },
      ];

      // act
      await dbService.insert(
        "Person",
        ["firstname", "lastname"],
        "id",
        newPersons
      );

      // assert
      const rows = await getPersonsWithoutIds();
      expect(rows).to.include.deep.members(newPersons);
    });

    it("should insert new person only", async () => {
      const persons = await getPersons();
      const newPersons = [
        ...persons,
        { firstname: "marilyn", lastname: "monroe" },
        { firstname: "john", lastname: "wayne" },
      ];

      // act
      await dbService.insert(
        "Person",
        ["firstname", "lastname"],
        "id",
        newPersons
      );

      // assert
      const rows = await getPersonsWithoutIds();
      expect(rows).to.include.deep.members(withoutIds(newPersons));
    });
  });

  describe("update", () => {
    it("should update 2 persons", async () => {
      // arrange
      const persons = await getPersons();

      const updatedPersons = [
        { ...persons[0], firstname: "marilyn", lastname: "monroe" },
        { ...persons[1], firstname: "john", lastname: "wayne" },
      ];

      // act
      await dbService.update(
        "Person",
        ["firstname", "lastname"],
        "id",
        updatedPersons
      );

      // assert
      rows = await getPersons();
      expect(rows).to.include.deep.members(updatedPersons);
    });
  });

  describe("upsert", () => {
    it("should update 2 persons and add 3 persons with ids", async () => {
      // arrange
      const persons = await getPersons();

      const updatedPersons = [
        { ...persons[0], firstname: "marilyn", lastname: "monroe" },
        { ...persons[1], firstname: "john", lastname: "wayne" },
      ];
      const newPersons = [
        { id: 99999, firstname: "bob", lastname: "marley" },
        { id: 99998, firstname: "eric", lastname: "clapton" },
        { id: 99997, firstname: "janis", lastname: "joplin" },
      ];

      // act
      await dbService.upsert("Person", ["firstname", "lastname"], "id", [
        ...updatedPersons,
        ...newPersons,
      ]);

      // assert
      const rows = await getPersons();
      expect(rows, "updated failed").to.include.deep.members(updatedPersons);

      // for new person, ignore ids as they have changed
      expect(withoutIds(rows), "new failed").to.include.deep.members(
        withoutIds(newPersons)
      );
    });

    it("should update 2 persons and add 3 persons without ids", async () => {
      // arrange
      const persons = await getPersons();

      const updatedPersons = [
        { ...persons[0], firstname: "marilyn", lastname: "monroe" },
        { ...persons[1], firstname: "john", lastname: "wayne" },
      ];
      const newPersons = [
        { firstname: "bob", lastname: "marley" },
        { firstname: "eric", lastname: "clapton" },
        { firstname: "janis", lastname: "joplin" },
      ];

      // act
      await dbService.upsert("Person", ["firstname", "lastname"], "id", [
        ...updatedPersons,
        ...newPersons,
      ]);

      // assert
      const rows = await getPersons();
      expect(rows, "updated failed").to.include.deep.members(updatedPersons);

      // for new person, ignore ids as they have changed
      expect(withoutIds(rows), "new failed").to.include.deep.members(
        newPersons
      );
    });
  });
});
