const dotenv = require("dotenv");
dotenv.config();

const express = require("express");
const cors = require("cors");
const { CTAManager } = require("./src/CTAManager");

const PORT = process.env.PORT || 3000;
const CTA_DATA_REFRESHING_PERIOD =
  (process.env.REFRESH_PERIOD_IN_MINUTES || 10) * 60 * 1000;

const corsOptions = {
  origin: process.env.CORS_ORIGIN || "http://localhost:3002",
  optionSuccessStatus: 200,
};

const app = express();
app.use(cors(corsOptions));

const ctaManager = new CTAManager();

/**
 * ENTRY POINTS
 */

/**
 * Get the full CTA collection supply statistics.
 */
app.get("/stats", async (req, res) => {
  const separateFoil = req.query?.separateFoil;
  const stats = await ctaManager.getCollectionStats(separateFoil);
  res.json(stats);
});

/**
 * Get the list of CTA cards.
 */
app.get("/collection", async (req, res) => {
  const collection = await ctaManager.getCardCollection();
  res.json(collection);
});

/**
 * Get statistics about a card.
 */
app.get("/card", async (req, res) => {
  const id = req?.query?.id;
  if (!id) return res.send("error");

  const data = await ctaManager.getCardDetail(id);
  res.json(data);
});

/**
 * Get statistics about a user.
 */
app.get("/user", async (req, res) => {
  const address = req.query.address;
  if (!address) return res.send("error");

  const data = await ctaManager.getUserCollection(address);
  res.json(data);
});

/**
 * Get the list of users with their card count.
 */
app.get("/users", async (req, res) => {
  const data = await ctaManager.getUsers();
  res.json(data);
});

// Default response for any other request
app.use((req, res) => res.status(404));

/**
 * CTA Init & Update
 */
setImmediate(async () => {
  await ctaManager.initialize();
  await ctaManager.update();
});

setInterval(async () => {
  await ctaManager.update();
}, CTA_DATA_REFRESHING_PERIOD);

/**
 * Server listening
 */
app.listen(PORT, () => {
  console.log(`listening on ${PORT} ...`);
});
