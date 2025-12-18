// index.js
require("dotenv").config();

const { initializeDataLake } = require("./db/initializeDatalake");
const { startWebServer } = require("./src/app");
const { startScheduler } = require("./src/services/scheduler.service");

const logger = {
  info: (msg) => console.log(`[INFO] ${msg}`),
  error: (msg, err) => console.error(`[ERROR] ${msg}`, err),
};

process.on("uncaughtException", (err) => {
  logger.error("There was an uncaught error", err);
  process.exit(1);
});

async function main() {
  try {
    logger.info("Initializing Data Lake connection...");

    await initializeDataLake();
    startWebServer();
    startScheduler();
    logger.info("Application started successfully.");

    setInterval(() => {}, 1000);
  } catch (err) {
    logger.error("A critical error occurred during application startup:", err);
    process.exit(1);
  }
}

main();
