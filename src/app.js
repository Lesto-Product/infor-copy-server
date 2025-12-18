const express = require("express");
const cors = require("cors");
const syncService = require("./services/sync.service");
const standardDefs = require("./definitions/tables");
const preactorDefs = require("./definitions/preactor");
const localProvider = require("./providers/local.provider");

function startWebServer() {
  const app = express();
  app.use(cors());
  app.use(express.json());

  // GENERIC TRIGGER
  app.post("/trigger/:tableKey", async (req, res) => {
    const { tableKey } = req.params;
    try {
      const result = await syncService.syncTable(tableKey);
      res.json(result);
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    }
  });

  app.get("/health", (req, res) => {
    res.send("Service is running correctly!");
  });

  app.get("/tables", async (req, res) => {
    try {
      const logs = await localProvider.getSyncLogs();

      // Комбинираме дефинициите
      const allDefs = { ...standardDefs, ...preactorDefs };

      const list = Object.keys(allDefs).map((key) => {
        const def = allDefs[key];
        return {
          key: key,
          localTable: def.localTable,
          // Ако е preactor, името е "Complex Query", иначе е името на облачната таблица
          cloudTable: def.cloudTable || "Preactor SQL Query",
          lastSync: logs[key]?.date || null,
          lastRows: logs[key]?.rows || 0,
        };
      });

      res.json(list);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.listen(3005, () => {
    console.log("Web server listening on port 3005.");
  });
}

module.exports = { startWebServer };
