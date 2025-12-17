const express = require("express");
const cors = require("cors");
const syncService = require("./services/sync.service");
const tableDefinitions = require("./definitions/tables");

function startWebServer() {
  const app = express();
  app.use(cors());
  app.use(express.json());

  // GENERIC TRIGGER
  // Този endpoint може да викне: /trigger/tdsls401 или /trigger/tisfc001
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
    const logs = await localProvider.getSyncLogs();

    const list = Object.keys(tableDefinitions).map((key) => {
      const logEntry = logs[key];

      return {
        key: key,
        localTable: tableDefinitions[key].localTable,
        cloudTable: tableDefinitions[key].cloudTable,
        lastSync: logEntry ? logEntry.date : null,
        lastRows: logEntry ? logEntry.rows : 0,
      };
    });

    res.json(list);
  });

  app.listen(3005, () => {
    console.log("Web server listening on port 3005.");
  });
}

module.exports = { startWebServer };
