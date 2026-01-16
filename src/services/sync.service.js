// src/services/sync.service.js
const standardDefs = require("../definitions/tables");
const preactorDefs = require("../definitions/preactor"); // Новият файл
const cloudProvider = require("../providers/cloud.provider");
const localProvider = require("../providers/local.provider");

async function syncTable(tableKey) {
  const isPreactor = tableKey.startsWith("preactor_");
  const def = isPreactor ? preactorDefs[tableKey] : standardDefs[tableKey];

  if (!def) throw new Error(`Definition for '${tableKey}' not found.`);

  console.log(`--- Starting Sync for ${tableKey} ---`);

  let query = "";
  if (isPreactor) {
    query = def.query;
  } else {
    let lastDate = "1970-01-01T00:00:00.000Z";
    if (def.incrementalColumn) {
      lastDate = await localProvider.getMaxTimestamp(
        def.localTable,
        def.incrementalColumn
      );
    }

    let where = def.incrementalColumn
      ? `WHERE CAST([${def.incrementalColumn}] AS DATETIME) > '${lastDate}'`
      : "WHERE 1=1";

    if (def.baseFilter) where += ` AND ${def.baseFilter}`;

    query = `SELECT ${def.fields} FROM ${def.cloudTable} ${where}`;
    if (def.groupBy) {
      query += ` GROUP BY ${def.groupBy}`;
    }
    console.log(`Constructed Query: ${query}`);
  }

  // 2. Fetch
  const data = await cloudProvider.fetchQuery(query);
  console.log(`Fetched ${data.length} rows.`);

  // 3. Save
  if (data.length > 0) {
    if (isPreactor || tableKey === "tdpur401") {
      await localProvider.truncateAndInsert(def.localTable, data);
    } else {
      await localProvider.upsertData(def.localTable, data, def.primaryKeys);
    }
  }

  await localProvider.updateSyncLog(tableKey, "SUCCESS", data.length);
  return { table: tableKey, rows: data.length, status: "SUCCESS" };
}

module.exports = { syncTable };
