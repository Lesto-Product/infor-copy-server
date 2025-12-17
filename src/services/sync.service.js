const definitions = require("../definitions/tables");
const cloudProvider = require("../providers/cloud.provider");
const localProvider = require("../providers/local.provider");

async function syncTable(tableKey) {
  const def = definitions[tableKey];
  if (!def) throw new Error(`Definition for table '${tableKey}' not found.`);

  console.log(`--- Starting Sync for ${tableKey} ---`);

  // 1. Проверка за инкрементален update
  let whereClause = "";
  if (def.incrementalColumn) {
    const lastDate = await localProvider.getMaxTimestamp(
      def.localTable,
      def.incrementalColumn
    );
    console.log(`Last timestamp for ${def.localTable}: ${lastDate}`);
    whereClause = `WHERE CAST([${def.incrementalColumn}] AS DATETIME) > '${lastDate}'`;
  } else {
    whereClause = "WHERE 1=1"; // Dummy start
  }

  // 2. Добавяне на статичен филтър (ако има)
  if (def.baseFilter) {
    whereClause += ` AND ${def.baseFilter}`;
  }

  // 3. Сглобяване на Query-то
  const query = `SELECT ${def.fields} FROM ${def.cloudTable} ${whereClause}`;
  console.log("Executing Cloud Query:", query);

  // 4. Fetch
  const data = await cloudProvider.fetchQuery(query);
  console.log(`Fetched ${data.length} rows from Cloud.`);

  // 5. Upsert Local
  if (data.length > 0) {
    await localProvider.upsertData(def.localTable, data, def.primaryKeys);
  }
  await localProvider.updateSyncLog(tableKey, "SUCCESS", data.length);

  return { table: tableKey, rows: data.length, status: "SUCCESS" };
}

module.exports = { syncTable };
