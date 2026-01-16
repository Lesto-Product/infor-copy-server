const sql = require("mssql");
const config = require("../../config/db.config").local;

// Глобална променлива за поддържане на една активна връзка (Pool)
let globalPool = null;

/**
 * Създава или връща съществуващ Connection Pool.
 * Предотвратява изчерпването на ресурсите на SQL Express.
 */
async function getPool() {
  if (globalPool && globalPool.connected) {
    return globalPool;
  }

  try {
    globalPool = await new sql.ConnectionPool(config).connect();
    console.log("[MSSQL] Глобалният Connection Pool е установен успешно.");
    return globalPool;
  } catch (err) {
    console.error(
      "[MSSQL] Критична грешка при свързване с локалната база:",
      err
    );
    globalPool = null;
    throw err;
  }
}

/**
 * Основна функция за Bulk Upsert чрез глобална временна таблица.
 * Решава проблеми с Collation и производителност.
 */
async function upsertData(tableName, data, keys) {
  if (!data || data.length === 0) return 0;

  const pool = await getPool();
  // Уникално име за глобалната временна таблица, за да се вижда от Bulk процеса
  const tempTableName = `##TempBulk_${tableName}_${Date.now()}`;

  try {
    const columns = Object.keys(data[0]);
    const request = pool.request();
    request.timeout = 300000;
    // 1. СЪЗДАВАНЕ С DATABASE_DEFAULT (Решава Collation Conflict)
    const createTempTableQuery = `
      CREATE TABLE ${tempTableName} (
        ${columns
          .map((col) => `[${col}] NVARCHAR(MAX) COLLATE DATABASE_DEFAULT`)
          .join(", ")}
      );
    `;
    await request.query(createTempTableQuery);

    // 2. ПОДГОТОВКА И BULK INSERT
    const table = new sql.Table(tempTableName);
    columns.forEach((col) => {
      table.columns.add(col, sql.NVarChar(sql.MAX), { nullable: true });
    });

    data.forEach((row) => {
      // Защита за PlannedOrder
      if (
        row.hasOwnProperty("PlannedOrder") &&
        (row.PlannedOrder === null || row.PlannedOrder === undefined)
      ) {
        row.PlannedOrder = "0";
      }
      table.rows.add(
        ...columns.map((c) => (row[c] !== null ? String(row[c]) : null))
      );
    });

    await request.bulk(table);

    // 3. MERGE С ИЗРИЧЕН COLLATE ПРИ КЛЮЧОВЕТЕ И DISTINCT
    const matchCondition = keys
      .map((k) => `Target.[${k}] = Source.[${k}] COLLATE DATABASE_DEFAULT`)
      .join(" AND ");

    const nonKeyColumns = columns.filter((c) => !keys.includes(c));
    const updateSet = nonKeyColumns
      .map((c) => `Target.[${c}] = Source.[${c}]`)
      .join(", ");
    const insertCols = columns.map((c) => `[${c}]`).join(", ");
    const insertVals = columns.map((c) => `Source.[${c}]`).join(", ");

    const finalMergeQuery = `
      MERGE [dbo].[${tableName}] AS Target
      USING (SELECT DISTINCT * FROM ${tempTableName}) AS Source
      ON (${matchCondition})
      WHEN MATCHED THEN
        UPDATE SET ${updateSet}
      WHEN NOT MATCHED BY TARGET THEN
        INSERT (${insertCols}) VALUES (${insertVals});
      
      DROP TABLE ${tempTableName};
    `;

    await request.query(finalMergeQuery);
    console.log(`[BULK SUCCESS] ${tableName}: ${data.length} rows merged.`);
    return data.length;
  } catch (err) {
    console.error(`Грешка при Bulk Upsert в ${tableName}:`, err);
    try {
      await pool
        .request()
        .query(
          `IF OBJECT_ID('tempdb..${tempTableName}') IS NOT NULL DROP TABLE ${tempTableName}`
        );
    } catch (e) {}
    throw err;
  }
  // pool.close() е премахнат, за да се запази глобалната връзка
}

/**
 * Взима последната дата за инкрементален ъпдейт.
 */
async function getMaxTimestamp(tableName, timeColumn) {
  const pool = await getPool();
  try {
    const result = await pool
      .request()
      .query(
        `SELECT MAX(CAST([${timeColumn}] as DATETIME)) as maxTs FROM [dbo].[${tableName}]`
      );
    const maxTs = result.recordset[0].maxTs;
    return maxTs ? new Date(maxTs).toISOString() : "1970-01-01T00:00:00.000Z";
  } catch (err) {
    console.warn(
      `Could not get timestamp for ${tableName}, defaulting to 1970.`
    );
    return "1970-01-01T00:00:00.000Z";
  }
}

/**
 * Обновява системния лог за синхронизация.
 */
async function updateSyncLog(tableName, status, rowsCount) {
  const pool = await getPool();
  try {
    const safeRows = rowsCount || 0;
    await pool
      .request()
      .input("tbl", sql.NVarChar, tableName)
      .input("stat", sql.NVarChar, status)
      .input("rows", sql.Int, safeRows).query(`
        MERGE [dbo].[_AppSyncLog] AS target
        USING (SELECT @tbl AS TableName) AS source
        ON (target.TableName = source.TableName)
        WHEN MATCHED THEN
            UPDATE SET LastSync = GETDATE(), Status = @stat, RowsAffected = @rows
        WHEN NOT MATCHED THEN
            INSERT (TableName, LastSync, Status, RowsAffected) 
            VALUES (@tbl, GETDATE(), @stat, @rows);
      `);
  } catch (err) {
    console.error("Failed to update sync log", err);
  }
}

/**
 * Взима логовете за всички таблици.
 */
async function getSyncLogs() {
  const pool = await getPool();
  try {
    const res = await pool
      .request()
      .query(
        "SELECT TableName, LastSync, RowsAffected FROM [dbo].[_AppSyncLog]"
      );
    const logs = {};
    res.recordset.forEach((row) => {
      logs[row.TableName] = {
        date: row.LastSync,
        rows: row.RowsAffected,
      };
    });
    return logs;
  } catch (err) {
    console.error("Failed to get sync logs.", err);
    return {};
  }
}

/**
 * Изтрива всичко и налива наново (използва се за Preactor таблици).
 */
async function truncateAndInsert(tableName, data) {
  if (!data || data.length === 0) return 0;

  const pool = await getPool();
  try {
    console.log(`[LOCAL] Truncating table ${tableName}...`);
    await pool.request().query(`DELETE FROM [dbo].[${tableName}]`);

    const columns = Object.keys(data[0]);
    const insertCols = columns.map((c) => `[${c}]`).join(", ");
    const insertVals = columns.map((c) => `@${c}`).join(", ");
    const insertQuery = `INSERT INTO [dbo].[${tableName}] (${insertCols}) VALUES (${insertVals})`;

    for (const row of data) {
      const request = pool.request();
      if (row.PlannedOrder === null || row.PlannedOrder === undefined) {
        row.PlannedOrder = "0";
      }
      columns.forEach((col) => {
        let value =
          row[col] !== null && row[col] !== undefined ? String(row[col]) : null;
        request.input(col, sql.NVarChar, value);
      });
      await request.query(insertQuery);
    }
    return data.length;
  } catch (err) {
    throw err;
  }
}

module.exports = {
  upsertData,
  getMaxTimestamp,
  updateSyncLog,
  getSyncLogs,
  truncateAndInsert,
};
