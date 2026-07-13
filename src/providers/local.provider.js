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
 * Cloud-ът (Compass JDBC) при някои заявки връща една и съща колона повече от
 * веднъж (напр. при composite ключове / изрази). Тогава генерираният INSERT/SET
 * списък съдържа колоната два пъти -> SQL Msg 264 ("column specified more than
 * once"). Махаме дубликатите case-insensitive, като пазим първото срещане.
 */
function dedupeColumns(rawColumns, tableName) {
  const seen = new Set();
  const out = [];
  const dropped = [];
  for (const c of rawColumns) {
    const lc = String(c).trim().toLowerCase();
    if (seen.has(lc)) {
      dropped.push(c);
      continue;
    }
    seen.add(lc);
    out.push(c);
  }
  if (dropped.length) {
    console.warn(
      `[DEDUP] ${tableName}: махнати дублирани колони ${JSON.stringify(
        dropped
      )} (пълен списък от cloud: ${JSON.stringify(rawColumns)})`
    );
  }
  return out;
}

/**
 * Основна функция за Bulk Upsert чрез глобална временна таблица.
 * Решава проблеми с Collation и производителност.
 */
async function upsertData(tableName, data, keys, incrementalColumn) {
  if (!data || data.length === 0) return 0;

  const pool = await getPool();
  // Уникално име за глобалната временна таблица, за да се вижда от Bulk процеса
  const tempTableName = `##TempBulk_${tableName}_${Date.now()}`;

  try {
    const columns = dedupeColumns(Object.keys(data[0]), tableName);
    const keySet = new Set(keys);
    // Ключовите колони ги правим NVARCHAR(200) (индексируеми, < 1700B лимит на
    // индекса дори при съставен ключ). Останалите остават NVARCHAR(MAX).
    const keySqlType = sql.NVarChar(200);
    const colTypeSql = (col) =>
      keySet.has(col) ? "NVARCHAR(200)" : "NVARCHAR(MAX)";

    // Timeout-ът за .query() идва от pool config (requestTimeout, 30 мин).
    // node-mssql НЯМА работещо `request.timeout` property - затова се вдига
    // на ниво pool в config/db.config.js.
    const request = pool.request();
    // 1. СЪЗДАВАНЕ С DATABASE_DEFAULT (Решава Collation Conflict)
    const createTempTableQuery = `
      CREATE TABLE ${tempTableName} (
        ${columns
          .map((col) => `[${col}] ${colTypeSql(col)} COLLATE DATABASE_DEFAULT`)
          .join(", ")}
      );
    `;
    await request.query(createTempTableQuery);

    // 2. ПОДГОТОВКА И BULK INSERT (на партиди)
    // ВАЖНО: request.bulk() игнорира request.timeout и ползва requestTimeout
    // на пула (600000ms). При голяма таблица (напр. tcibd001 - пълно
    // презареждане, incrementalColumn: null) един bulk с всички редове удря
    // този лимит и пада с "Timeout: Request failed to complete in 600000ms".
    // Затова наливаме на партиди - всяка партида е отделна кратка заявка,
    // а ## глобалната temp таблица е видима от всички заявки към пула.
    const BULK_CHUNK_SIZE = 5000;
    for (let offset = 0; offset < data.length; offset += BULK_CHUNK_SIZE) {
      const chunk = data.slice(offset, offset + BULK_CHUNK_SIZE);

      const table = new sql.Table(tempTableName);
      columns.forEach((col) => {
        table.columns.add(col, keySet.has(col) ? keySqlType : sql.NVarChar(sql.MAX), {
          nullable: true,
        });
      });

      chunk.forEach((row) => {
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

      // bulk() не се влияе от requestTimeout - стриймва до край.
      const bulkRequest = pool.request();
      await bulkRequest.bulk(table);

      const loaded = Math.min(offset + BULK_CHUNK_SIZE, data.length);
      console.log(`[BULK] ${tableName}: ${loaded}/${data.length} реда в temp.`);
    }

    // 2b. Индекс върху ключовите колони -> MERGE join-ът вече не е пълно
    // сканиране на temp таблицата. Това е основната причина merge-ът да
    // отнемаше >10 мин при tcibd001 и да падаше с timeout.
    const indexCols = keys.map((k) => `[${k}]`).join(", ");
    console.log(`[INDEX] ${tableName}: изграждане на индекс по ключа...`);
    await request.query(
      `CREATE INDEX [IX_keys] ON ${tempTableName} (${indexCols});`
    );

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

    const rowNumberPartition = keys.map((k) => `[${k}]`).join(", ");
    // Дедупликация по ключа (един ред на ключ). За инкременталните таблици -
    // най-новият по [incrementalColumn]. За пълните презареждания - без
    // подредба. Ползваме ROW_NUMBER вместо DISTINCT *, защото DISTINCT върху
    // NVARCHAR(MAX) колони прави огромен и бавен сорт.
    const orderBy = incrementalColumn
      ? `[${incrementalColumn}] DESC`
      : `(SELECT NULL)`;
    const deduplicatedSource = `SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ${rowNumberPartition} ORDER BY ${orderBy}) AS _rn
            FROM ${tempTableName}
          ) AS _dedup WHERE _rn = 1`;

    const finalMergeQuery = `
      MERGE [dbo].[${tableName}] AS Target
      USING (${deduplicatedSource}) AS Source
      ON (${matchCondition})
      WHEN MATCHED THEN
        UPDATE SET ${updateSet}
      WHEN NOT MATCHED BY TARGET THEN
        INSERT (${insertCols}) VALUES (${insertVals});
      
      DROP TABLE ${tempTableName};
    `;

    console.log(
      `[MERGE DBG] ${tableName} keys=${JSON.stringify(
        keys
      )} cols(${columns.length})=${JSON.stringify(columns)}`
    );
    console.log(`[MERGE DBG] ${tableName} insertCols=${insertCols}`);
    console.log(`[MERGE DBG] ${tableName} updateSet=${updateSet}`);
    console.log(`[MERGE DBG] ${tableName} SQL>>>\n${finalMergeQuery}\n<<<`);
    console.log(`[MERGE] ${tableName}: сливане на ${data.length} реда...`);
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
 * Брой редове в локална таблица (за да разберем дали е за първоначално
 * зареждане). При грешка връщаме 1 (приемаме "не е празна" -> безопасно,
 * върви по MERGE пътя, а не по разрушителния full reload).
 */
async function countRows(tableName) {
  const pool = await getPool();
  try {
    const res = await pool
      .request()
      .query(`SELECT COUNT_BIG(*) AS c FROM [dbo].[${tableName}]`);
    return Number(res.recordset[0].c) || 0;
  } catch (err) {
    console.warn(`Could not count rows for ${tableName}: ${err.message}`);
    return 1;
  }
}

/**
 * Бързо ПЪЛНО зареждане без MERGE - за bootstrap на празна таблица.
 * bulk в ## temp (стриймва, не се влияе от requestTimeout) + едно set-based
 * DELETE + INSERT..SELECT. Няма скъпия UPDATE-на-всеки-ред на MERGE-а.
 */
async function fullReload(tableName, data, keys, incrementalColumn) {
  if (!data || data.length === 0) return 0;

  const pool = await getPool();
  const tempTableName = `##TempFull_${tableName}_${Date.now()}`;
  const request = pool.request();

  try {
    const columns = dedupeColumns(Object.keys(data[0]), tableName);
    await request.query(
      `CREATE TABLE ${tempTableName} (${columns
        .map((c) => `[${c}] NVARCHAR(MAX) COLLATE DATABASE_DEFAULT`)
        .join(", ")});`
    );

    const BULK_CHUNK_SIZE = 5000;
    for (let offset = 0; offset < data.length; offset += BULK_CHUNK_SIZE) {
      const chunk = data.slice(offset, offset + BULK_CHUNK_SIZE);

      const table = new sql.Table(tempTableName);
      columns.forEach((col) => {
        table.columns.add(col, sql.NVarChar(sql.MAX), { nullable: true });
      });
      chunk.forEach((row) => {
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

      const bulkRequest = pool.request();
      await bulkRequest.bulk(table);

      const loaded = Math.min(offset + BULK_CHUNK_SIZE, data.length);
      console.log(`[FULL] ${tableName}: ${loaded}/${data.length} реда в temp.`);
    }

    // Дедупликация по ключа (както при MERGE), за да няма дублирани ключове.
    const insertCols = columns.map((c) => `[${c}]`).join(", ");
    let sourceSelect = `SELECT ${insertCols} FROM ${tempTableName}`;
    if (keys && keys.length) {
      const partition = keys.map((k) => `[${k}]`).join(", ");
      const orderBy = incrementalColumn
        ? `[${incrementalColumn}] DESC`
        : `(SELECT NULL)`;
      sourceSelect = `SELECT ${insertCols} FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ${partition} ORDER BY ${orderBy}) AS _rn
            FROM ${tempTableName}
          ) AS _d WHERE _rn = 1`;
    }

    console.log(`[FULL] ${tableName}: презареждане на целевата таблица...`);
    await request.query(`
      DELETE FROM [dbo].[${tableName}];
      INSERT INTO [dbo].[${tableName}] (${insertCols})
      ${sourceSelect};
      DROP TABLE ${tempTableName};
    `);
    console.log(`[FULL SUCCESS] ${tableName}: ${data.length} реда заредени.`);
    return data.length;
  } catch (err) {
    console.error(`Грешка при Full Reload в ${tableName}:`, err);
    try {
      await pool
        .request()
        .query(
          `IF OBJECT_ID('tempdb..${tempTableName}') IS NOT NULL DROP TABLE ${tempTableName}`
        );
    } catch (e) {}
    throw err;
  }
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
  fullReload,
  countRows,
  getMaxTimestamp,
  updateSyncLog,
  getSyncLogs,
  truncateAndInsert,
};
