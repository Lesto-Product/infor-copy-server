const sql = require("mssql");
const config = require("../../config/db.config").local;

async function getPool() {
  return await new sql.ConnectionPool(config).connect();
}

/**
 * Генерира MERGE заявка и я изпълнява.
 * @param {string} tableName - Името на таблицата в MSSQL (напр. 'original_tdsls401')
 * @param {Array} data - Масив от обекти с данни
 * @param {Array} keys - Масив от стрингове, кои колони са Primary Key (напр. ['orno', 'pono'])
 */
async function upsertData_old(tableName, data, keys) {
  if (!data || data.length === 0) return 0;

  const pool = await getPool();
  let processed = 0;

  try {
    const columns = Object.keys(data[0]);

    const matchCondition = keys
      .map((k) => `Target.[${k}] = Source.[${k}]`)
      .join(" AND ");

    // 3. Update Set: Всичко, което НЕ е ключ
    const nonKeyColumns = columns.filter((c) => !keys.includes(c));
    const updateSet = nonKeyColumns
      .map((c) => `Target.[${c}] = Source.[${c}]`)
      .join(", ");

    // 4. Insert Columns и Values
    const insertCols = columns.map((c) => `[${c}]`).join(", ");
    const insertVals = columns.map((c) => `Source.[${c}]`).join(", ");

    const mergeQuery = `
      MERGE [dbo].[${tableName}] AS Target
      USING (SELECT ${columns
        .map((c) => `@${c} as [${c}]`)
        .join(", ")}) AS Source
      ON (${matchCondition})
      WHEN MATCHED THEN
        UPDATE SET ${updateSet}
      WHEN NOT MATCHED BY TARGET THEN
        INSERT (${insertCols}) 
        VALUES (${insertVals});
    `;

    for (const row of data) {
      const request = pool.request();

      columns.forEach((col) => {
        request.input(col, sql.NVarChar, row[col]);
      });

      await request.query(mergeQuery);
      processed++;
    }

    console.log(`Upserted ${processed} rows into ${tableName}`);
    return processed;
  } catch (err) {
    console.error(`Error upserting into ${tableName}:`, err);
    throw err;
  } finally {
    pool.close();
  }
}

async function upsertData(tableName, data, keys) {
  if (!data || data.length === 0) return 0;

  const pool = await getPool();
  try {
    const columns = Object.keys(data[0]);
    const tempTableName = `#Temp_${tableName}`;

    // 1. РЪЧНО СЪЗДАВАНЕ НА ВРЕМЕННАТА ТАБЛИЦА
    // Това гарантира, че SQL Server "вижда" обекта преди bulk операцията
    const createTempTableQuery = `
      CREATE TABLE ${tempTableName} (
        ${columns.map((col) => `[${col}] NVARCHAR(MAX)`).join(", ")}
      );
    `;

    const request = pool.request();
    await request.query(createTempTableQuery);

    // 2. ПОДГОТОВКА НА BULK ОБЕКТА
    const table = new sql.Table(tempTableName);
    columns.forEach((col) => {
      table.columns.add(col, sql.NVarChar(sql.MAX), { nullable: true });
    });

    data.forEach((row) => {
      if (row.PlannedOrder === null || row.PlannedOrder === undefined) {
        row.PlannedOrder = "0";
      }
      table.rows.add(
        ...columns.map((c) => (row[c] !== null ? String(row[c]) : null))
      );
    });

    // 3. ИЗПЪЛНЕНИЕ НА BULK INSERT
    await request.bulk(table);

    // 4. MERGE ЛОГИКА
    const matchCondition = keys
      .map((k) => `Target.[${k}] = Source.[${k}]`)
      .join(" AND ");
    const updateSet = columns
      .filter((c) => !keys.includes(c))
      .map((c) => `Target.[${c}] = Source.[${c}]`)
      .join(", ");

    const insertCols = columns.map((c) => `[${c}]`).join(", ");
    const insertVals = columns.map((c) => `Source.[${c}]`).join(", ");

    const finalMergeQuery = `
      MERGE [dbo].[${tableName}] AS Target
      USING ${tempTableName} AS Source
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
    console.error(`Bulk Error in upsertData for ${tableName}:`, err);
    throw err;
  } finally {
    pool.close();
  }
}
/**
 * Взима последната дата за инкрементален ъпдейт
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
  } finally {
    pool.close();
  }
}

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
  } finally {
    pool.close();
  }
}

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
    console.error("Failed to get sync logs. Did you run the SQL script?", err);
    return {};
  } finally {
    pool.close();
  }
}
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
        let value;
        if (col === "PlannedOrder") {
          value =
            row[col] !== undefined && row[col] !== null
              ? String(row[col])
              : null;
        } else {
          value = row[col] ? String(row[col]) : null;
        }
        request.input(col, sql.NVarChar, value);
      });

      try {
        await request.query(insertQuery);
      } catch (queryErr) {
        console.error(`[ERROR] Failed to insert row into ${tableName}!`);
        console.error("Problematic Record:", JSON.stringify(row, null, 2));
        throw queryErr;
      }
    }

    return data.length;
  } catch (err) {
    throw err;
  } finally {
    pool.close();
  }
}
module.exports = {
  upsertData,
  getMaxTimestamp,
  updateSyncLog,
  getSyncLogs,
  truncateAndInsert,
};
