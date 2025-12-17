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
async function upsertData(tableName, data, keys) {
  if (!data || data.length === 0) return 0;

  const pool = await getPool();
  let processed = 0;

  try {
    // Взимаме колоните от първия запис
    const columns = Object.keys(data[0]);

    // Подготвяме "динамичните" части на SQL-а
    // 1. Source parameters: @orno, @pono, @item...
    // 2. Match condition: Target.orno = Source.orno AND Target.pono = Source.pono
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

    // Генерираме SQL шаблона веднъж
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

    // Изпълняваме ред по ред (за безопасност при complex locks)
    // *Оптимизация за бъдещето: Може да се ползва BulkInsert или TVP, но за начало това е по-стабилно*
    for (const row of data) {
      const request = pool.request();

      // Биндим параметрите
      columns.forEach((col) => {
        // MSSQL Driver понякога се бърка с типовете, затова подаваме всичко като String (NVarChar)
        // както правеше в стария проект
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
    const safeRows = rowsCount || 0; // Защита срещу null

    await pool
      .request()
      .input("tbl", sql.NVarChar, tableName)
      .input("stat", sql.NVarChar, status)
      .input("rows", sql.Int, safeRows) // Подаваме бройката
      .query(`
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

// --- НОВО: Взима дата И брой редове ---
async function getSyncLogs() {
  const pool = await getPool();
  try {
    // Вече не проверяваме дали таблицата съществува, защото я създаде ръчно
    const res = await pool
      .request()
      .query(
        "SELECT TableName, LastSync, RowsAffected FROM [dbo].[_AppSyncLog]"
      );

    // Връщаме обект: { "tdsls401": { date: "...", rows: 500 }, ... }
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

module.exports = { upsertData, getMaxTimestamp, updateSyncLog, getSyncLogs };
