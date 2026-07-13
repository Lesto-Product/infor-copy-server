const java = require("java");
const dbConfig = require("../../config/db.config").cloud;

async function fetchQuery(query) {
  let connection;
  try {
    const DriverManager = java.import("java.sql.DriverManager");

    connection = await DriverManager.getConnectionPromise(
      dbConfig.url,
      dbConfig.user,
      dbConfig.password
    );

    const statement = await connection.createStatementPromise();
    const resultSet = await statement.executeQueryPromise(query);
    const meta = await resultSet.getMetaDataPromise();
    const colCount = await meta.getColumnCountPromise();

    const columns = [];
    for (let i = 1; i <= colCount; i++) {
      columns.push(await meta.getColumnNamePromise(i));
    }

    // Ако cloud-ът върне дублирани имена на колони (case/whitespace-insensitive),
    // дъмпваме ги с char-codes - иначе тихо биха счупили по-нататък INSERT-а.
    const lc = columns.map((c) => String(c).trim().toLowerCase());
    const dupIdx = lc
      .map((v, i) => (lc.indexOf(v) !== i ? i : -1))
      .filter((i) => i >= 0);
    if (dupIdx.length) {
      console.warn(`[CLOUD DUP] дублирани имена на колони от JDBC!`);
      const uniq = new Set(dupIdx.flatMap((i) => [lc.indexOf(lc[i]), i]));
      for (const i of uniq) {
        const codes = Array.from(String(columns[i])).map((ch) =>
          ch.charCodeAt(0)
        );
        console.warn(
          `[CLOUD DUP]   idx=${i} [${columns[i]}] charCodes=${codes.join(",")}`
        );
      }
    }

    const results = [];
    while (await resultSet.nextPromise()) {
      const row = {};
      for (const col of columns) {
        const val = await resultSet.getObjectPromise(col);
        row[col] = val ? String(val) : null;
      }
      results.push(row);
    }

    return results;
  } catch (err) {
    console.error("Cloud Provider Error:", err);
    throw err;
  } finally {
    if (connection) await connection.closePromise();
  }
}

module.exports = { fetchQuery };
