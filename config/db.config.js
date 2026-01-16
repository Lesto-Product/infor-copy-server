require("dotenv").config();

module.exports = {
  local: {
    user: process.env.LOCAL_DB_USER || "sa",
    password: process.env.LOCAL_DB_PASSWORD || "evrista_pass359",
    server: process.env.LOCAL_DB_SERVER || "192.168.1.187\\SQLEXPRESS",
    database: process.env.LOCAL_DB_NAME || "Lesto",
    options: {
      encrypt: false,
      enableArithAbort: false,
      trustServerCertificate: true,
      connectTimeout: 300000,
      requestTimeout: 600000,
    },
  },
  cloud: {
    url: process.env.CLOUD_DB_URL || "jdbc:infordatalake://ILESTOPRODUCT_PRD",
    user: process.env.CLOUD_DB_USER || "",
    password: process.env.CLOUD_DB_PASSWORD || "",
  },
};
