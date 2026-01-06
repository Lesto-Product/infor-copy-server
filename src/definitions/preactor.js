// src/definitions/preactor.js
module.exports = {
  preactor_production: {
    localTable: "LNProductionOrders",
    query: require("./queries/preactor/production.query"),
  },
  preactor_bom: {
    localTable: "LNBOM",
    query: require("./queries/preactor/bom.query"),
  },
  preactor_sales: {
    localTable: "LNSalesOrders",
    query: require("./queries/preactor/sales.query"),
  },
  preactor_stock: {
    localTable: "LNStock",
    query: require("./queries/preactor/stock.query"),
  },
  preactor_purchase: {
    localTable: "LNPurchaseOrders",
    query: require("./queries/preactor/purchase.query"),
  },
  preactor_invoices: {
    localTable: "LNInvoices",
    query: require("./queries/preactor/invoice.query"),
  },
  preactor_purchase_invoices: {
    localTable: "LNPurchaseInvoices",
    query: require("./queries/preactor/purchaseInvoice.query"),
  },
  preactor_open_sales: {
    localTable: "ProdOrderWithOpenSls",
    query: require("./queries/preactor/openSales.query"),
  },
};
