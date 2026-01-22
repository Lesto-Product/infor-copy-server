// src/definitions/tables.js

// --- 1. Дефиниране на полетата (Copy-Paste от стария fields.js с леко изчистване) ---

const fields = {
  sls400: `[orno], [ddat], [corn_bg_BG], [oamt], [ccur], [ofbp], [odat], [hdst], [itbp], [timestamp]`,

  sls401: `[oamt], [ofbp], [orno], [ddta], [item], [pric], [odat], [qoor], [timestamp], [corn_bg_BG], [pono], [ttyp], [clyn]`,

  sfc001: `[pdno], [mitm], [qrdr], [osta], [cprj], [grid], [qdlv], [timestamp]`,

  sfc010: `[opno], [refo], [opst], [cwoc], [rutm], [mtyp], [pdno], [qpli], [qcmp], [timestamp]`,

  rou001: `[cwoc], [dsca_bg_BG], [kowc]`,

  rou002: `[dsca_bg_BG], [mcno]`,

  ibd001: `main.[cdf_bcod], main.[item], main.[dsca_bg_BG], main.[cdf_adal_bg_BG], main.[cuni], main.[wght], main.[citg], main.[cdf_quad], client.[aitc_bg_BG]`,

  cst001: `[pono], [opno], [sitm], [qune]`,

  com100: `[nama_bg_BG], [bpid]`,

  sli305: `[idat], [brid], [ccur],MAX([amti]) as [amti], [itbp]`,

  pur400: `[hdst], [orno], [ccur]`,

  pur401: `[item], [ddta], [pric], [qidl], [orno], [otbp], [pono]`,

  tibom300: `[bmdl], MAX([bmrv]) AS [bmrv], [mitm]`,

  tibom310: `l.pono, l.sitm, MAX(l.qana) as qana, MAX(l.scpf) as scpf, l.bmdl, l.bmrv`,

  tdisa001: `item, ccur, cups, pris, cvat, qimo, timestamp`,

  tirou401: `t401.[opno], t401.[refo], t401.[cwoc], t401.[mitm], t401.[rutm], t401.[mtyp], t401.[prte], t401.[prtm], t401.[rorv], t401.[timestamp], t450.dsca_bg_BG`,
};

// --- 2. Описване на правилата за всяка таблица ---

const tableDefinitions = {
  tdsls400: {
    localTable: "original_tdsls400",
    cloudTable: "LN_tdsls400",
    fields: fields.sls400,
    primaryKeys: ["orno"],
    incrementalColumn: "timestamp",
    baseFilter: "CAST(ddat AS DATE) > '2020-01-01'",
  },

  tdsls401: {
    localTable: "original_tdsls401",
    cloudTable: "LN_tdsls401",
    fields: fields.sls401,
    primaryKeys: ["orno", "pono"],
    incrementalColumn: "timestamp",
    baseFilter: "CAST(ddta AS DATE) > '2020-01-01'",
  },

  // --- PRODUCTION ---
  tisfc001: {
    localTable: "original_tisfc001",
    cloudTable: "LN_tisfc001",
    fields: fields.sfc001,
    primaryKeys: ["pdno"],
    incrementalColumn: "timestamp",
  },

  tisfc010: {
    localTable: "original_tisfc010",
    cloudTable: "LN_tisfc010",
    fields: fields.sfc010,
    primaryKeys: ["pdno", "opno"],
    incrementalColumn: "timestamp",
    baseFilter: "pdno LIKE 'SFC%' AND CAST(prdt AS DATE) > '2023-12-31'",
  },

  // --- ITEMS (COMPLEX JOIN) ---
  //ТРЯБВА ДА ПРОВЕРЯ ЗАЩО ВЗИМА 130k ЗАПИСА, А ЗАПИСВА 30к+
  tcibd001: {
    localTable: "original_tcibd001",
    cloudTable:
      "LN_tcibd001 main LEFT JOIN LN_tcibd004 client ON client.item = main.item",
    fields: fields.ibd001,
    primaryKeys: ["item", "aitc_bg_BG"],
    baseFilter: "",
    incrementalColumn: null,
  },

  tirou001: {
    localTable: "original_tirou001",
    cloudTable: "LN_tirou001",
    fields: fields.rou001,
    primaryKeys: ["cwoc"],
    incrementalColumn: null,
  },

  tirou002: {
    localTable: "original_tirou002",
    cloudTable: "LN_tirou002",
    fields: fields.rou002,
    primaryKeys: ["mcno"],
    incrementalColumn: null,
  },

  tccom100: {
    localTable: "original_tccom100",
    cloudTable: "LN_tccom100",
    fields: fields.com100,
    primaryKeys: ["bpid"],
    baseFilter: "nama_bg_BG not like N'%TAX%' and (bprl = '2' or bprl = '4')",
  },

  cisli305: {
    localTable: "original_cisli305",
    cloudTable: "LN_cisli305",
    fields: fields.sli305,
    primaryKeys: ["brid", "itbp", "idat"],
    baseFilter: "CAST(idat AS DATE) > '2018-01-01'",
    incrementalColumn: null,
    groupBy: "[idat], [brid], [ccur], [itbp]",
  },

  tdpur400: {
    localTable: "original_tdpur400",
    cloudTable: "LN_tdpur400",
    fields: fields.pur400,
    primaryKeys: ["orno"],
    incrementalColumn: null,
  },

  tdpur401: {
    localTable: "original_tdpur401",
    cloudTable: "LN_tdpur401",
    fields: fields.pur401,
    primaryKeys: ["orno", "pono", "item"],
    incrementalColumn: null,
  },

  tibom300: {
    localTable: "original_tibom300",
    cloudTable: "LN_tibom300",
    fields: fields.tibom300,
    primaryKeys: ["bmdl", "mitm"],
    incrementalColumn: null,
    baseFilter: "bmst = '20'",
    groupBy: "[bmdl], [mitm]",
  },

  tibom310: {
    localTable: "original_tibom310",
    cloudTable: `LN_tibom310 l INNER JOIN (
    SELECT bmdl, MAX(bmrv) AS bmrv, bmst
    FROM LN_tibom300
    WHERE bmst = '20'
    GROUP BY bmdl, bmst
    ) p ON l.bmdl = p.bmdl AND l.bmrv = p.bmrv`,
    fields: fields.tibom310,
    primaryKeys: ["bmdl", "pono", "sitm"],
    incrementalColumn: null,
    groupBy: "l.pono, l.sitm, l.bmdl, l.bmrv",
  },

  tdisa001: {
    localTable: "original_tdisa001",
    cloudTable: "LN_tdisa001",
    fields: fields.tdisa001,
    primaryKeys: ["item"],
    incrementalColumn: "timestamp",
  },
  tirou401: {
    localTable: "original_tirou401",
    cloudTable: `LN_tirou401 t401 LEFT JOIN (
      SELECT refo, MAX(dsca_bg_BG) as dsca_bg_BG 
      FROM LN_tirou450 
      GROUP BY refo
    ) t450 ON t401.refo = t450.refo`,
    fields: fields.tirou401,
    primaryKeys: ["mitm", "opno", "rorv"],
    incrementalColumn: "timestamp",
    baseFilter: "trim(t401.mitm) NOT LIKE 'SLS%'",
  },
};

module.exports = tableDefinitions;
