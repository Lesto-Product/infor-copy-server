// queries/preactor/production.query.js

// This is your original, complex query. It is now a simple exported string again.
const productionQuery = `SELECT 
DISTINCT LN_tisfc001.pdno AS OrderNo,
LN_tisfc001.cprj AS Project,
LN_tcmcs052.dsca_bg_BG AS ProjectDescription,
LTRIM(LN_tisfc001.mitm) AS PartNo,
MainItem.dsca_bg_BG AS Product,
CASE
     WHEN LN_tisfc001.cdld = '1970-01-01 00:00:00.000' THEN LN_tisfc001.rdld
     ELSE LN_tisfc001.cdld
END AS DueDate,
CASE
     WHEN LN_tisfc010.opno IS NULL THEN 10
     ELSE LN_tisfc010.opno
END AS OpNo,
CASE
     WHEN LN_tisfc010.tano IS NULL THEN '10'
     ELSE LN_tisfc010.tano
END AS TaskNo,
CASE
     WHEN LN_tirou003.dsca_bg_BG IS NULL THEN N'Поръчка без операции'
     ELSE LN_tirou003.dsca_bg_BG
END AS Operation,
CASE
     WHEN LN_tisfc010.cwoc IS NULL THEN '999'
     ELSE LN_tisfc010.cwoc
END AS ResourceGroup,
CASE
     WHEN LN_tisfc010.qpli IS NULL THEN LN_tisfc001.qrdr
     ELSE LN_tisfc010.qpli
END AS OrderedQuantity,
CASE
     WHEN LN_tisfc010.qpli IS NULL THEN LN_tisfc001.qrdr - LN_tisfc001.qdlv
     ELSE LN_tisfc010.qpli - LN_tisfc010.qcmp
END AS RemainingQuantity,
LN_tisfc010.rutm AS RemainingTime,
     CASE
     WHEN LN_tisfc010.mvtm IS NULL THEN 0
     ELSE LN_tisfc010.mvtm / 24
END AS MoveTime,
     CASE
     WHEN LN_tirou001.dsca_bg_BG IS NULL THEN N'Поръчки без операции'
     ELSE LN_tirou001.dsca_bg_BG
END AS ResourceGroupName,
N'Време за Партида' AS TimeType,
SUBSTRING(LN_tisfc001.mitm, 10, 40) AS Drawing,
CASE
     WHEN LN_tisfc010.mopr IS NULL THEN 0
     ELSE LN_tisfc010.mopr
END AS NumOfOp,
MAINITEM.wght AS Weight,
LN_tcmcs061.dsca_bg_BG AS CoverType,
Materials.item AS MaterialType,
Materials.seab_bg_BG AS Thickness,
CASE
     WHEN LN_tisfc001.cdld = '1970-01-01 00:00:00.000' THEN '1970-01-01 00:00:00.000'
     ELSE LN_tisfc001.cdld
END AS ConfirmedDeliveryDate,
CASE
     WHEN LN_tisfc001.cdld = '1970-01-01 00:00:00.000' THEN 0
     ELSE 1
END AS ConfirmedDeliveryDateFromLN,
LN_tisfc001.rgrp AS RoutingGroup,
0 AS PlannedOrder,
Materials.Dscb_bg_BG as Dscb,
LN_tcmcs052.Seab AS Seab,
LN_tisfc010.opst AS Status
FROM
LN_tisfc001
LEFT JOIN LN_tisfc010 ON
LN_tisfc001.pdno = LN_tisfc010.pdno
AND LN_tisfc010.opst <= 5
INNER JOIN LN_tcibd001 MainItem ON
LN_tisfc001.mitm = MainItem.item
LEFT JOIN LN_tirou003 ON
LN_tisfc010.tano = LN_tirou003.tano
LEFT JOIN LN_tirou001 ON
LN_tisfc010.cwoc = LN_tirou001.cwoc
LEFT JOIN LN_tcmcs052 ON
LN_tisfc001.cprj = LN_tcmcs052.cprj
LEFT JOIN LN_tcmcs061 ON
MAINITEM.cpln = LN_tcmcs061.cpln
LEFT JOIN LN_ticst001 ON
LN_tisfc001.pdno = LN_ticst001.pdno
AND LN_ticst001.pono = 10
LEFT JOIN LN_tcibd001 Materials ON
LN_ticst001.sitm = Materials.item
AND Materials.citg = N'301'
LEFT JOIN (
SELECT
     orno,
     opno,
     SUM(hrea) AS ReportedTime
FROM
     LN_bptmm120
GROUP BY
     orno,
     opno
) ReportedHours ON
LN_tisfc010.pdno = ReportedHours.orno
AND LN_tisfc010.opno = ReportedHours.opno
WHERE LN_tisfc001.osta <= 70 AND LN_tisfc001.rdld > '2023-01-01'
--AND CASE WHEN LN_tisfc010.qpli IS NULL THEN LN_tisfc001.qrdr - LN_tisfc001.qdlv ELSE LN_tisfc010.qpli - LN_tisfc010.qcmp END > 0 
`;

module.exports = productionQuery;
