const purchaseQuery = `SELECT CAST(LN_tdpur401.orno AS varchar(10)) + '_' + CAST(LN_tdpur401.pono AS varchar(10)) AS OrderNo, 
LTRIM(LN_tdpur401.item) AS PartNo,
LN_tcibd001.dsca_bg_BG AS Product, 
CASE WHEN LN_tdpur401.cuqp = LN_tcibd001.cuni THEN LN_tdpur401.qoor - LN_tdpur401.qidl ELSE (LN_tdpur401.qoor - LN_tdpur401.qidl) * LN_tcibd003.conv END AS Quantity, 
CASE WHEN LN_tdpur401.ddtb < GETDATE() THEN GETDATE() ELSE LN_tdpur401.ddtb END AS DeliveryDate,
LN_tdpur401.oamt as Amount,
LN_tdpur400.ccur as Currency,
LN_tdpur400.hdst as Status
FROM LN_tdpur401
INNER JOIN LN_tdpur400 ON LN_tdpur401.orno = LN_tdpur400.orno
INNER JOIN LN_tcibd001 ON LN_tdpur401.item = LN_tcibd001.item 
LEFT JOIN LN_tcibd003 ON LN_tdpur401.item = LN_tcibd003.item AND LN_tdpur401.cuqp = LN_tcibd003.unit
WHERE LN_tdpur401.item IN (SELECT LN_ticst001.sitm
FROM LN_ticst001
WHERE LN_ticst001.pdno IN (SELECT pdno FROM LN_tisfc010 WHERE opst <= 5) AND LN_ticst001.ques - LN_ticst001.qucs > 0.01)
AND LN_tdpur401.qoor > LN_tdpur401.qidl AND LN_tdpur400.hdst < 25
`;
module.exports = purchaseQuery;
