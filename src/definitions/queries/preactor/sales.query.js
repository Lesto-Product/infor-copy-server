const salesQuery = `SELECT LN_tdsls400.orno AS OrderNo, 
LN_tdsls401.pono AS PoNo, 
LN_tccom100.nama_bg_BG AS Customer, 
LN_tdsls400.ddat as SalesDueDate,
LN_tdsls401.ddta AS DueDate, 
LTRIM(LN_tdsls401.item) AS PartNo, 
LN_tdsls401.qoor - LN_tdsls401.qidl AS Quantity, 
Inv.Quantity AS Stock,
LN_tcibd001.dsca_bg_BG AS Product,
LN_tdsls400.hdst as Status,
LN_tdsls400.ccur as Currency,
LN_tdsls401.pric as Amount
FROM LN_tdsls400
INNER JOIN LN_tccom100 ON LN_tdsls400.ofbp = LN_tccom100.bpid
INNER JOIN (
  SELECT orno, pono, item, MIN(ddta) AS ddta, MIN(qoor) AS qoor, SUM(qidl) AS qidl , pric
  FROM LN_tdsls401 
  GROUP BY orno, pono, item,pric) LN_tdsls401 ON LN_tdsls400.orno = LN_tdsls401.orno
INNER JOIN LN_tcibd001 ON LN_tdsls401.item = LN_tcibd001.item
LEFT JOIN (
  SELECT item, SUM(qhnd) AS Quantity 
  FROM LN_whinr140 
  WHERE cwar = '1' AND LEFT(item, 1) <> '' 
  GROUP BY item) Inv ON LN_tdsls401.item = Inv.item
WHERE LN_tdsls400.hdst < 30 AND LN_tdsls401.qoor > LN_tdsls401.qidl AND LTRIM(LN_tdsls401.item) NOT LIKE '3%' AND LTRIM(LN_tdsls401.item) NOT LIKE '6%'`;

module.exports = salesQuery;
