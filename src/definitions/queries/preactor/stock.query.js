const bomQuery = `SELECT LTRIM(LN_whinr140.item) AS PartNo, 
LN_tcibd001.dsca_bg_BG AS Product, 
SUM(LN_whinr140.qhnd) AS Quantity,
LN_whinr140.cwar as Warehouse
FROM LN_whinr140
INNER JOIN LN_tcibd001 ON LN_whinr140.item = LN_tcibd001.item 
WHERE LN_whinr140.item IN (SELECT LN_ticst001.sitm
FROM LN_ticst001
WHERE LN_ticst001.pdno IN (SELECT pdno FROM LN_tisfc010 WHERE opst <= 50) AND LN_ticst001.ques - LN_ticst001.qucs > 0.01)
AND LN_whinr140.qhnd > 0
GROUP BY LN_whinr140.item, LN_tcibd001.dsca_bg_BG, LN_whinr140.cwar`;

module.exports = bomQuery;
