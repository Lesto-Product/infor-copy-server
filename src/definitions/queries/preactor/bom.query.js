const bomQuery = `SELECT LN_ticst001.pdno AS OrderNo, 
LTRIM(LN_tisfc001.mitm) AS MainItem, 
LTRIM(LN_ticst001.sitm) AS RequiredItem,
LN_tcibd001.dsca_bg_BG AS RequiredMaterial, 
CASE WHEN LN_ticst001.opno = 0 THEN 10 ELSE LN_ticst001.opno END AS OpNo, 
LN_ticst001.ques - LN_ticst001.qucs AS RequiredQuantity,
LN_tcibd001.cdf_adal_bg_BG AS AdalCode,
LN_tcibd001.cdf_quad AS ItemSquare
FROM LN_ticst001
INNER JOIN LN_tisfc001 ON LN_ticst001.pdno = LN_tisfc001.pdno
INNER JOIN LN_tcibd001 ON LN_tisfc001.mitm = LN_tcibd001.item
WHERE LN_tisfc001.osta <= 70 AND LN_ticst001.ques - LN_ticst001.qucs > 0.01 AND  LTRIM(LN_tisfc001.mitm) <> LTRIM(LN_ticst001.sitm)
UNION ALL
SELECT LN_cprrp010.orno AS OrderNo, LTRIM(LN_cprrp010.mitm) AS MainItem, LTRIM(LN_cprrp010.sitm) AS RequiredItem, LN_tcibd001.dsca_bg_BG AS RequiredMaterial,
LN_cprrp010.opno AS OpNo, LN_cprrp010.qana AS RequiredQuantity,LN_tcibd001.cdf_adal AS AdalCode, LN_tcibd001.cdf_quad AS ItemSquare
FROM LN_cprrp010
INNER JOIN LN_tcibd001 ON LTRIM(LN_cprrp010.sitm) = LTRIM(LN_tcibd001.item)
WHERE LN_cprrp010.kotr = 2`;

module.exports = bomQuery;
