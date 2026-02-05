const openSalesQuery = `SELECT 
    LN_tisfc001.pdno AS OrderNo, 
    ISNULL(LN_tisfc010.opno, 10) AS OpNo,
    LN_tisfc010.refo,
    LN_tisfc010.opst AS Status,
    LN_tisfc001.cprj AS Project, 
    CASE
        WHEN LN_tisfc001.cdld = '1970-01-01 00:00:00.000' THEN LN_tisfc001.rdld
        ELSE LN_tisfc001.cdld
    END AS DueDate,
    SUBSTRING(LN_tisfc001.mitm, 10, 40) AS Drawing,
    LTRIM(LN_tisfc001.mitm) as PartNo,
    -- Changed: Using qoor directly from the production order table
    LN_tisfc001.qoor AS OrderedQuantity, 
    ibd_main.dsca_bg_BG as Product,
    -- Using MAX on sales ref if the link works, else NULL
    MAX(LN_tdsls401.corn_bg_BG) AS corn_bg_BG,
    ibd_drawing_004.aitc_bg_BG
FROM 
    LN_tisfc001
LEFT JOIN 
    LN_tisfc010 ON LN_tisfc001.pdno = LN_tisfc010.pdno
LEFT JOIN 
    LN_tcibd001 AS ibd_main ON LTRIM(RTRIM(LN_tisfc001.mitm)) = LTRIM(RTRIM(ibd_main.item))
LEFT JOIN 
    LN_tcibd001 AS ibd_drawing ON TRIM(ibd_drawing.item) = TRIM(SUBSTRING(LN_tisfc001.mitm, 10, 40)) 
LEFT JOIN 
    LN_tcibd004 AS ibd_drawing_004 ON ibd_drawing.item = ibd_drawing_004.item
-- Trying to link Sales via reco (Reference Order) since cprj is empty
LEFT JOIN 
    LN_tdsls401 ON LTRIM(RTRIM(LN_tisfc001.reco)) = LTRIM(RTRIM(LN_tdsls401.orno))
LEFT JOIN 
    LN_tdsls400 ON LTRIM(RTRIM(LN_tisfc001.reco)) = LTRIM(RTRIM(LN_tdsls400.orno))
WHERE 
    LN_tisfc010.opst < 7
GROUP BY
    LN_tisfc001.pdno,
    LN_tisfc001.cprj,
    LN_tisfc001.mitm,
    LN_tisfc001.cdld,
    LN_tisfc001.rdld,
    LN_tisfc010.opno,
    LN_tisfc010.opst,
    LN_tisfc001.qoor,
    ibd_main.dsca_bg_BG,
    ibd_drawing_004.aitc_bg_BG,
    LN_tisfc010.refo`;

module.exports = openSalesQuery;
