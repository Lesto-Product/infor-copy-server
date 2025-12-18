const purchaseInvoiceQuery = `SELECT 
  idoc, -- FIX: Changed from 'idoc as inv_number' to just 'idoc'
  iamt as amount,
  icur as currency,
  ityp as inv_type,
  inv.item as item, 
  item.dsca_bg_BG as item_desc,
  inv.orno as order_num,
  iqan as quantity,
  data as approval_date,
  apry as fiscal_year,
  bp.bpid as bp_id,
  bp.nama_bg_BG as bp_name
FROM 
  LN_tfacp256 inv
LEFT JOIN 
  LN_tcibd001 item on item.item = inv.item 
LEFT JOIN 
  LN_tdpur400 pur on pur.orno = inv.orno
LEFT JOIN 
  LN_tccom100 bp on bp.bpid = pur.otbp
WHERE 
  ityp='F02' AND apry > 2020`;

module.exports = purchaseInvoiceQuery;
