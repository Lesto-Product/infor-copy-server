const invoiceQuery = `SELECT 
  inv.ttyp as inv_type, 
  inv.ninv as inv_number, 
  bp.nama_bg_BG as business_partner, 
  inv.docd as inv_date, 
  inv.ccur as currency, 
  inv.amnt as amount, 
  inv.vata as tax, 
  inv.dued as maturity_date,
  inv.balc as inv_balance, 
  inv.orno as order_num, 
  inv.vaty as fiscal_year
FROM 
  LN_tfacr200 inv
LEFT JOIN 
  LN_tccom100 bp on bp.bpid = inv.itbp
WHERE 
  vaty > 2019
`;

module.exports = invoiceQuery;
