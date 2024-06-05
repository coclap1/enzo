with all_data as (
  SELECT
    date_format(inputs.`date`, "yyyy-MM") AS date_month,
    market,
    COUNT(*) as nb_jobs
  FROM
    sandbox.prod_enzo.care_high_prio AS inputs
    LEFT JOIN silver.prod_offers.enriched_offers AS offers ON inputs.job_id = offers.id
  GROUP BY
    offers.market,
    date_month
),
tri as (select * except(two.date_month,three.date_month,four.date_month,four.no_market),case when four.no_market is null then 0 else four.no_market end as no_markets, one.HSP_FR+two.HLC_FR+three.HSP_UK+no_markets as total, date(one.date_month)
from
  (select smb.date_month, smb_jobs+Catering_jobs as HSP_FR
  from (select date_month, nb_jobs as smb_jobs from all_data where market = "1. HOSP SMB FRA") as SMB join (select date_month, nb_jobs as Catering_jobs from all_data where market = "2. HOSP Catering FRA") as Catering on SMB.date_month=Catering.date_month
  order by SMB.date_month) as one
  left join (select date_month, nb_jobs as HLC_FR from all_data where market = "4. CARE FRA") as two on one.date_month=two.date_month
  left join (select date_month, nb_jobs as HSP_UK from all_data where market = "3. HOSP UK") as three on one.date_month=three.date_month
  left join (select date_month, nb_jobs as no_market from all_data where market is null) as four on one.date_month=four.date_month

order by one.date_month)

(select * from tri)
