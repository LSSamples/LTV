### MultiLien Automation

##Box Auth + Get List of apps that are good:
{
    box_auth(client_id = "XXXX", client_secret = "XXXX", interactive = FALSE)
    box_fetch(dir_id = 39873501047, local_dir = "C:/Users/lscadden/Desktop/Box", overwrite = TRUE )
    exclusions <- read.xlsx("C:/Users/lscadden/Desktop/Box/ML_exclusions.xlsx")
    exclusions <- sqldf("select app_id, 'exlude' as exclude from exclusions")
    atp_exclusions <- read.xlsx("C:/Users/lscadden/Desktop/Box/ATP_exclusions.xlsx")
    atp_exclusions <- sqldf("select app_id, 'exlude' as exclude from atp_exclusions")
}

###
# Queries

# Get Funded Apps
{
    funded <- dbGetQuery(dw,"select prop_apn as apn, 
                         prop_address as funded_address, 
                         prop_state as funded_state, 
                         avg(mortgage_bal)::int as funded_mort_bal, 
                         avg(prop_val)::int as funded_prop_val, 
                         sum(assessment_amt)::int as funded_assessment_val, 
                         case when sum(bond_principal_pmt_amt) is null then 0 else sum(bond_principal_pmt_amt) end as prepay 
                         from mkeeton.assessments a
                         left join lscadden.prepayments p on a.app_id = p.app_id
                         group by 1,2,3")
}


# Get Pipeline Addresses
{
    pipe_addresses <- dbGetQuery(panda,"select f.id,
                                 a.street as prop_add_street,
                                 a.state as prop_add_st
                                 FROM public.financing_applications f
                                 LEFT JOIN public.properties p ON (f.property_id = p.id)
                                 LEFT JOIN public.addresses a ON (p.address_id = a.id)
                                 LEFT JOIN public.contacts c ON (f.applicant_id = c.id)
                                 LEFT JOIN public.addresses a1 on (c.mailing_address_id = a1.id)")
}

# Get Pipeline Apps
{
    inpipe <- dbGetQuery(dw,"SELECT distinct financing_application_id as pipe_app, valid_since, status
                         FROM public.v_current_application_dimension
                         where status not in ('funded','withdrawn','declined')
                         " )
}

# Get APNs from Lenny 
{
    allapn <- dbGetQuery(lennyUW, "select financing_application_id as finapp
                         , max(assessors_parcel_identifier) as apn
                         from public.decision_requests dr
                         inner join decision_properties dp on dr.decision_property_id = dp.id
                         inner join instant_legal_and_vesting_reports lnv on lnv.id = dp.instant_legal_and_vesting_report_id
                         where 1=1 
                         and assessors_parcel_identifier not like '%error%'
                         and assessors_parcel_identifier is not null 
                         group by 1")
}


## Get pipe Valuation Data QRY
{
    dw_valdata <- dbGetQuery(dw,"with updt as (
                             select ad.financing_application_id
                             , current_value
                             , property_value
                             , updated_at
                             , rank() over (partition by financing_application_id order by updated_at desc) as rnk
                             from public.application_value_fact avf
                             join public.application_dimension ad on ad.id = avf.application_dimension_id
                             where status_collapsed not in ('withdrawn','declined')
                             and (initial_improvement_value + final_improvement_value + initial_assessment_amount + final_assessment_amount) > .01
                             group by 1,2,3,4
    ) 
                             select financing_application_id, current_value, property_value 
                             from updt
                             where rnk = 1")
}

## Get multi apn / app ids from funded QRY
{
    apn_apps <-dbGetQuery(dw,"select * from apn_apps")
}

## Get current mortgage balance from Panda QRY
{
    curr_bal <-dbGetQuery(panda,"
                          select appid, max(mortbal)from (
                          select financing_application_id as appid
                          , cast(response as JSON) #>>'{reports,current_mortgage_balance}' as mortbal, max(updated_at)
                          from public.auto_decisions
                          where financing_application_id in (select id from public.financing_applications where status not in ('declined','withdrawn','funded'))
                          group by 1, 2) c
                          group by 1")
    
}

## Merge pipeline and funded
{
    inpipe <- sqldf("select inpipe.*, allapn.apn from inpipe join allapn on inpipe.pipe_app = allapn.finapp where allapn.apn is not null")
    
    multilien_apns <- merge(inpipe,funded, by.x = "apn", by.y = "apn")
    
    final_ml <- sqldf("select ml.apn
                      , ml.pipe_app
                      , ml.valid_since as pipe_date
                      , ml.status as pipe_status
                      , dv.current_value as pipe_assessment
                      , cb.max as pipe_mort_val
                      , dv.property_value as pipe_prop_value
                      , pa.prop_add_street as pipe_address      
                      , pa.prop_add_st as pipe_state
                      , ml.funded_mort_bal
                      , ml.funded_prop_val
                      , ml.funded_assessment_val
                      , ml.funded_address
                      , ml.funded_state
                      , ml.prepay
                      , ((dv.current_value + ml.funded_assessment_val - ml.prepay)/dv.property_value) as LTV
                      , ((dv.current_value + ml.funded_assessment_val + (case when cb.max > 0.01 then cb.max else funded_mort_bal end) 
                      - ml.prepay)/(case when dv.property_value > 0.01 then dv.property_value else ml.funded_prop_val end)) as CLTV
                      , aa.app_ids as funded_apps
                      , e.exclude
                      from multilien_apns ml
                      join dw_valdata dv on ml.pipe_app = dv.financing_application_id
                      join apn_apps aa on aa.prop_apn = ml.apn
                      left join curr_bal cb on cb.appid = ml.pipe_app
                      left join pipe_addresses pa on pa.id = ml.pipe_app
                      left join exclusions e on e.app_id = ml.pipe_app
                      where trim(ml.pipe_app) <> trim(aa.app_ids)")
    
    
    all_pipe_breach <- sqldf("select cb.appid
                             , ip.status
                             , dw.current_value as assesment_value
                             , cb.max as mortgage_bal
                             , dw.property_value as prop_value 
                             , pa.prop_add_street as street
                             , pa.prop_add_st as state
                             , dw.current_value/dw.property_value as LTV
                             , cb.max/dw.property_value as MLTV
                             , (dw.current_value + cb.max)/dw.property_value as CLTV
                             from curr_bal cb 
                             join dw_valdata dw on cb.appid = dw.financing_application_id
                             left join pipe_addresses pa on pa.id = cb.appid
                             left join inpipe ip on ip.pipe_app = cb.appid")
    
    LTV_1 <- sqldf("select *, 
                   case when state like '%FL%' and LTV > .20000 then 'FL LTV'
                   when state like '%CA%' and LTV > .15000 then 'CA LTV'
                   when CLTV > 1.00000 then 'CLTV'
                   when mortgage_bal < .100 then 'mort bal'
                   when prop_value < 10000 then 'prop value'
                   when MLTV > .95000 then 'MLTV'
                   when prop_value is null or mortgage_bal is null then 'NULL VALUES'
                   else 'None'
                   end as reason
                   from all_pipe_breach where 1=1")
    
    LTV_Exceptions <- sqldf("select * from LTV_1 where reason not like '%None%' order by reason")
    
## BaseDate Stuff
    Base_Date <-dbGetQuery(panda,"SELECT fa.id, status, fa.funded_at::date 
       ,fa.projected_funding_at::date
       ,substring(split_part(repayment_estimate::text, 'base_date', 2),4,10)::date AS base_date
        FROM financing_applications fa
        JOIN document_sets ds ON ds.financing_application_id = fa.id
        JOIN financing_offers fo ON fo.id = ds.financing_offer_id
        WHERE ds.name = 'funding_documents' AND ds.archived is false
        AND projected_funding_at IS NOT NULL
        AND status IN ('pending_memo_finance_agreement','ready_for_recording','pending_recording','pending_funding','funded') 
        AND substring(split_part(repayment_estimate::text, 'base_date', 2),4,10)::date != fa.projected_funding_at::date
        AND projected_funding_at > '2018-01-01'")
    
## ATP Query
    atp <-dbGetQuery(dw,"SELECT financing_application_id, created_at, status, status_collapsed, financing_documents_signed_at
                         FROM public.v_current_application_dimension
                         WHERE status not in ('withdrawn','declined')
                         AND created_at >= '2018-03-29'
                         AND state like '%CA%'")
    
    final_atp <- sqldf("select * from atp
                      left join atp_exclusions e on e.app_id = atp.financing_application_id")
    
    
}

Filepath <- paste("C:/Users/lscadden/Desktop/BOX/MLs_LTVs_",Sys.Date(),".xlsx",sep = "")
## File Writing, etc

## dbWriteTable(dw, "final_ml", final_ml, overwrite = TRUE)
## Zip path needed to zip excel file
Sys.setenv("R_ZIPCMD" = "C:/Program Files/Rtools/bin/zip.exe")

# Write final_ml, all_pipe_breach, LTV_Exceptions to xlsx
wb <- createWorkbook()
addWorksheet(wb, "Multiliens")
addWorksheet(wb, "LTV_Exceptions")
addWorksheet(wb, "All_pipe")
addWorksheet(wb, "Base_Date")
addWorksheet(wb, "ATP")
writeData(wb, "Multiliens", final_ml) 
writeData(wb, "LTV_Exceptions", LTV_Exceptions)
writeData(wb, "All_pipe", all_pipe_breach)
writeData(wb, "Base_Date", Base_Date)
writeData(wb, "ATP", final_atp)
saveWorkbook(wb,Filepath, overwrite = TRUE)
rm(wb)


box_push(dir_id = XXXX, local_dir = "C:/Users/lscadden/Desktop/BOX", overwrite = TRUE)