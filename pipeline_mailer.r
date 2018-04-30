## Gets pipeline apps and splits into files for Mailing List
## Change dates below in pipe query

mailing <- dbGetQuery(panda,"select f.id AS id
, first_name
           , last_name
           , email
           , a1.street AS mail_add_street
           , a1.unit_number AS mail_add_unit
           , a1.city AS mail_add_city
           , a1.state AS mail_add_st
           , a1.zip AS mail_add_zip
           , a1.county AS mail_add_county
           , a1.user_entered_address AS mail_add_user_entered
           FROM public.financing_applications f
           LEFT JOIN public.properties p ON (f.property_id = p.id)
           LEFT JOIN public.addresses a ON (p.address_id = a.id)
           LEFT JOIN public.contacts c ON (f.applicant_id = c.id)
           LEFT JOIN public.addresses a1 on (c.mailing_address_id = a1.id)")


sponsor <- dbGetQuery(dw, "SELECT financing_application_id AS id, sponsor FROM reporting_tableau.application_sponsor_lookup")

sponsor$id <- as.integer(sponsor$id)

pipe <- dbGetQuery(dw, "SELECT financing_application_id as id
FROM v_current_application_dimension cad
                       WHERE financing_documents_generated_at IS NOT NULL
                       AND status NOT IN ('funded','withdrawn', 'declined')
                       AND contractor_company_name NOT LIKE '%Test%'")


mailing_pipe <- sqldf("select mailing.* from mailing join pipe on mailing.id = pipe.id")
mail_spon_pipe <- sqldf("select mailing_pipe.*, sponsor.sponsor, 'Home Owner' as company_name, 'Customer' as lead_source from mailing_pipe join sponsor on mailing_pipe.id = sponsor.id")


CSCDA <- sqldf("select * from mail_spon_pipe where sponsor like '%CSCDA%'")
FGFA <- sqldf("select * from mail_spon_pipe where sponsor like '%FGFA%'")
LA <- sqldf("select * from mail_spon_pipe where sponsor like '%LA_ISD%'")
WRCOG <- sqldf("select * from mail_spon_pipe where sponsor like '%WRCOG%'")


write.table(CSCDA, file = paste("C:/Users/lscadden/Documents/mailer/CSCDA_",Sys.Date(),"_",format(Sys.time(),"%H%M"),".txt",sep = ""), sep = "\t", quote = FALSE, row.names = FALSE)
write.table(FGFA, file = paste("C:/Users/lscadden/Documents/mailer/FGFA_",Sys.Date(),"_",format(Sys.time(),"%H%M"),".txt",sep = ""), sep = "\t", quote = FALSE, row.names = FALSE)
write.table(LA, file = paste("C:/Users/lscadden/Documents/mailer/LA_",Sys.Date(),"_",format(Sys.time(),"%H%M"),".txt",sep = ""), sep = "\t", quote = FALSE, row.names = FALSE)
write.table(WRCOG, file = paste("C:/Users/lscadden/Documents/mailer/WRCOG_",Sys.Date(),"_",format(Sys.time(),"%H%M"),".txt",sep = ""), sep = "\t", quote = FALSE, row.names = FALSE)

box_auth(client_id = "xxxxxxxxxx", client_secret = "xxxxx", interactive = FALSE)
box_push(dir_id = xxxxxxxx, local_dir = "C:/Users/lscadden/Documents/mailer", overwrite = TRUE)


# WRITE TABLE OPTIONS
# 
# write.table(x, file = "", append = FALSE, quote = TRUE, sep = " ", 
#             eol = "\n", na = "NA", dec = ".", row.names = TRUE, 
#             col.names = TRUE, qmethod = c("escape", "double")) 


# write.table(CSCDA, file = "CSCDA.txt", sep = "|")
# write.table(FGFA, file = "FGFA.txt", sep = "|")
# write.table(LA, file = "LA.txt", sep = "|")
# write.table(WRCOG, file = "WRCOG.txt", sep = "|")