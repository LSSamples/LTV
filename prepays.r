# Prepayments Link
PrepayURL <- "https://docs.google.com/spreadsheets/d/XXXXXXXXXXXX"

# Get Sheet Info
prepays <- gs_url(PrepayURL, lookup =FALSE, visibility = "private")


# Get sheet 2 data
data <- gs_read(prepays, 2)
dbSendStatement(dw, "TRUNCATE prepay_all")
dbWriteTable(dw, "prepay_all", data, append = TRUE)

# Write a backup to Box.com
write.table(data, file = paste("C:/Users/lscadden/Documents/PP_Archive/Prepayment_gsheet_",Sys.Date(),".csv",sep = ""), col.names=TRUE, row.names=FALSE, sep=",", append=FALSE)

# Box Push
box_auth(client_id = "xxxx", client_secret = "xxxxxxxx", interactive = FALSE)
box_push(dir_id = xxxxxx, local_dir = "C:/Users/lscadden/Documents/PP_Archive", overwrite = TRUE)


data <- sqldf("SELECT Timestamp added_to_tracker
              ,`Formatted Application ID` as app_id
              ,`Date RF sent Request for Schedule to DTA` as schedule_request_dt
              ,`Date PO/RF received Payoff/Paydown Schedule from DTA` as schedule_receipt_dt
              ,`Date Trustee Confirmed Receipt of Prepayment` as pmt_received_dt
              ,`Prepayment Amount Received` as prepay_amt_received
              ,`Bond Call Date` as bond_prepayment_dt
              ,`Assessment/Bond Principal to be Prepaid` as bond_principal_pmt_amt
              ,`Recording Date` as recording_dt
              ,`Who submitted` as submitted_by
              ,`Type of Prepayment` as prepay_type
              , COALESCE(`Reason for Full Payoff`,`Reason for Partial Prepayment`) as reason
              ,`RF Internal Notes` as notes
              FROM data")

# filter out data without payment
data <- subset(data, pmt_received_dt != "")

# Truncate and re-write data
dbSendStatement(dw, "TRUNCATE prepay_stage")
dbWriteTable(dw, "prepay_stage", data, append = TRUE)