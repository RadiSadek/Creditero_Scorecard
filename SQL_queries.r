##################################
######## Define SQL queries ######
##################################

# Define query which reads from loans
gen_app_sql_query <- function(db_name,application_id){
  applications_sql_query <- paste("SELECT 
    ",db_name,".loans.id AS loan_id,
    ",db_name,".clients.id AS client_id,
    ",db_name,".loans.product_id,
    ",db_name,".loans.requested_amount,
    ",db_name,".loans.amount,
    ",db_name,".loans.installments,
    ",db_name,".loans.days_period,
    ",db_name,".loans.created_at,
    ",db_name,".clients.email,
    ",db_name,".clients.dni,
    ",db_name,".client_additional_data.birth_date,
    ",db_name,".client_additional_data.gender,
    ",db_name,".client_additional_data.marital_status,
    ",db_name,".client_additional_data.education,
    ",db_name,".client_additional_data.ownership,
    ",db_name,".client_additional_data.salary,
    ",db_name,".client_additional_data.work_experience,
    ",db_name,".settlements.zip_code,
    ",db_name,".loan_referrals.source,
    ",db_name,".addresses.type as address_type
    FROM ",db_name,".loans
    LEFT JOIN ",db_name,".clients
    ON ",db_name,".loans.client_id = ",db_name,".clients.id
    LEFT JOIN ",db_name,".client_additional_data
    ON ",db_name,".client_additional_data.client_id=",db_name,".clients.id
    LEFT JOIN ",db_name,".addresses
    ON ",db_name,".addresses.addressable_id=",db_name,".clients.id
    LEFT JOIN ",db_name,".settlements
    ON ",db_name,".settlements.id=",db_name,".addresses.settlement_id
    LEFT JOIN ",db_name,".loan_referrals
    ON ",db_name,".loan_referrals.loan_id=",db_name,".loans.id
    WHERE
	",db_name,".addresses.addressable_type = 'App\\\\Models\\\\Clients\\\\Client'
	AND loans.id = ",application_id,";", sep=""
  )
  return(applications_sql_query)
}

gen_bank_report_query <- function(db_name,client_id){
  bank_report_query<-paste(
    "SELECT 
    ",db_name,".unnax_bank_aggregations.*, 
    ",db_name,".clients.dni
    FROM ",db_name,".unnax_bank_aggregations
    LEFT JOIN ",db_name,".clients
    ON ",db_name,".unnax_bank_aggregations.client_id=
    ",db_name,".clients.id
    WHERE clients.id = ",client_id,"
    ORDER BY id DESC;", sep=""
  )
  return(bank_report_query)
}

# Define query for getting all credits for client 
gen_all_credits_query <- function(db_name,client_id){
  all_credits_query<-paste(
    "SELECT id AS loan_id, client_id, status, created_at, 
    activated_at, finished_at AS deactivated_at, requested_amount, 
    amount, installments FROM ",db_name,".loans 
    WHERE status <> 5 
    AND client_id=",client_id,";", sep ="")
  return(all_credits_query)
}

# Define query to get the maximum of delay days from previous credits
gen_days_delay <- function(db_name,list_ids_max_delay){
  return(paste(
      "SELECT loan_id, MAX(days_delay) AS max_delay 
      FROM ",db_name,".loan_repayment_schedule
      WHERE loan_id in(",list_ids_max_delay,")
      GROUP BY loan_id;", sep=""
    )
  )
}

# Define query for products periods and amounts
gen_products_query <- function(db_name,application_df){
  return(paste(
          "SELECT * FROM ", db_name, ".product_amounts_and_installments
           WHERE product_id =",application_df$product_id, ";", 
          sep="")
  )
}

# # counter
# update_counter <- function(db_name,application_id){
#   return(paste(
#   "UPDATE ",db_name,".loan_details
# 	  SET score_edit_counter = 
#     IF(score_edit_counter IS NULL,1,score_edit_counter+1)
#   WHERE loan_id=",application_id,";",
#     sep="")
#   )
# }

# Generate equifax debt
gen_equifax_report <- function(db_name, client_id){
  equifax_query<-paste(
    "SELECT equifax_reports.*
    FROM ",db_name,".equifax_reports
    WHERE client_id = ",client_id,"
    ORDER BY id DESC LIMIT 1;", sep=""
  )
  return(equifax_query)
}

# Generate previous loan
gen_prev_loan_df <- function(db_name,loan_id){
  prev_loan_query<-paste(
    "SELECT loans.id,loans.activated_at, loans.finished_at, loans.days_period,
    SUM(loan_repayment_schedule.days_delay) AS total_delay,
    MAX(loan_repayment_schedule.days_delay) AS max_delay
    FROM ",db_name,".loans
    INNER JOIN ",db_name,".loan_repayment_schedule
    ON loan_repayment_schedule.loan_id=loans.id
    WHERE loans.id = ",loan_id,";", sep=""
  )
  return(prev_loan_query)
}

gen_taxes_df_query <- function(db_name,loan_id){
  taxes_df_query<-paste(
    "SELECT id, loan_id, type, amount, 
    paid_amount, paid_at,
    created_at, deleted_at
    FROM ",db_name,".loan_fees
    WHERE loan_fees.loan_id=",loan_id,";", sep="")
  return(taxes_df_query)
}

gen_pre_score_raw_sql_query <- function(db_name,id,type){
  if(type=="client"){
    pre_score_raw_df<-paste(
      "SELECT 
      client_prescore.client_id,
      client_prescore.age_type,
      client_prescore.gender,
      client_prescore.salary_type,
      client_prescore.work_experience_type 
      FROM ", db_name, ".client_prescore
      WHERE client_prescore.client_id=",id,";", sep="")
  } else if(type=="affiliate"){
    pre_score_raw_df<-paste(
      "SELECT affiliate_loan_requests.id AS request_id,
      CAST(affiliate_loan_requests.payload AS char) AS payload,
      affiliate_loan_requests.prescore_priority AS priority,
      affiliate_loan_requests.prescore_pd AS pd
      FROM ", db_name, ".affiliate_loan_requests
      WHERE affiliate_loan_requests.id='",id,"';", sep="")
  }
  return(pre_score_raw_df)
}

gen_old_scores_query <- function(db_name,loan_ids){
  if(length(loan_ids)>0){
    return(
      paste("SELECT loan_id, score, pd
            FROM loan_scoring
            WHERE loan_id IN (",
            paste(loan_ids,collapse=", "),");",sep=""))
  } else {
    return(
      paste("SELECT loan_id, score, pd
            FROM loan_scoring
            WHERE loan_id=-1;",sep=""))
  }
}

gen_old_pre_scores_query<-function(db_name,loan_ids){
  return(paste("SELECT client_id, priority, pd
  FROM client_prescore
  WHERE client_id=",client_id,"
  AND priority IS NOT NULL;"))
}

gen_client_id <- function(db_name,loan_id){
  return(paste(
    "SELECT id AS loan_id,
  client_id
  FROM ",db_name,".loans
  WHERE loans.id=",loan_id,";",sep=""))
}


update_pre_score_query <- function(db_name, id, output_df,type){
  if(nrow(output_df)==1&type=="client"){
    return(
      paste("UPDATE ",db_name,".client_prescore
            SET priority = '",output_df$priority,"',
            pd=",round(output_df$pd,3),"
            WHERE client_id=",client_id, sep="")
    )
  } else if(nrow(output_df)==1&type=="affiliate") {
    return(
      paste("UPDATE ",db_name,".affiliate_loan_requests
            SET prescore_priority = '",output_df$priority,"',
            prescore_pd = ",round(output_df$pd,3),"
            WHERE id = '",id,"';", sep="")
    )
  } else {
    return(NA)
  }
}

update_old_client_pre_score_query <- function(db_name,output_df){
  if(nrow(output_df)==1&all(c("pd","priority")%in%names(output_df))){
    return(
      paste("INSERT INTO ",db_name,".client_prescore (client_id, priority, pd)
            VALUES (",output_df$client_id,",'",output_df$priority,
            "',",round(output_df$pd,3),");",sep="")
    )
  } else {
    return(NA)
  }
}

# gen_po_terminated_query<-function(db_name,client_id){
#   return(paste("SELECT *
#     FROM ",db_name,".clients_prior_approval_applications
#     WHERE clients_prior_approval_applications.client_id=",client_id,";",sep=""))
# }