
gen_terminated_fct <- function(con,client_id,product_id,last_id,
                               flag_limit_offer){

  
  
#########################################
####### Define reading parameters #######
#########################################
  
# Defines the directory where custom .env file is located
load_dot_env(file = here('.env'))
  
  
# Defines the directory where the RScript is located
base_dir <- Sys.getenv("RSCRIPTS_PATH", unset = "", names = FALSE)  



#################################
####### Load source files #######
#################################

# Load other r files
source(file.path(base_dir,"Additional_Restrictions.r"))
source(file.path(base_dir,"Create_Bins.r"))
source(file.path(base_dir,"Cutoffs.r"))
source(file.path(base_dir,"Generate_Scoring_Table.r"))
source(file.path(base_dir,"Join_Bank_Report_Application.r"))
source(file.path(base_dir,"Read_Bank_Aggs.r"))
source(file.path(base_dir,"SQL_Queries.r"))
source(file.path(base_dir,"Useful_Functions.r"))
source(file.path(base_dir,"Generate_Behavioral_Criteria.r"))
  


########################
####### Settings #######
########################

# Load predefined libraries
rdata <- file.path(base_dir, "rdata","beh_application_model.rdata")
load(rdata)


  
####################################
### Read database and build data ###
####################################
  
# Get client id 
clients <- client_id


# Get last application_id of terminated or active credit		
loan_id <- last_id


# Read credits applications
all_df <- get_correct_zip(gen_query(con,gen_app_sql_query(db_name,loan_id)))
all_df$created_at<-force_tz(as.POSIXct(all_df$created_at),tz="Europe/Madrid")


# Apply some checks to main credit dataframe
if(!is.na(product_id)){
  all_df$product_id <- product_id
}
if(nrow(all_df)>1){
  all_df <- all_df[!duplicated(all_df$application_id),]
}


# Read product's periods and amounts
products <- gen_query(con,gen_products_query(db_name,all_df))


# Get closets to product amount and installments 
all_df$installments <- products$installments[
  which.min(abs(products$installments - all_df$installments))]
all_df$amount <- products$amount[
  which.min(abs(products$amount - all_df$amount))]


# Check all credits of client
all_credits <- gen_query(con,gen_all_credits_query(db_name,all_df$client_id))
if(nrow(all_credits)>0){
  all_credits <- all_credits[which(all_credits$loan_id==loan_id | 
    (!is.na(all_credits$activated_at) & 
    all_credits$activated_at<=all_df$created_at)),]
}

all_df$has_prev_apps<-ifelse(nrow(all_credits)>1,1,0)

# Read all previous active or terminated credits of client
all_id <- all_credits[which(all_credits$loan_id==loan_id | 
                              all_credits$status %in% c(8,9,13,16,17)),]


# Compute flag repeats
flag_beh <- ifelse(nrow(all_id)>0,1,0)
max_amount<-0
if(flag_beh==1){
  max_amount<-max(all_id$amount)
}


# Read Unnax bank aggregations report
all_bank_reports <- gen_query(con,gen_bank_report_query(db_name,
                                                        all_df$client_id))
bank_report <- bank_criteria_JSON_reader(all_bank_reports)


# Work on variables and create scoring bins
bank_report<-suppressWarnings(take_relevant_fields(bank_report,flag_beh))
bank_report<-suppressWarnings(create_bins(bank_report,flag_beh))

# demographic criteria
if(is.na(bank_report[1])){
  all_df<-gen_demographic_criteria(all_df,flag_beh,all_df$birth_date)
} else if("aggs_birth_date" %in% names(bank_report)){
  if(!is.na(bank_report$aggs_birth_date)){
    all_df<-gen_demographic_criteria(all_df,flag_beh,bank_report$aggs_birth_date)
  } else {
    all_df<-gen_demographic_criteria(all_df,flag_beh,all_df$birth_date)
  }
} else {
  all_df<-gen_demographic_criteria(all_df,flag_beh,all_df$birth_date)
}
# bank report criteria
all_df<-suppressWarnings(application_wtih_aggs_data(all_df,bank_report))

# behavioral criteria
if(flag_beh==1){
  prev_loans <- all_id
  prev_loans <- prev_loans[order(prev_loans$loan_id,decreasing = TRUE),]
  prev_loans <- prev_loans[1,]
  prev_loan_id <- prev_loans$loan_id
  prev_df <- gen_query(con,gen_prev_loan_df(db_name,prev_loan_id))
  prev_df <- gen_revolve_data(prev_df)
  prev_loans$loan_id <- -999
  all_id_here <- rbind(all_id,prev_loans)
  all_df<-gen_behavioral_criteria(all_df,all_id_here,prev_df)
} else {
  all_df<-gen_app_behavior(all_df)
}


############################################################
################# Apply model coefficients #################
############################################################

scoring_df <- suppressWarnings(gen_scoring_df(products,loan_id,all_df))

# Build column PD
if(!("pd" %in% names(scoring_df))){
  scoring_df$pd <- NA
}

#calculate PD, group and colour
scoring_df<- suppressMessages(gen_score_app(all_df,scoring_df,flag_beh,
      beh_application_model,first_application_model))


# stop("Break: line 170")
######################################
### Generate final output settings ###
######################################

# Generate equifax report
equifax_report <- gen_query(con,gen_equifax_report(db_name,all_df$client_id))
all_df<-merge_equifax_report(all_df,equifax_report)


# Generate scoring dataframe
scoring_df$created_at <- Sys.time()
scoring_df <- scoring_df[,c("application_id","amount","period","score",
     "color","pd","created_at")]

## Readjust scoring table by applying policy rules
#create flags
all_df<-gen_age_flag(all_df)
all_df<-gen_transactions_flag(all_df)
all_df<-suppressWarnings(gen_debt_restructuring_flag(all_df,
   all_df$aggregations_id,all_bank_reports))
all_df<-gen_equifax_flag(all_df)
all_df<-gen_affiliate_flag(all_df)


scoring_df<-merge(scoring_df,all_df[,c("loan_id","age_flag","transactions_flag",
    "debt_restructuring_flag","no_aggregations_flag",
    "equifax_flag","affiliate_flag")],
    by.x = "application_id", by.y = "loan_id")
#apply policy rules
scoring_df <- policy_rules(scoring_df,flag_beh,max_amount)


# Create column for table display
scoring_df <- gen_final_table_display(scoring_df,all_df$amount)


# Subset scoring dataframe according to criteria
correct_scoring_df <- subset(scoring_df,scoring_df$color!=1 &
      scoring_df$score %in% c("Indeterminate","Good 1",
                              "Good 2","Good 3","Good 4"))


# Get highest amount
get_max_amount <- suppressWarnings(max(correct_scoring_df$amount))


# Get maximum installment
scoring_df <- merge(scoring_df,products[,c("amount","installments",
   "installment_amount")],by.x = c("amount","installments"),
   by.y = c("amount","installments"),all.x = TRUE)
if(is.infinite(get_max_amount)){
  get_max_installment <- -Inf	
} else {	
  get_max_installment <- max(scoring_df$installment_amount[
    scoring_df$color!=1 & scoring_df$amount==get_max_amount])	
}


# Get score of highest amount
if(get_max_amount>-Inf){
  sub <- subset(scoring_df,scoring_df$color!=1 &
                  scoring_df$installment_amount==get_max_installment & 
                  scoring_df$amount==get_max_amount)
  get_score <- 
    ifelse(nrow(subset(sub,sub$score=="Good 4"))>0,
      "Good 4",
    ifelse(nrow(subset(sub,sub$score=="Good 3"))>0,
      "Good 3",
    ifelse(nrow(subset(sub,sub$score=="Good 2"))>0,
      "Good 2",
    ifelse(nrow(subset(sub,sub$score=="Good 1"))>0,
     "Good 1",
    ifelse(nrow(subset(sub,sub$score=="Indeterminate"))>0,
    "Indeterminate",
     NA)))))
} else {
  get_score <- NA
}


# Make final list and return result
final_list <- list(get_max_amount,get_max_installment,get_score,
  max(suppressWarnings(fetch(dbSendQuery(con,
  gen_days_delay(db_name,paste(all_id$loan_id,collapse=","))),n=-1))$max_delay))
return(final_list)

}
