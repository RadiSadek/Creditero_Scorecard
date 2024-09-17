

################################################################################
#               Joint script for Application and Behavioral scoring            #
#         Apply Gradient Boost Model on all products (Creditero Spain)         #
#                          Version 5.0 (2024/01/10)                            #
################################################################################



########################
### Initial settings ###
########################

# Library
suppressMessages(suppressWarnings(require(RMySQL)))
suppressMessages(suppressWarnings(require(here)))
suppressMessages(suppressWarnings(require(dotenv)))
suppressMessages(suppressWarnings(require(reshape)))
suppressMessages(suppressWarnings(require(openxlsx)))
suppressMessages(suppressWarnings(require(jsonlite)))
suppressMessages(suppressWarnings(require(gbm)))
suppressMessages(suppressWarnings(require(lubridate)))


# Defines the directory where custom .env file is located
load_dot_env(file = here('.env'))


#########################
### Command arguments ###
#########################

args <- commandArgs(trailingOnly = TRUE)
loan_id <- as.numeric(args[1])
product_id <- as.numeric(args[2])


#######################
### Manual settings ###
#######################

# Defines the directory where the RScript is located
base_dir <- Sys.getenv("RSCRIPTS_PATH", unset = here(), names = FALSE)



#####################
####### MySQL #######
#####################

db_host <- Sys.getenv("DB_HOST", 
                      unset = "localhost", 
                      names = FALSE)
db_port <- strtoi(Sys.getenv("DB_PORT", 
                             unset = "3306", 
                             names = FALSE))
db_name <- Sys.getenv("DB_DATABASE", 
                      unset = "spain", 
                      names = FALSE)
db_username <- Sys.getenv("DB_USERNAME", 
                          unset = "root", 
                          names = FALSE)
db_password <- Sys.getenv("DB_PASSWORD", 
                          unset = "123456", 
                          names = FALSE)
con <- dbConnect(MySQL(), user=db_username, 
                 password=db_password, dbname=db_name, 
                 host=db_host, port = db_port)
sqlMode <- paste("SET sql_mode=''", sep ="")
suppressWarnings(dbSendQuery(con, sqlMode))


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
#source(file.path(base_dir,"Adjust_Scoring_Prior_Approval.r"))


########################
####### Settings #######
########################

# Load predefined libraries
rdata <- file.path(base_dir, "rdata","first_application_model.rdata")
rdata2 <- file.path(base_dir, "rdata","beh_application_model.rdata")
load(rdata)
load(rdata2)


####################################
### Read database and build data ###
####################################

# Read credits applications
all_df <- gen_query(con,gen_app_sql_query(db_name,loan_id))
all_df <- get_correct_zip(all_df)
all_df$created_at<-force_tz(as.POSIXct(all_df$created_at,tz="Europe/Madrid"))
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
flag_beh <- ifelse(nrow(all_id[all_id$loan_id!=loan_id,])>0,1,0)
max_amount<-0
if(flag_beh==1){
  max_amount<-max(all_id$amount[which(all_id$loan_id!=loan_id)])
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
  prev_loan_id<-select_last_credit(all_id,loan_id)
  prev_df <- gen_query(con,gen_prev_loan_df(db_name,prev_loan_id))
  prev_df <- gen_revolve_data(prev_df)
  all_df<-gen_behavioral_criteria(all_df,all_id,prev_df)
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
scoring_df<-suppressMessages(gen_score_app(all_df,scoring_df,flag_beh,
      beh_application_model,first_application_model))


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
# all_df$flag_bankruptcy <- sapply(all_df$dni, gen_bankruptcy_http_request)

scoring_df<-merge(scoring_df,all_df[c("loan_id","age_flag", "transactions_flag",
              "debt_restructuring_flag","no_aggregations_flag","equifax_flag",
              "affiliate_flag")],
                  by.x = "application_id", by.y = "loan_id")

# apply policy rules
scoring_df <- policy_rules(scoring_df,flag_beh,max_amount)

# Correct for prior approval offers
if(flag_beh==1){
  scoring_df <- gen_correction_po_fct(con,db_name,all_df,scoring_df)
}

# Create column for table display
scoring_df <- gen_final_table_display(scoring_df,all_df$amount)

# Update table credits applications scoring
write_sql_query <- paste("DELETE FROM ",db_name,".loan_scoring 
      WHERE loan_id=",loan_id,";", sep="")
suppressMessages(dbSendQuery(con,write_sql_query))
if(any(scoring_df$score!="NULL")){
  suppressMessages(dbWriteTable(con, name = "loan_scoring", 
    value = scoring_df[,c("loan_id","amount","installments","score",
                          "display_score","color","pd","created_at")],
    field.types = c(loan_id="numeric", amount="integer", 
    installments="integer", score="character(20)", display_score="character(20)",
    color="integer",pd="numeric",created_at="datetime"),
    row.names = F, append = T))
}
#suppressMessages(dbSendQuery(con,update_counter(db_name,loan_id)))

#######
# END #
#######

