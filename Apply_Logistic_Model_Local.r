start_time<-Sys.time()

################################################################################
#               Joint script for Application and Behavioral scoring            #
#         Apply Logistic Regression on all products (Creditero Spain)          #
#                          Version 1.0 (2022/02/15)                            #
################################################################################

########################
### Initial settings ###
########################

# Library
suppressMessages(suppressWarnings(library(RMySQL)))
suppressMessages(suppressWarnings(library(here)))
suppressMessages(suppressWarnings(library(dotenv)))
suppressMessages(suppressWarnings(require("reshape")))
suppressMessages(suppressWarnings(library(openxlsx)))
suppressMessages(suppressWarnings(require(jsonlite)))
suppressMessages(suppressWarnings(require(gbm)))
suppressMessages(suppressWarnings(library(lubridate)))


# Database
db_user <- "useres1"
db_password <- "7Gy87Ibjewfp#CTFiD"
db_name <- "creditero"
db_host <- "192.168.2.116"
df_port <- 3306
con <- dbConnect(MySQL(), user=db_user, password=db_password, 
                 dbname=db_name, host=db_host, port = df_port)


# Define work directory
main_dir <- paste("\\\\192.168.2.30\\Analyses\\Shared\\Scorecards\\Spain\\",
                  "Creditero_Scorecard\\",sep = "")


# Read argument of ID
args <- commandArgs(trailingOnly = TRUE)
loan_id <- as.numeric(args[1])
loan_id <- 950308
product_id <- 1


# Set working directory for input (R data for logistic regression) and output #
setwd(main_dir)


# Load other r files
source(paste(main_dir,"Additional_Restrictions.r", sep=""))
source(paste(main_dir,"Create_Bins.r", sep=""))
source(paste(main_dir,"Cutoffs.r", sep=""))
source(paste(main_dir,"Generate_Scoring_Table.r", sep=""))
source(paste(main_dir,"Join_Bank_Report_Application.r", sep=""))
source(paste(main_dir,"Read_Bank_Aggs.r", sep=""))
source(paste(main_dir,"SQL_Queries.r", sep=""))
source(paste(main_dir,"Useful_Functions_Local.r", sep=""))
source(paste(main_dir,"Generate_Behavioral_Criteria.r", sep=""))


# Load predefined libraries
load("rdata\\first_application_model.rdata")
load("rdata\\beh_application_model.rdata")


####################################
### Read database and build data ###
####################################

# Read credits applications
all_df <- gen_query(con,gen_app_sql_query(db_name,loan_id))
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
scoring_df<-gen_score_app(all_df,scoring_df,flag_beh)



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


# Create output dataframe
final <- as.data.frame(cbind(scoring_df$loan_id[1],
      scoring_df$amount[scoring_df$amount == unique(scoring_df$amount)[
        which.min(abs(all_df$amount - unique(scoring_df$amount)))]
        & 
          scoring_df$installments==unique(scoring_df$installments)[
            which.min(abs(all_df$installments - unique(scoring_df$installments)))]],
      scoring_df$installments[scoring_df$amount == unique(scoring_df$amount)[
        which.min(abs(all_df$amount - unique(scoring_df$amount)))]
        & 
          scoring_df$installments==unique(scoring_df$installments)[
            which.min(abs(all_df$installments - unique(scoring_df$installments)))]],
      scoring_df$score[scoring_df$amount == unique(scoring_df$amount)[
        which.min(abs(all_df$amount - unique(scoring_df$amount)))]
        & 
          scoring_df$installments==unique(scoring_df$installments)[
            which.min(abs(all_df$installments - unique(scoring_df$installments)))]],
      scoring_df$display_score[scoring_df$amount== unique(scoring_df$amount)[
        which.min(abs(all_df$amount - unique(scoring_df$amount)))]
                               & 
      scoring_df$installments==unique(scoring_df$installments)[
        which.min(abs(all_df$installments - unique(scoring_df$installments)))]]))
names(final) <- c("loan_id","amount","installments","score","display_score")
final$pd <- scoring_df$pd[scoring_df$amount== unique(scoring_df$amount)
        [which.min(abs(all_df$amount - unique(scoring_df$amount)))]
                          & 
      scoring_df$installments==unique(scoring_df$installments)
        [which.min(abs(all_df$installments - unique(scoring_df$installments)))]]
final$flag_beh <- flag_beh
final<-merge(final,all_df[,c(which(names(all_df)=="loan_id"),
                      which(!names(all_df)%in%names(final)))],by="loan_id")
# Read and write
final_exists <- readxl::read_xlsx(paste(main_dir,
                           "Monitoring\\Files\\Scored_Credits.xlsx", sep=""))
final[names(final_exists)[which(!names(final_exists)%in%names(final))]]<-NA
final_exists[names(final)[which(!names(final)%in%names(final_exists))]]<-NA
final <- rbind(final,final_exists)
final <- final[!duplicated(final$loan_id),] 

final$wrong_age<-ifelse(final$birth_date>"2022-01-01",1,0)

end_time<-Sys.time()

writexl::write_xlsx(final, 
            paste(main_dir,"Monitoring\\Files\\Scored_Credits.xlsx", sep=""))


### update edit counter
#suppressMessages(dbSendQuery(con,update_counter(db_name,loan_id)))



#######
# END #
#######