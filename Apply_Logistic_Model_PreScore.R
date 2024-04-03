

################################################################################
#               Joint script for Application and Behavioral scoring            #
#         Apply Logistic Regression on for client prescore assessment          #
#                          Version 1.0 (2022/11/30)                            #
################################################################################



########################
### Initial settings ###
########################

# Library
suppressMessages(suppressWarnings(require(RMySQL)))
suppressMessages(suppressWarnings(require(here)))
suppressMessages(suppressWarnings(require(dotenv)))
suppressMessages(suppressWarnings(require("reshape")))
suppressMessages(suppressWarnings(require(openxlsx)))
suppressMessages(suppressWarnings(require(jsonlite)))
suppressMessages(suppressWarnings(require(lubridate)))


# Defines the directory where custom .env file is located
load_dot_env(file = here('.env'))


#########################
### Command arguments ###
#########################

args <- commandArgs(trailingOnly = TRUE)
id <- args[1]
type<-as.character(args[2])
if(type=="client"){
  client_id<-as.numeric(id)
}
if(type=="affiliate"){
  request_id<-id
}
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
suppressWarnings(dbExecute(con, sqlMode))


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
rdata <- file.path(base_dir, "rdata","pre_score_model.rdata")
load(rdata)

####################################
### Read database and build data ###
####################################


### compute prev credit data
flag_beh<-0
if(type=="client"){
  # prev credits
  all_apps <- gen_query(con,gen_all_credits_query(db_name,client_id))
  
  loan_id<-max(all_apps$loan_id)
  
  all_credits <- all_apps[which(all_apps$status %in% c(8,9,13,16,17)),]
  
  # Compute flag repeats
  flag_beh <- ifelse(nrow(all_credits[all_credits$loan_id!=loan_id,])>0,1,0)
}
# Read pre-score raw data and compute pre-score
all_df <- gen_query(con,gen_pre_score_raw_sql_query(db_name,id,type))
dbClearResult(res = fetch_df)

if(type=="affiliate"){
  all_df <- unpack_raw_covariates_prescore(all_df)
}

all_df <- gen_pre_score_criteria(all_df,type)
priority_df <- gen_client_prescore(all_df,cu_pre_score,prescore_model,type)

### fetch beahivour history if client has previous apps
if(type=="client"){
  
  # fetch old prescore
  gen_query(con,gen_old_pre_scores_query(db_name,client_id))

  # fetch last score
  all_scores <- gen_query(con, gen_old_scores_query(db_name,
    all_apps$loan_id[which(all_apps$loan_id<loan_id)]))
  all_scores <- all_scores[!duplicated(all_scores$loan_id),]

  # last prescore if it exists
  loan_priority <- gen_last_pre_score(all_pre_scores,all_scores,client_id)
  if(nrow(loan_priority)>0){
    priority_df <- loan_priority
  }
}

# finalize dataframe
priority_df$priority<-ifelse(priority_df$priority=="Bad","Indeterminate",
                             priority_df$priority)

### write output sql data
if(flag_beh==0&nrow(priority_df)>0){
  if(nrow(all_df)>0){
    edit_df<-dbExecute(con,update_pre_score_query(db_name,id,priority_df,type))
    if(edit_df==0){ 
      stop("Updating table failed.")
      force_error()
      quit(save = "no")
    }
  } else if(type=="client"){
    suppressMessages(suppressWarnings(dbExecute(con,
                update_old_client_pre_score_query(db_name,priority_df))))
  }
}