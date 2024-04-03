

###############################################################################
#             Joint script for Application and Behavioral scoring             #
#          Apply Pre-score logistic on affiliate and client requests          #
#                          Version 2.0 (2023/11/22)                           #
###############################################################################

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
id <- args[1]
id <- 31945
type<-as.character(args[2])
type<-"client"

if(type=="client"){
  client_id<-as.numeric(id)
}
if(type=="affiliate"){
  request_id<-id
}


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
source(paste(main_dir,"Useful_Functions.r", sep=""))
source(paste(main_dir,"Generate_Behavioral_Criteria.r", sep=""))


# Load models
load("rdata\\pre_score_model.rdata")


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
  all_pre_scores <- gen_query(con,gen_old_pre_scores_query(db_name,client_id))
  
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
    #edit_df<-dbExecute(con,update_pre_score_query(db_name,id,priority_df,type))
    
    if(edit_df==0){ 
      processlist_query<-dbSendQuery(con,"show processlist;")
      processlist<-dbFetch(processlist_query,n=-1)
      dbClearResult(processlist_query)
      print(processlist)
      stop("Updating table failed.")
      quit(save = "no")
      print("Unsuccessful quit!")
    }
    suppressMessages(suppressWarnings(dbExecute(con,
      update_pre_score_query(db_name,id,priority_df,type))))
  } else if(type=="client"){
    suppressMessages(suppressWarnings(dbExecute(con,
       update_old_client_pre_score_query(db_name,priority_df))))
  }
}