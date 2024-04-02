

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
#id<-"32a7b916-efcf-42cb-8a31-5e7422071829"
id<-31945
type<-as.character(args[2])
#type<-"affiliate"
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

# temp_df<-expand.grid(1,c(18,40,60,NA),c(24,120,NA),c(1200,2000,NA),c("M","F",NA))
# names(temp_df)<-c("request_id","age","work_experience","salary","gender")
# temp_df$request_id<-1:nrow(temp_df)
# temp_df<-gen_pre_score_criteria(temp_df,"affiliate")
# temp_df<-merge(temp_df[,1:5],gen_client_prescore(temp_df,cu_pre_score,
#                                            prescore_model,"affiliate"),
#                by="request_id",all.x = T)
# temp_df$pd<-round(temp_df$pd,3)
# writexl::write_xlsx(temp_df,"~/Spain/Scoring/Applied_model_Creditero/examples_prescore.xlsx")
####################################
### Read database and build data ###
####################################


### compute prev credit data
flag_beh<-0
if(type=="client"){
  # prev credits
  all_apps <- suppressWarnings(dbFetch(dbSendQuery(con, 
                    gen_all_credits_query(db_name,client_id)), n=-1))
  
  loan_id<-max(all_apps$loan_id)
  
  
  all_credits <- all_apps[which(all_apps$status %in% c(8,9,13,16,17)),]
  
  # Compute flag repeats
  flag_beh <- ifelse(nrow(all_credits[all_credits$loan_id!=loan_id,])>0,1,0)
}
# Read pre-score raw data and compute pre-score
fetch_df<-dbSendQuery(con,gen_pre_score_raw_sql_query(db_name,id,type))
all_df <- suppressWarnings(dbFetch(fetch_df, n=-1))
dbClearResult(res = fetch_df)

if(type=="affiliate"){
  all_df<-unpack_raw_covariates_prescore(all_df)
}
#print("JSON unpacked! ")

all_df<-gen_pre_score_criteria(all_df,type)
#print("Bins created! ")

priority_df<-gen_client_prescore(all_df,cu_pre_score,prescore_model,type)
#print("Pre-score computed! ")

### fetch beahivour history if client has previous apps
if(type=="client"){
  # fetch old prescore
  all_pre_scores<-suppressWarnings(dbFetch(dbSendQuery(con,
                                                     gen_old_pre_scores_query(db_name,client_id)), n=-1))
  
  
  # fetch last score
  all_scores <- suppressWarnings(dbFetch(dbSendQuery(con,
                                                   gen_old_scores_query(db_name,all_apps$loan_id[which(
                                                     all_apps$loan_id<loan_id)])), n=-1))
  all_scores<-all_scores[!duplicated(all_scores$loan_id),]
  
  # last prescore if it exists
  loan_priority<-gen_last_pre_score(all_pre_scores,all_scores,client_id)
  if(nrow(loan_priority)>0){
    priority_df<-loan_priority
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