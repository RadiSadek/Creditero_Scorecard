
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
source(paste(main_dir,"Terminated_Local.r", sep=""))




#############################
### Read current po table ###
#############################

# # Read current table
get_po_sql <- paste("SELECT * FROM ",db_name,
".clients_prior_approval_applications",sep="")
po <- suppressWarnings(fetch(dbSendQuery(con,get_po_sql), n=-1))
po_raw <- po
 
# Read current database
if(nrow(po)==0){
 id_max <- 1
} else {
 id_max <- max(po$id)+1
}



###################################################
### Generate data of potential credits to offer ###
###################################################

# Get date of previous day
prev_day <- Sys.Date() - 1

# Read all credits
get_actives_sql <- paste("
SELECT id, client_id, amount, activated_at,
finished_at, status, product_id
FROM ",db_name,".loans WHERE activated_at IS NOT NULL",sep="")
all_credits <- suppressWarnings(fetch(dbSendQuery(con,get_actives_sql), n=-1))
all_credits_raw <- subset(all_credits,all_credits$status %in% c(9,13))

# Read credit applications
select <- subset(all_credits,!is.na(all_credits$finished_at) & 
  substring(all_credits$finished_at,1,10)==prev_day)
select <- subset(select,select$status %in% c(9,13))



#####################################################
### Apply selection criteria for credits to offer ###
#####################################################

# Remove those who have active credit of corresponding company
all_credits$activated_at_day <- substring(all_credits$activated_at,1,10)
actives <- subset(all_credits,is.na(all_credits$finished_at))
actives <- subset(actives,actives$activated_at_day>=prev_day)
select <- select[!(select$client_id %in% actives$client_id),]



#####################
### Compute score ###
#####################

if(nrow(select)>0){
  
  # Compute and append score
  select <- select[!duplicated(select$client_id),]
  select$max_amount <- NA
  select$max_installment_amount <- NA
  select$score_max_amount <- NA
  select$max_delay <- NA
  for(i in 1:nrow(select)){
    suppressWarnings(tryCatch({
      client_id <- select$client_id[i]
      last_id <- select$id[i]
      calc <- gen_terminated_fct(con,client_id,NA,last_id,0)
      select$max_amount[i] <- calc[[1]]
      select$max_installment_amount[i] <- calc[[2]]
      select$score_max_amount[i] <- calc[[3]]
      select$max_delay[i] <- as.numeric(calc[[4]])
    }, error=function(e){}))
  }
  
  
  
  ##################################
  ### Reapply selection criteria ###
  ##################################
  
  # Select based on score and DPD
  select <- subset(select,select$max_amount>-Inf & select$max_amount<Inf)
  select <- subset(select,select$max_delay<=360)
  
  
  
  #############################################
  ### Work on final dataset and write in DB ###
  #############################################
  
  if(nrow(select)>0){
    
    # Create final dataframe for writing in DB
    offers <- select
    offers$id <- seq(id_max,id_max+nrow(offers)-1,1)
    offers$created_at <- Sys.time()
    offers$updated_at <- NA
    offers$deleted_at <- NA
    offers$credit_amount_updated <- NA
    offers$installment_amount_updated <- NA
    offers <- offers[,c("id","client_id",
       "max_amount", "credit_amount_updated","max_installment_amount",
       "installment_amount_updated","product_id",
       "created_at","updated_at","deleted_at")]
    names(offers)[names(offers)=="max_amount"] <- "credit_amount"
    names(offers)[names(offers)=="max_installment_amount"] <- 
      "installment_amount"
    offers[is.na(offers)] <- "NULL"
    
    
    # Make result ready for SQL query
    string_sql <- gen_sql_string_po_terminated(offers,1)
    if(nrow(offers)>1){
      for(i in 2:nrow(offers)){
        string_sql <- paste(string_sql,gen_sql_string_po_terminated(offers,i),
                            sep=",")
      }
    }

    # Output real offers
    update_prior_query <- paste("INSERT INTO ",db_name,
        ".clients_prior_approval_applications VALUES ",string_sql,";", sep="")

    # Write in database
    if(nrow(offers)>0){
      suppressMessages(suppressWarnings(dbSendQuery(con,update_prior_query)))
    }}}



######################################################
### Check for special cases and delete immediately ###
######################################################

# Read special cases (deceased and gdrk marketing clients) 
get_special_sql <- paste("
SELECT id FROM ",db_name,".clients
WHERE dead_at IS NOT NULL",sep="")
special <- suppressWarnings(fetch(dbSendQuery(con,get_special_sql), n=-1))

# Remove special cases if has offer
po_get_special_query <- paste(
  "SELECT id, client_id
  FROM ",db_name,".clients_prior_approval_applications
  WHERE deleted_at IS NULL",sep="")
po_special <- suppressWarnings(fetch(dbSendQuery(con,po_get_special_query), 
    n=-1))
po_active <- po_special
po_special <- po_special[po_special$client_id %in% special$id,]

if(nrow(po_special)>0){
  po_special_query <- paste("UPDATE ",db_name,
     ".clients_prior_approval_applications SET updated_at = '",
     substring(Sys.time(),1,19),"', deleted_at = '",
     paste(substring(Sys.time(),1,10),"04:00:00",sep=),"'
   WHERE id IN",gen_string_po_terminated(po_special), sep="")
  suppressMessages(suppressWarnings(dbSendQuery(con,po_special_query)))
}


################################################################
### Remove clients who have a active credit and active offer ###
################################################################

all_actives <- subset(all_credits,is.na(all_credits$finished_at))
po_active_todelete <- po_active[po_active$client_id %in% all_actives$client_id,]

if(nrow(po_active_todelete)>0){
  po_special_query <- paste("UPDATE ",db_name,
    ".clients_prior_approval_applications SET updated_at = '",
    substring(Sys.time(),1,19),"', deleted_at = '",
    paste(substring(Sys.time(),1,10),"04:00:00",sep=),"'
    WHERE id IN",gen_string_po_terminated(po_active_todelete), sep="")
  suppressMessages(suppressWarnings(dbSendQuery(con,po_special_query)))
}


###########
### End ###
###########



