
############################################################
######## Functions to read JSON file from bank report ######
############################################################


########libraries#########
library(jsonlite)
library(lubridate)

############ read JSON column from unnaix.bank.aggregations ###########

bank_criteria_JSON_reader<-function(all_bank_reports){
  if(all(is.na(all_bank_reports))){
    return(NA)
  } else if(nrow(all_bank_reports)>0){
    okay<-0
    j<-1
    while(okay==0&j<=nrow(all_bank_reports)){
      bank_report<-all_bank_reports[j,]
      df<-bank_report[,-which(names(bank_report)=="data")]
      # Get fields
      df_loop <- lapply(bank_report$data,
                        function(j) as.list(fromJSON(j)))[[1]]
      
      ################# BANK VARIABLES  #############
      df$transactions<-ifelse(
        length(df_loop$statements)>0,nrow(df_loop$statements),0)
      if(df$transactions>=1){
        df$start_date<-df_loop$statements$deposit_date[df$transactions]
        okay<-1
      }
      if(df$transactions>=1){
        df$end_date <- df_loop$statements$deposit_date[1]
      }
      
      if(okay==1){
        
        ################ personal data ###############
        df$accounts<-ifelse(length(df_loop$accounts)>0,nrow(df_loop$accounts),0)
        df$customers <- 
                   ifelse(length(df_loop$customers)>0,nrow(df_loop$customers),0)
        if(df$customers>=1){
          customer_ids<-df_loop$customers$`_id`
        } else{
          customer_ids<-NA
        }
        suppressWarnings(tryCatch({
          id<-which(grepl(df$dni,customer_ids))
        }, error=function(e){}))  
        
        suppressWarnings(tryCatch({
          if(!is.null(df_loop$customers$address[[id]])&
             length(df_loop$customers$address[[id]])>0){
            df$aggs_address<-df_loop$customers$address[[id]]
          }
        }, error=function(e){}))
        
        suppressWarnings(tryCatch({
          df$aggs_birth_date<-df_loop$customers$birth_date[id]
        }, error=function(e){}))
        
        ############### work on criteria ####################
        suppressWarnings(tryCatch({
          df$calendar_months <-
            round(df_loop$summary$all_accounts$total$total_days/30)
        }, error=function(e){}))
        suppressWarnings(tryCatch({
          df$above_threshold<-
              df_loop$summary$all_accounts$total$days_above_threshold/
                  df_loop$summary$all_accounts$total$total_days
        }, error=function(e){})) 
        suppressWarnings(tryCatch({
          df$overdraft<-df_loop$summary$all_accounts$total$days_overdraft/
            df_loop$summary$all_accounts$total$total_days
        }, error=function(e){})) 
        suppressWarnings(tryCatch({
          df$max_balance <- df_loop$summary$all_accounts$total$max_balance/100
        }, error=function(e){}))
        suppressWarnings(tryCatch({
          df$min_balance <- df_loop$summary$all_accounts$total$min_balance/100
        }, error=function(e){}))
        suppressWarnings(tryCatch({
          df$income <- sum(df_loop$summary$all_accounts$by_month$total_income)/
                                                        (100*df$calendar_months)
        }, error=function(e){}))
        suppressWarnings(tryCatch({
          df$outcome <- sum(df_loop$summary$all_accounts$by_month$total_outcome)/
                                                        (100*df$calendar_months)
        }, error=function(e){}))
        suppressWarnings(tryCatch({
          df$avg_balance <- sum(df_loop$summary$all_accounts$by_month$avg_balance*
                                df_loop$summary$all_accounts$by_month$total_days)/
            sum(df_loop$summary$all_accounts$by_month$total_days)/100
        }, error=function(e){}))
        suppressWarnings(tryCatch({
          df$current_balance <- df_loop$summary$all_accounts$current_balance/100
        }, error=function(e){}))
        suppressWarnings(tryCatch({
          temp<-0
          for (j in 1:df$accounts){
            temp<-temp+
                  sum(df_loop$summary$by_account$by_month[[j]]$financial_expenses)
          }
          df$finance_expense <- temp/100/df$calendar_months
        }, error=function(e){}))
        
        ############## INCOME/OUTCOME BY TRANSACTION ############
        suppressWarnings(tryCatch({
          parent_category<-character(0)
          child_category<-character(0)
          for(j in 1:df$transactions){
            parent_category[j]<-df_loop$statements$category$hierarchy[[j]]$title[1]
            child_category[j]<-df_loop$statements$category$hierarchy[[j]]$title[2]
          }
          df_loop$statements$parent_category<-parent_category
          df_loop$statements$child_category<-child_category
          
          parent_category <- 
                        as.data.frame(table(df_loop$statements$parent_category))
          parent_category$Var1<-as.character(parent_category$Var1)
          parent_category$outcome<-0
          parent_category$income<-0
          parent_category$outgoing_transactions<-0
          parent_category$incoming_transactions<-0
          
          child_category <- 
                         as.data.frame(table(df_loop$statements$child_category))
          child_category$Var1<-as.character(child_category$Var1)
          child_category$outcome<-0
          child_category$income<-0
          child_category$outgoing_transactions<-0
          child_category$incoming_transactions<-0
          
          if(nrow(parent_category)>0){
            for (p in 1:nrow(parent_category)){
              parent_category$Freq[p]<-parent_category$Freq[p]/df$calendar_months
              
              parent_category$outcome[p]<-sum(df_loop$statements$amount[which(
                df_loop$statements$amount<0&df_loop$statements$parent_category==
                  parent_category$Var1[p])])/100/df$calendar_months
              
              parent_category$income[p]<-sum(df_loop$statements$amount[which(
                df_loop$statements$amount>0&df_loop$statements$parent_category==
                  parent_category$Var1[p])])/100/df$calendar_months
              
              parent_category$outgoing_transactions[p]<-length(which(
                df_loop$statements$amount<0&df_loop$statements$parent_category==
                  parent_category$Var1[p]))/df$calendar_months
              
              parent_category$incoming_transactions[p]<-length(which(
                df_loop$statements$amount>0&df_loop$statements$parent_category==
                  parent_category$Var1[p]))/df$calendar_months
              
              df[c(parent_category$Var1[p])] <- parent_category$Freq[p]
              
              df[c(paste(parent_category$Var1[p],"_outcome",sep=""))] <- 
                                            parent_category$outcome[p]
              
              df[c(paste(parent_category$Var1[p],"_income",sep=""))] <- 
                                            parent_category$income[p]
              
              df[c(paste(parent_category$Var1[p],
                                          "_outgoing_transactions",sep=""))] <- 
                                        parent_category$outgoing_transactions[p]
              
              df[c(paste(parent_category$Var1[p],
                                          "_incoming_transactions",sep=""))] <- 
                                        parent_category$incoming_transactions[p]
            }
          }
          if(nrow(child_category)>0){
            for (p in 1:nrow(child_category)){
              child_category$Freq[p]<-child_category$Freq[p]/df$calendar_months
              
              child_category$outcome[p]<-sum(df_loop$statements$amount[which(
                df_loop$statements$amount<0&df_loop$statements$child_category==
                                child_category$Var1[p])])/100/df$calendar_months
              
              child_category$income[p]<-sum(df_loop$statements$amount[which(
                df_loop$statements$amount>0&df_loop$statements$child_category==
                                child_category$Var1[p])])/100/df$calendar_months
              
              child_category$outgoing_transactions[p]<-length(which(
                df_loop$statements$amount<0&df_loop$statements$child_category==
                                  child_category$Var1[p]))/df$calendar_months
              
              child_category$incoming_transactions[p]<-length(which(
                df_loop$statements$amount>0&df_loop$statements$child_category==
                                  child_category$Var1[p]))/df$calendar_months
              
              df[c(child_category$Var1[p])] <- child_category$Freq[p]
              
              df[c(paste(child_category$Var1[p],"_outcome",sep=""))] <- 
                                child_category$outcome[p]
              
              df[c(paste(child_category$Var1[p],"_income",sep=""))] <- 
                                child_category$income[p]
              
              df[c(paste(child_category$Var1[p],
                                          "_outgoing_transactions",sep=""))] <- 
                                child_category$outgoing_transactions[p]
              
              df[c(paste(child_category$Var1[p],
                                          "_incoming_transactions",sep=""))] <- 
                                child_category$incoming_transactions[p]
            }
          }
        }, error=function(e){}))
        
        suppressWarnings(tryCatch({
          df$transactions<-df$transactions/df$calendar_months
        }, error=function(e){}))
      
      
      } else {
        j<-j+1
      }
    }
  } else {
    return(NA)
  }
  if(okay==1){
    return(df)
  } else{
    return(NA)
  }
  
}

########### filter bank aggregations ############

take_relevant_fields<-function(bank_aggregations_unfiltered,flag_beh){
  if(length(bank_aggregations_unfiltered[!is.na(
    bank_aggregations_unfiltered)])>0){
    aggregated_criteria<-
        c("current_balance","avg_balance","transactions","above_threshold","aggs_birth_date")
    bank_criteria<-c("FAST LOANS_outcome","OTHER TRANSFERS_income",
                     "CONSUMER LOANS_outcome","GENERAL LOANS_outcome",
                     "CREDIT CARD PAYMENT & TRANSFERS_outcome","INSURANCE_outcome",
                     "EDUCATION, HEALTH AND BEAUTY_outcome","CASH DEPOSIT_income",
                     "GROCERIES & SUPERMARKETS_outcome","UTILITY SERVICES",
                     "TOBACCO AND ALCOHOL_outcome","ONLINE SERVICES_outcome",
                     "TAXES AND GOVERNMENT FEES_outcome","CASH WITHDRAWAL_outcome")
    
    if(flag_beh==1){
      suppressWarnings(tryCatch({ 
        df<-bank_aggregations_unfiltered[c("id","created_at","client_id","dni",
           names(bank_aggregations_unfiltered)[which(
              names(bank_aggregations_unfiltered)%in%aggregated_criteria)],
           names(bank_aggregations_unfiltered)[which(
              names(bank_aggregations_unfiltered)%in%bank_criteria)])]
        
        df[aggregated_criteria[which(!aggregated_criteria%in%names(df))]]<-NA
        df[bank_criteria[which(!bank_criteria%in%names(df))]]<-NA
      }, error=function(e){print("Unnax failed to generate bank report")}))
    } else {
      df<-bank_aggregations_unfiltered
    }
    if(df$transactions>0&!is.na(df$transactions)){
      return(df)
    } else {
      return(NA)
    }
  } else {
    return(NA)
  }
}
