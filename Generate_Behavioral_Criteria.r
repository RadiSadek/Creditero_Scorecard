library(lubridate)
##############################################################
#########  Functions to draw behavioral information  #########
##############################################################

# previous credit
select_last_credit <- function(prev_loans,loan_id){
  df<-prev_loans[which(prev_loans$loan_id!=loan_id),]
  df<-df[order(df$loan_id,decreasing = TRUE),]
  return(df$loan_id[1])
}

gen_revolve_data<-function(credit_df){
  loan_fees<-suppressWarnings(fetch(dbSendQuery(con,
                          gen_taxes_df_query(db_name,credit_df$id)),n=-1))
  if(nrow(loan_fees)>0){
    credit_df$revolve_7<-length(which(loan_fees$type %in%
                                      c(1,16,19)&is.na(loan_fees$deleted_at)))
    credit_df$revolve_14<-length(which(loan_fees$type %in%
                                        c(2,17,20)&is.na(loan_fees$deleted_at)))
    credit_df$revolve_30<-length(which(loan_fees$type %in%
                                        c(3,18,21)&is.na(loan_fees$deleted_at)))
  } else {
    credit_df$revolve_7<-0
    credit_df$revolve_14<-0
    credit_df$revolve_30<-0
  }
  credit_df$revolved_days<-7*credit_df$revolve_7+14*credit_df$revolve_14+
                                30*credit_df$revolve_30
  
  credit_df$days_to_repay<-as.numeric(as.Date(credit_df$finished_at)-
                                        as.Date(credit_df$activated_at))
  
  credit_df$delay_pct_months_mat<-ifelse(
    credit_df$days_to_repay>credit_df$days_period+credit_df$revolved_days,
    (credit_df$days_to_repay-credit_df$days_period-credit_df$revolved_days)/30,
    credit_df$days_to_repay/(credit_df$days_period+credit_df$revolved_days)-1)
  
  credit_df$adjusted_delay_pct<-ifelse(credit_df$total_delay>0,credit_df$total_delay/30,
                                  credit_df$delay_pct_months_mat)
  
  return(credit_df)
}

# create behavioral bins
gen_behavioral_criteria <- function(application_df,prev_credits,last_credit){
  df<-application_df
  # create raw data
  df$all_time_step<-df$requested_amount-max(prev_credits$amount[which(
                                  prev_credits$loan_id!=df$loan_id)])
  df$prev_adjusted_delay_pct<-last_credit$adjusted_delay_pct
  
  df$time_of_application<-as.numeric(substr(df$created_at,12,13))
  # waiting_time<-round(as.numeric(
  #   difftime(substr(application_df$created_at,1,10),
  #            substr(last_credit$finished_at,1,10),units = "days"),digits = 0))
  prev_credits$hidden_refinance<-NA
  if(nrow(prev_credits)>1){
  for(i in 2:nrow(prev_credits)){
    prev_credits$hidden_refinance[i]<-ifelse(
      as.numeric(difftime(prev_credits$created_at[i],
               prev_credits$deactivated_at[i-1],units = "hours"))<24,1,0)
  }
  df$hidden_refinance_pct<-sum(prev_credits$hidden_refinance,na.rm = T)/
                                                        (nrow(prev_credits)-1)
  } else {
    df$hidden_refinance_pct<-0
  }
  # define bins
  df$ALL_TIME_STEP<-as.factor(ifelse(df$all_time_step<=0,"<=0",">0"))
  
  df$DAYS_PERIOD<-as.factor(
    ifelse(is.na(df$days_period),"25+",
    ifelse(df$days_period<=24,"1-24","25+")))
  
  df$PREVIOUS_CREDIT_EARLY_REPAYMENT<-
    ifelse(is.na(df$prev_adjusted_delay_pct),"0-40%",
    ifelse(df$prev_adjusted_delay_pct<=(-0.6),"0-40%",
    ifelse(df$prev_adjusted_delay_pct<=(-0.1),"40-90%",
    ifelse(df$prev_adjusted_delay_pct<=0,"90-100%","100%+"))))
  
  df$WAITING_TIME<-ifelse(round(as.numeric(difftime(df$created_at,
            last_credit$finished_at,units="hours")),digits = 0)<=2,"0-2h","2h+")
  
  df$HIDDEN_REFINANCE_PCT<-as.factor(ifelse(df$hidden_refinance_pct==1,"100%","u100%"))
  
  df$PREV_ON_TIME<-as.factor(ifelse(last_credit$total_delay==0,"Yes","No"))
  
  df$PREV_CREDIT_REVOLVED<-ifelse(last_credit$revolved_days<=7,"No","Yes")
  
  df$APPLICATION_TIME<-as.factor(
    ifelse(df$time_of_application<=6,"Night(20:00-7:00)",
    ifelse(df$time_of_application>=20,"Night(20:00-7:00)","Day(7:00-20:00)")))

  
  return(df)
}

gen_app_behavior<-function(application_df){
  #application_df$DAYS_PERIOD <- ifelse(application_df$days_period<=25,"u25","26+")
  return(application_df)
}