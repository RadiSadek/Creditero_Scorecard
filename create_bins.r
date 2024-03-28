####################################################
######## Functions to evaluate scoring bins ########
####################################################

############# first applications ###############

create_bins_first_app<-function(bank_aggregations_filtered){
  if(!is.na(bank_aggregations_filtered)){
    df<-bank_aggregations_filtered
    
    # Fast loans outcome
    df$FAST_LOANS_OUTCOME<-as.factor(
      ifelse(is.na(df$`FAST LOANS_outcome`),"u50",
      ifelse(df$`FAST LOANS_outcome`<=-1600,"o1600",
      ifelse(df$`FAST LOANS_outcome`<=-600,"_in_(600,1600]",
      ifelse(df$`FAST LOANS_outcome`<=-50,"_in_(50,600]","u50")))))
    
    # Consumer loans
    df$CONSUMER_LOANS_OUTCOME<-as.factor(
      ifelse(is.na(df$`CONSUMER LOANS_outcome`),"0-50",
      ifelse(df$`CONSUMER LOANS_outcome`>=-50,"0-50",
      ifelse(df$`CONSUMER LOANS_outcome`>=-250,"50-250","250+"))))
    
    # % of days above 100 Euros
    df$ABOVE_THRESHOLD<-as.factor(
      ifelse(df$above_threshold<0.4,"u40%","40+%"))
    
    # credit card payments
    df$CREDIT_CARD_PAYMENTS<-as.factor(
      ifelse(is.na(df$`CREDIT CARD PAYMENT & TRANSFERS_outcome`),"0-200",
      ifelse(df$`CREDIT CARD PAYMENT & TRANSFERS_outcome`>=-200,"0-200",
      ifelse(df$`CREDIT CARD PAYMENT & TRANSFERS_outcome`>=-750,"200-750","750+"))))
    
    # current balance
    df$CURRENT_BALANCE<-as.factor(
      ifelse(df$current_balance<=-20,"<=-20",">-20"))
    
    # insurance
    df$INSURANCE<-as.factor(
      ifelse(is.na(df$INSURANCE_outcome),"<20",
      ifelse(df$INSURANCE_outcome<=-20,"20+","<20")))
    
    # health & beauty
    df$HEALTH_BEAUTY<-as.factor(
      ifelse(is.na(df$`EDUCATION, HEALTH AND BEAUTY_outcome`),"u20",
      ifelse(df$`EDUCATION, HEALTH AND BEAUTY_outcome`>-20,"u20","20+")))
    
    # transactions
    df$TRANSACTIONS<-as.factor(
      ifelse(df$transactions<70,"<70","70+"))
 
    # Groceries
    df$GROCERIES_OUTCOME<-as.factor(
      ifelse(is.na(df$`GROCERIES & SUPERMARKETS_outcome`),"u200",
      ifelse(df$`GROCERIES & SUPERMARKETS_outcome`>-200,"u200","200+")))
   
    # utility services
    df$UTILITY_SERVICES<-as.factor(
      ifelse(is.na(df$`UTILITY SERVICES`),"1-2",
      ifelse(df$`UTILITY SERVICES`<1,"<1",
      ifelse(df$`UTILITY SERVICES`<=2,"1-2","2+"))))
 
    # Cash deposit
    df$CASH_DEPOSIT_INCOME<-as.factor(
      ifelse(is.na(df$`CASH DEPOSIT_income`),"u300",
      ifelse(df$`CASH DEPOSIT_income`<=300,"u300","300+")))
  
    # Tobacco and alcohol
    df$TOBACCO_AND_ALCOHOL_OUTCOME<-as.factor(
      ifelse(is.na(df$`TOBACCO AND ALCOHOL_outcome`),"=0",
      ifelse(df$`TOBACCO AND ALCOHOL_outcome`==0,"=0",">0")))

     return(df)
  } else {
    return(NA)
  }
}
############# consecutive credit application ###############
create_bins_beh <- function(bank_aggregations_filtered){
  if(!is.na(bank_aggregations_filtered)){
    df <- bank_aggregations_filtered
    df$FAST_LOANS_OUTCOME<-
      ifelse(is.na(df$`FAST LOANS_outcome`),"0-1000",
      ifelse(df$`FAST LOANS_outcome`>-1000,"0-1000","1000+"))
    
    df$OTHER_INCOME <- 
      ifelse(is.na(df$`OTHER TRANSFERS_income`),"0-1500",
      ifelse(df$`OTHER TRANSFERS_income`<=1500,"0-1500","1500+"))
    
    df$GENERAL_LOANS_OUTCOME<-
      ifelse(is.na(df$`GENERAL LOANS_outcome`),"0-600",
      ifelse(df$`GENERAL LOANS_outcome`<=-600,"600+","0-600"))
    
    df$DEPOSIT<-
      ifelse(is.na(df$`CASH DEPOSIT_income`),"0-1000",
      ifelse(df$`CASH DEPOSIT_income`<1000,"0-1000","1000+"))
    
    df$ABOVE_THRESHOLD<-
      ifelse(is.na(df$above_threshold),"<85%",
      ifelse(df$above_threshold>=0.85,">=85%","<85%"))
      
    df$CONSUMER_LOANS<-
      ifelse(is.na(df$`CONSUMER LOANS_outcome`),"0-300",
      ifelse(df$`CONSUMER LOANS_outcome`<=-300,"300+","0-300"))
    
    df$AVG_BALANCE<-
      ifelse(is.na(df$avg_balance),"u400",
      ifelse(df$avg_balance>=400,"400+","u400"))
    
    df$INSURANCE_OUTCOME<-
      ifelse(is.na(df$INSURANCE_outcome),"0-200",
      ifelse(df$INSURANCE_outcome<=-200,"200+","0-200"))
    
    df$CREDIT_CARD_PAYMENTS<-
      ifelse(is.na(df$`CREDIT CARD PAYMENT & TRANSFERS_outcome`),"0-100",
      ifelse(df$`CREDIT CARD PAYMENT & TRANSFERS_outcome`<=-100,"100+","0-100"))
    
    df$HEALTH_BEAUTY<-
      ifelse(is.na(df$`EDUCATION, HEALTH AND BEAUTY_outcome`),"0-50",
      ifelse(df$`EDUCATION, HEALTH AND BEAUTY_outcome`<=-50,"50+","0-50"))
    
    df$ONLINE_SERVICES<-
      ifelse(is.na(df$`ONLINE SERVICES_outcome`),"0-20",
      ifelse(df$`ONLINE SERVICES_outcome`>-20,"0-20","20+"))
      
    df$TAXES<-
      ifelse(is.na(df$`TAXES AND GOVERNMENT FEES_outcome`),"0-10",
      ifelse(df$`TAXES AND GOVERNMENT FEES_outcome`>-10,"0-10","10+"))
    
    df$WITHDRAWAL<-
      ifelse(is.na(df$`CASH WITHDRAWAL_outcome`),"0-50",
      ifelse(df$`CASH WITHDRAWAL_outcome`<=-50,"50+","0-50"))
    
    return(df)
  } else {
    df$FAST_LOANS_OUTCOME<-"0-1000"
    df$OTHER_INCOME<-"0-1500"
    df$GENERAL_LOANS_OUTCOME<-"0-600"
    df$DEPOSIT<-"0-1000"
    df$ABOVE_THRESHOLD<-"<85%"
    df$CONSUMER_LOANS<-"0-300"
    df$AVG_BALANCE<-"u400"
    df$INSURANCE_OUTCOME<-"0-200"
    df$CREDIT_CARD_PAYMENTS<-"0-100"
    df$HEALTH_BEAUTY<-"0-50"
    df$ONLINE_SERVICES<-"0-20"
    df$TAXES<-"0-10"
    df$WITHDRAWAL<-"0-50"
  }
}
#### join bank report
create_bins <- function(bank_aggregations_filtered,flag_beh){
  if(flag_beh==0){
    df<-bank_aggregations_filtered
      if(!is.na(bank_aggregations_filtered)){
      empty_fields<-setdiff(first_application_model$var.names,names(df))
      empty_fields<-setdiff(empty_fields,c("has_prev_apps","immigrant_flag","age",
                                           "gender","education","marital_status",
                                           "ownership","salary","work_experience",
                                           "province"))
      empty_fields<-gsub("`","",empty_fields)
      df[,c(empty_fields)]<-0
    }
    return(df)
  } else {
    return(create_bins_beh(bank_aggregations_filtered))
  }
}

gen_demographic_criteria <- function(application_df,flag_beh,bank_dob){
  df<-application_df
  if(!is.na(bank_dob)){
    df$age<-gen_age(application_df$created_at,bank_dob)
  } else if(!is.na(application_df$birth_date)&
     difftime(substr(application_df$created_at,1,10),
              application_df$birth_date,units = "days")>365){
    df$age<-gen_age(application_df$created_at,application_df$birth_date)
  } else {
    df$age<-25
  }
  
  if(flag_beh==0){
    # df$MARITAL_STATUS<-as.factor(
    #   ifelse(is.na(df$marital_status),"_Not_married",
    #   ifelse(df$marital_status%in%c("2"),"_Married","_Not_married"))
    # )
    # df$SALARY <- as.factor(
    #   ifelse(is.na(df$salary),"<1800",
    #   ifelse(df$salary<1800,"<1800",">=1800"))
    # )
    # df$AGE<-as.factor(ifelse(is.na(df$age),"0-35",
    #     ifelse(df$age<=35,"0-35",
    #     ifelse(df$age<55,"36-54","55+")))
    # )
    # df$GENDER<-as.factor(ifelse(is.na(df$gender),"M",df$gender))
    # df$WORK_EXPERIENCE<-as.factor(
    #   ifelse(is.na(df$work_experience),"<=9_years",
    #   ifelse(df$work_experience<=108,"<=9_years",">9_years")))
    df$gender<-ifelse(is.na(df$gender),0,
               ifelse(df$gender=="M",2,1))
    df$immigrant_flag<-ifelse(substr(df$dni,1,1) %in%
                                c("1","2","3","4","5","6","7","8","9","0"),0,1)
    df$province<-as.numeric(substr(df$zip_code,1,2))
    if(is.na(df$province)){
      df$province<-0
    }
    
  } else {
    df$SALARY_CLIENT<-ifelse(is.na(df$salary),"0-1500",
                      ifelse(df$salary<1500,"0-1500","0-1500"))
    
    df$WORK_EXPERIENCE<-ifelse(is.na(df$work_experience),"0-4years",
                        ifelse(df$work_experience<48,"0-4years","4+years"))
    
    df$OWNERSHIP<-ifelse(is.na(df$ownership),"Other",
                  ifelse(df$ownership==1,"Own","Other"))
    
    df$AGE<-ifelse(is.na(df$age),"18-29",
            ifelse(df$age<30,"18-29","30+"))
    
    df$EDUCATION<-ifelse(is.na(df$education),"High_school/Basic",
                  ifelse(df$education%in%c(1,2),"University/Prof_training",
                                                "High_school/Basic"))
    
    df$GENDER<-ifelse(is.na(df$gender),"M",df$gender)

    
    df$MARITAL_STATUS<-ifelse(is.na(df$marital_status),"Other/NA",
                       ifelse(df$marital_status%in%c(2,4,5),"Married","Other/NA"))
    
    
    df$IMMIGRANT<-ifelse(substr(df$dni,1,1)%in%
                           as.character(c(0:9)),"Citizen","Immigrant")
    
  }
  return(df)
}


gen_pre_score_criteria <- function(df,type){
  if(type=="client"){
    df$SALARY <- as.factor(
      ifelse(is.na(df$salary_type),"<1800",
      ifelse(df$salary_type<=3,"<1800",">=1800")))
    
    df$AGE<-as.factor(
      ifelse(is.na(df$age_type),"0-35",
      ifelse(df$age_type<=2,"0-35",
      ifelse(df$age_type<=3,"36-54","55+"))))
    
    df$WORK_EXPERIENCE<-as.factor(
      ifelse(is.na(df$work_experience_type),"<=9_years",
      ifelse(df$work_experience_type<=4,"<=9_years",">9_years")))
    
    
    df$GENDER<-as.factor(ifelse(is.na(df$gender),"M",df$gender))
  } else {
    df$SALARY <- as.factor(
      ifelse(is.na(df$salary),"<1800",
      ifelse(df$salary<1800,"<1800",">=1800")))
    
    df$AGE<-as.factor(
      ifelse(is.na(df$age),"0-35",
      ifelse(df$age<=35,"0-35",
      ifelse(df$age<=54,"36-54","55+"))))
    
    df$WORK_EXPERIENCE<-as.factor(
      ifelse(is.na(df$work_experience),"<=9_years",
      ifelse(df$work_experience<=108,"<=9_years",">9_years")))
    
    nas<-which(is.na(df$gender))
    df$gender[nas]<-"M"
    df$GENDER<-df$gender
    df$gender[nas]<-NA
  }
  return(df)
}