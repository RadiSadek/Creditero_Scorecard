library(lubridate)
library(jsonlite)

############################
######## Policy rules ######
############################

gen_age_flag <- function(all_df){
  df<-all_df
  df$age_flag<-ifelse(df$age<=23,1,0)
  return(df)
}

gen_transactions_flag <- function(all_df){
  if(all_df$no_aggregations_flag<2){
    all_df$transactions_flag<-ifelse(
      is.na(all_df$transactions),1,
      ifelse(all_df$transactions<=10,1,0)
    )
  } else {
    all_df$transactions_flag<-1
  }
  return(all_df)
}

gen_debt_restructuring_flag <- function(all_df,report_id,all_bank_reports){
  
  #######list of bad companies
  bad_companies<-c("REPAGALIA",
                   "DEUDAFIX",
                   "RECLAMA POR MI",
                   "RESUELVE TU DEUDA",
                   "REPARADORA",
                   "SOLUCIONA MI DEUDA",
                   "SEGUNDA OPORTUNIDAD",
                   "RPG",
                   "RTD",
                   "FINTECH LABORATORY",
                   "PREICO",
                   "Arriaga Asociados")
  
  flag_companies_high_risk<-c("RGC",
                              "Renta Garantizada de Ciudadania",
                              "GGI",
                              "Renta de Garantia de Ingresos",
                              "IMV",
                              "Ingreso Minimo Vital")
  
  flag_companies_low_risk<-c("Desempleo",
                             "INEM",
                             "SPEE",
                             "Unemployment Benefit",
                             "Social Security Benefit",
                             "RETROC REC BD",
                             "GESTION DE DEVOLUCION",
                             "CUOTA IMPAGADA",
                             "Embargos")
  
  #######
  if(!is.null(report_id)){
    report<-all_bank_reports[which(all_bank_reports$id==report_id),]
    report$debt_restructuring_flag<-0
    
    report<-cbind(report[,which(names(report)!="data")],as.data.frame(lapply(
      report$data,function(j) as.list(unlist(fromJSON(j, flatten=TRUE))))[[1]]))
    #low_risk
    for(i in 1:length(flag_companies_low_risk)){
      company<-flag_companies_low_risk[i]
      words_company<-strsplit(company,split = " ")[[1]]
      debt_restruct_indicators<-logical(length(words_company))
      for (j in 1:length(words_company)){
        debt_restruct_indicators[j]<-
          all(grepl(pattern = words_company[j],x=report[which(
            grepl(pattern = "statements.concepts",x=names(report),fixed = TRUE))
          ],ignore.case = TRUE),na.rm = TRUE)
      }
      report$debt_restructuring_flag<-ifelse(
        all(debt_restruct_indicators), 3, report$debt_restructuring_flag
      )
      if(report$debt_restructuring_flag==3){
        break
      }
    }
    #high_risk
    for(i in 1:length(flag_companies_high_risk)){
      company<-flag_companies_high_risk[i]
      words_company<-strsplit(company,split = " ")[[1]]
      debt_restruct_indicators<-logical(length(words_company))
      for (j in 1:length(words_company)){
        debt_restruct_indicators[j]<-
          all(grepl(pattern = words_company[j],x=report[which(
            grepl(pattern = "statements.concepts",x=names(report),fixed = TRUE))
          ],ignore.case = TRUE),na.rm = TRUE)
      }
      report$debt_restructuring_flag<-ifelse(
        all(debt_restruct_indicators), 2, report$debt_restructuring_flag
      )
      if(report$debt_restructuring_flag==2){
        break
      }
    }
    #auto-rejects
    for(i in 1:length(bad_companies)){
      company<-bad_companies[i]
      words_company<-strsplit(company,split = " ")[[1]]
      debt_restruct_indicators<-logical(length(words_company))
      for (j in 1:length(words_company)){
        debt_restruct_indicators[j]<-
          all(grepl(pattern = words_company[j],x=report[which(
            grepl(pattern = "statements.concepts",x=names(report),fixed = TRUE))
          ],ignore.case = TRUE),na.rm = TRUE)
      }
      report$debt_restructuring_flag<-ifelse(
        all(debt_restruct_indicators), 1, report$debt_restructuring_flag
      )
      if(report$debt_restructuring_flag==1){
        break
      }
    }
    all_df<-merge(all_df,report[c("id","debt_restructuring_flag")],
                      by.x="aggregations_id",by.y="id")
  } else {
    all_df$debt_restructuring_flag<-NA
  }
  return(all_df)
}

gen_equifax_flag<-function(application_df){
  application_df$equifax_flag<-0
  if(is.na(application_df$equifax_unpaid_amount)){
    application_df$equifax_flag<-NA
  } else if(application_df$equifax_unpaid_amount>0){
    application_df$equifax_flag<-1
  }
  return(application_df)
}

gen_affiliate_flag<-function(application_df){
  application_df$affiliate_flag<-ifelse(is.na(application_df$source),0,
         ifelse(application_df$source%in%c("google_search","facebook"),0,1))
  return(application_df)
}

# Function to apply restrictions for first application scorecard
# (must have color and score fields)
policy_rules <- function(scoring_df,flag_beh,max_amount){
  
  ##### deny clients of loan management companies
  scoring_df$color<-ifelse(
    scoring_df$debt_restructuring_flag==1,1,scoring_df$color
  )
  
  ##### deny clients with less than 10 transactions
  scoring_df$color <- ifelse(
    scoring_df$transactions_flag==1,1,scoring_df$color)
  
  ##### Equifax debt
  scoring_df$color <- ifelse(
    is.na(scoring_df$equifax_flag),scoring_df$color,
    ifelse(scoring_df$equifax_flag==1,1,scoring_df$color)
  )
  
  ##### New clients with old reports
  if(flag_beh==0){
    scoring_df$color <-
      ifelse(scoring_df$no_aggregations_flag==1,1,scoring_df$color)
  }
  
  ##### age
  if(flag_beh==0){
    scoring_df$color<-ifelse(is.na(scoring_df$age_flag),scoring_df$color,
                      ifelse(scoring_df$age_flag==1,1,scoring_df$color))
  }
  #####max_amounts
  if (flag_beh==0){
    #amounts<-c(500,400,300,200,100)
    scoring_df$color <- 
      ifelse(scoring_df$score %in% c("NULL"),scoring_df$color,
      ifelse(scoring_df$score %in% c("Good 4") & scoring_df$amount>300,1,
      ifelse(scoring_df$score %in% c("Good 3") & scoring_df$amount>300,1,      
      ifelse(scoring_df$score %in% c("Good 2") & scoring_df$amount>200,1, 
      ifelse(scoring_df$score %in% c("Good 1") & scoring_df$amount>100,1,
      ifelse(scoring_df$score %in% c("Indeterminate") & scoring_df$amount>100,1,
                                                          scoring_df$color))))))
  }
  if (flag_beh==1){
    scoring_df$step<-scoring_df$amount-max_amount
    max_step<-c(200,150,100,100,0)
    scoring_df$color <- 
      ifelse(scoring_df$score %in% c("NULL"),scoring_df$color,
      ifelse(scoring_df$score %in% c("Good 4") & scoring_df$step>max_step[1],1,
      ifelse(scoring_df$score %in% c("Good 3") & scoring_df$step>max_step[2],1,      
      ifelse(scoring_df$score %in% c("Good 2") & scoring_df$step>max_step[3],1, 
      ifelse(scoring_df$score %in% c("Good 1") & scoring_df$step>max_step[4],1,
      ifelse(scoring_df$score %in% c("Indeterminate") & 
                  scoring_df$step>max_step[5],1,scoring_df$color))))))
  }
  
  return(scoring_df[,which(names(scoring_df)!="step")])
}