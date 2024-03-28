library(lubridate)

##############################################################
######## Functions to join bank report with application ######
##############################################################

application_wtih_aggs_data<-function(application_df,binned_aggs_df){
  if(!is.na(binned_aggs_df[1])){
    names(binned_aggs_df)[which(names(binned_aggs_df)=="id")]<-"aggregations_id"
    names(binned_aggs_df)[which(names(binned_aggs_df)=="created_at")]<-
                                                          "bank_report_created_at"
    names(application_df)[which(names(application_df)=="id")]<-"loan_id"
    df<-merge(application_df,binned_aggs_df[,which(names(binned_aggs_df)!="dni")],
              by="client_id")
    df$no_aggregations_flag<-0
    if(as.numeric(difftime(df$created_at,
                                df$bank_report_created_at,units = "days"))>=45){
      df$no_aggregations_flag<-1
    }
    return(df)
  } else {
    application_df$no_aggregations_flag<-2
    return(application_df)
  }
}