
#############################################################################
###### Functions to apply logistic regression on application Creditero ######
#############################################################################

gen_score_app <- function(binned_df,scoring_df,flag_repeat){
  if(binned_df$no_aggregations_flag<2){
    if(flag_repeat==1){
      #model <- repeated_customers_model
      model <- beh_application_model
    } else {
      model <- first_application_model
    }
    for(i in 1:nrow(scoring_df)){
      apply_logit <- predict(model, newdata=binned_df, type="response")
      scoring_df$score[i] <- apply_logit
      scoring_df$score[i] <- gen_group_scores(scoring_df$score[i],flag_repeat)
      scoring_df$pd[i] <- round(apply_logit,3)
      scoring_df$color[i] <- 0
      scoring_df$color[i] <- ifelse(scoring_df$color[i]==1 | 
                                                scoring_df$score[i]=="Bad", 1, 
              ifelse(scoring_df$score[i]=="Indeterminate", 6,
              ifelse(scoring_df$score[i]=="Good 1", 6,
              ifelse(scoring_df$score[i]=="Good 2", 6,
              ifelse(scoring_df$score[i]=="Good 3", 6,
              ifelse(scoring_df$score[i]=="Good 4", 6, NA))))))
    }
  } else {
    scoring_df$pd<-NA
    scoring_df$score<-"NULL"
    scoring_df$color<-2
  }
  return(scoring_df)
}

gen_client_prescore<-function(df,cutoffs,model,type){
  if(nrow(all_df)>0){
    df$pd<-predict(model,newdata=df, type="response")
    if(type=="client"){
      metrics<-c("age_type","gender","salary_type","work_experience_type")
    } else {
      metrics<-c("age","gender","salary","work_experience")
    }
    df$pd[which(rowSums(is.na(df[,metrics]))>1)]<-NA
    
    df$priority<-
      ifelse(is.na(df$pd),"NULL",
      ifelse(df$pd>cutoffs[1],"Indeterminate",
      ifelse(df$pd>cutoffs[2],"Good 1",
      ifelse(df$pd>cutoffs[3],"Good 2",
      ifelse(df$pd>cutoffs[4],"Good 3","Good 4")))))
    if(!"client_id"%in%names(df)){
      result<-df[,c("request_id","priority","pd")]
    } else {
      result<-df[,c("client_id","priority","pd")]
    }
  } else {
    result<-as.data.frame(matrix(data = NA,nrow = 0,ncol = 3))
    names(result)[2:3]<-c("priority","pd")
    names(result)[1]<-ifelse(type=="client","client_id","request_id")
  }
  
  return(result)
}

# gen_priority_color<-function(df,cutoffs){
#   df$color<-
#     ifelse(is.na(df$pd),1,
#     ifelse(df$priority=="Good 4",6,
#     ifelse(df$priority=="Good 3",5,
#     ifelse(df$priority=="Good 2",4,
#     ifelse(df$priority=="Good 1",3,
#     ifelse(df$priority=="Indeterminate",2,1))))))
#   return(df)
# }