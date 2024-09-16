library(jsonlite)
library(httr)
###################################################
######## Define some extra useful functions  ######
###################################################

# Define function to generate query
gen_query <- function(con,input){
  return(suppressWarnings(dbFetch(dbSendQuery(con,input))))
}

# Define function to get apply cutoffs
gen_group_scores <- function(var,flag_beh){
  if(!is.na(var)){
    if(flag_beh==1){
      cutoffs <- cu_repeat_app
    } else {
      cutoffs <- cu_first_app
    }
    if (var>cutoffs[1]){output="Bad"} 
    else if (var>cutoffs[2]) {output="Indeterminate"} 
    else if (var>cutoffs[3]) {output="Good 1"} 
    else if (var>cutoffs[4]) {output="Good 2"} 
    else if (var>cutoffs[5]) {output="Good 3"} 
    else {output="Good 4"}
    return (output)
  } else {
    return ("NULL")
  }
}

gen_scoring_df <- function(products,application_id,all_df){
  
  # Read table with number of payments/number of for city cash
  table_flex <- table(products$installments, products$amount)
  for (i in 1:nrow(table_flex)){
    for (j in 1:ncol(table_flex)){
      if (table_flex[i,j]>0){
        table_flex[i,j] <- row.names(table_flex)[i]
      } else {
        table_flex[i,j] <- NA}}
  }
  
  # Make dataframe of all possible amounts/installments
  vect_flex_installment <- sort(as.numeric(unique(unlist(table_flex))))
  vect_flex_amount <- colnames(table_flex, do.NULL = TRUE, 
                               prefix = "col")
  PD_flex <- matrix("", ncol = length(vect_flex_installment), 
                    nrow = length(vect_flex_amount))
  colnames(PD_flex) <- vect_flex_installment
  rownames(PD_flex) <- vect_flex_amount
  melted <- as.data.frame(melt(t(PD_flex)))
  names(melted) <- c("period","amount","value")
  melted$value <- as.numeric(melted$value)
  
  # Remove unneccessary rows from melted dataframe
  for(i in 1:nrow(melted)){
    c1 <- as.character(melted$period[i]) 
    c2 <- as.character(melted$amount[i])
    melted$value[i] <- ifelse(is.na(table_flex[c1,c2]),0,1)
  }
  scoring_df <- subset(melted, melted$value==1)[,1:2]
  names(scoring_df) <- c("period","amount")
  scoring_df$application_id <- application_id
  return(scoring_df)
}

# Function to create column for scoring table for display
gen_final_table_display <- function(scoring_df, requested_amount){
  scoring_df$display_score <- 
    ifelse(scoring_df$color == 1,"No",scoring_df$score)
  scoring_df$color <- ifelse(scoring_df$display_score=="No",1,
                             ifelse(scoring_df$display_score=="NULL",2, 6))
  names(scoring_df)[names(scoring_df) == 'application_id'] <- 'loan_id'
  names(scoring_df)[names(scoring_df) == 'period'] <- 'installments'
  
  scoring_df<-scoring_df[scoring_df$amount<=requested_amount,]
  return(scoring_df)
}

# Function to draw data Equifax report
merge_equifax_report <- function(application_df,equifax_report){
  if(nrow(equifax_report)==1){
    equifax_report<-as.list(equifax_report)
    equifax_report$data<-as.list(fromJSON(equifax_report$data,flatten = TRUE))
    application_df$equifax_unpaid_amount<-
                equifax_report$data$summaryInformation$totalUnpaidPaymentAmount
  } else {
    application_df$equifax_unpaid_amount<-NA
  }
  return(application_df)
}

gen_age<-function(application_date,dob){
  result<-NA
  if(!is.na(dob)){
    result<-as.numeric(substr(application_date,1,4))-as.numeric(substr(dob,1,4))
    result<-ifelse(substr(dob,6,10)>substr(application_date,6,10),result-1,result)
  }
  return(result)
}

gen_last_pre_score <- function (pre_scores_df,scores_df,client_id){
  names(scores_df)[which(names(scores_df)=="score")]<-"priority"
  if(nrow(scores_df)>0){
    final_df<-scores_df
    final_df$client_id<-client_id
    final_df<-final_df[order(final_df$loan_id,decreasing = T),]
  } else {
    final_df<-pre_scores_df
  }
  if(nrow(final_df)>0){
    return(final_df[1,c("client_id","priority","pd")])
  } else {
    return(final_df)
  }
}

unpack_raw_covariates_prescore<-function(request_data){
  request_data$payload<-gsub(" ","",all_df$payload)
  request_data$payload<-gsub("\n","",all_df$payload)
  content_list <- lapply(request_data$payload, fromJSON)[[1]]
  
  
  for(var in c("age","gender","incomes","work_term","dni","email","phone_number","iban")){
    new_var<-ifelse(var=="incomes","salary",
             ifelse(var=="work_term","work_experience",var))
    
    if(!var %in% names(content_list$client)){
      request_data[,new_var]<-NA
    } else if(is.null(content_list$client[var][[1]])){
      request_data[,new_var]<-NA
    } else if(is.na(content_list$client[var][[1]])){
      request_data[,new_var]<-NA
    } else {
      request_data[,new_var]<-content_list$client[var][[1]]
    }
    if(var %in% c("age","incomes","work_term")){
      request_data[,new_var]<-as.numeric(request_data[,new_var])
    }
    if(var=="gender" & (!request_data[,new_var]%in%c("M","F"))){
      request_data[,new_var]<-NA
    }
  }
  
  return(request_data)
}

# gen_sql_string_po_terminated <- function(input,inc){
#   return(paste("(",input$id[inc],",",
#     input$client_id[inc],",",input$product_id[inc],",",input$loan_id[inc],",",
#     input$credit_amount[inc],",'",input$created_at[inc],"','",input$valid_until[inc],
#     "',",input$updated_at[inc],",",input$deleted_at[inc],")",sep=""))
# }


gen_bankruptcy_http_request <- function(dni) {
  url <- paste0("https://www.publicidadconcursal.es/consulta-publicidad-",
  "concursal-new?p_p_id=org_registradores_rpc_concursal_web_ConcursalWebPortlet",
  "&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=%2Fafectado",
  "%2Fsearch&p_p_cacheability=cacheLevelPage")
  
  headers <- c(
    "Accept" = "application/json, text/javascript, */*; q=0.01",
    "Content-Type" = "application/x-www-form-urlencoded; charset=UTF-8",
    "Sec-Fetch-Dest" = "empty",
    "Sec-Fetch-Mode" = "cors",
    "Sec-Fetch-Site" = "same-origin",
    "X-Requested-With" = "XMLHttpRequest"
  )
  
  # Request body
  
  body <- paste0("draw=2&columns%5B0%5D%5Bdata%5D=afectado&columns%5B0%5D%5B",
   "name%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D",
   "=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%",
   "5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=identificador&columns%5B1%",
   "5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Bord",
   "erable%5D=false&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5",
   "Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=deudor&columns%5B2",
   "%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Bord",
   "erable%5D=false&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5",
   "Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=inhabilitado&column",
   "s%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%",
   "5Borderable%5D=false&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3",
   "%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=administrador",
   "&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%",
   "5B4%5D%5Borderable%5D=false&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&colu",
   "mns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=summary",
   "&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%",
   "5B5%5D%5Borderable%5D=false&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&colu",
   "mns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&start=0&length=10&search%5Bvalue",
   "%5D=&search%5Bregex%5D=false&_org_registradores_rpc_concursal_web_Concur",
   "salWebPortlet_formDate=1702900564269&_org_registradores_rpc_concursal_web",
   "_ConcursalWebPortlet_section1=true&_org_registradores_rpc_concursal_web_",
   "ConcursalWebPortlet_section2=true&_org_registradores_rpc_concursal_web_",
   "ConcursalWebPortlet_section3=true&_org_registradores_rpc_concursal_web_",
   "ConcursalWebPortlet_identificador=", dni, "&_org_registradores_rpc_con",
   "cursal_web_ConcursalWebPortlet_provincia=0&_org_registradores_rpc_concu",
   "rsal_web_ConcursalWebPortlet_codJuzgado=0&_org_registradores_rpc_concur",
   "sal_web_ConcursalWebPortlet_checkboxNames=section1%2Csection2%2Csection3")


  tryCatch({
    response <- httr::POST(url, httr::add_headers(.headers=headers), body=body, 
                           encode="form", timeout(60)) # 60 seconds timeout
    if (httr::status_code(response) != 200) {
      warning("Non-200 status code received: ", httr::status_code(response))
      return(0)
    }
    
    json_content <- httr::content(response, "parsed", type = "application/json")
    if (is.null(json_content$data)) {
      warning("Invalid or empty JSON response")
      return(0)
    }
    
    bankruptcy <- ifelse(as.integer(json_content$data$recordsTotal) > 0, 1, 0)
    return(bankruptcy)
  }, error = function(e) {
    warning("Error occurred: ", e$message)
    return(0)
  })
}

# Function to make string for DB update of PO terminated (delete offer)
gen_string_po_terminated <- function(input){
  string_sql_update <- input$id[1]
  if(nrow(input)>1){
    for(i in 2:nrow(input)){
      string_sql_update <- paste(string_sql_update,input$id[i],
                                 sep=",")}}
  return(paste("(",string_sql_update,")",sep=""))
}

# Define sql string query for writing in DB for PO terminated
gen_sql_string_po_terminated <- function(input,inc){
  return(paste("(",input$id[inc],",",
    input$client_id[inc],",",input$credit_amount[inc],",",
    input$credit_amount_updated[inc],",",input$installment_amount[inc],",",
    input$installment_amount_updated[inc],",",
    input$product_id[inc],",'",input$created_at[inc],"',",input$updated_at[inc],
    ",",input$deleted_at[inc],")",sep=""))
}

# Get correct address
get_correct_zip <- function(input){
  app_sql_data <- input[input$address_type %in% c(1, 2), ]
  app_sql_data$address_type <- as.numeric(as.character(app_sql_data$address_type))
  app_sql_data <- app_sql_data[order(app_sql_data$loan_id, 
                                     -app_sql_data$address_type), ]
  app_sql_data <- app_sql_data[!duplicated(app_sql_data$loan_id), ]
  app_sql_data <- app_sql_data[ , !(names(app_sql_data) %in% "address_type")]
  return(app_sql_data)
}
