# server.R

# Load all necessary libraries.
library(shiny)
library(readxl)
library(tidyverse)
library(mclust)
library(moments)
library(shinyjs)
library(car)
library(refineR)
library(shinyFiles)
library(shinyWidgets)
library(bslib)
library(ggplot2)

# Source the server modules for each tab
source("server_main.R")
source("server_gmm.R")

server <- function(input, output, session) {
  
  # --- Reactive Values for State Management (Centralized) ---
  # These reactive values manage the application's state and are shared across all modules
  data_reactive <- reactiveVal(NULL)
  gmm_uploaded_data_rv <- reactiveVal(NULL)
  gmm_processed_data_rv <- reactiveVal(NULL)
  gmm_transformation_details_rv <- reactiveVal(list(male_hgb_transformed = FALSE, female_hgb_transformed = FALSE))
  selected_dir_reactive <- reactiveVal(NULL)
  message_rv <- reactiveVal(list(type = "", text = ""))
  analysis_running_rv <- reactiveVal(FALSE)

  # --- Centralized Message Display ---
  # Renders a UI element to display application-wide messages
  output$app_message <- renderUI({
    msg <- message_rv()
    if (is.null(msg) || msg$text == "") {
      return(NULL)
    }
    class_name <- switch(msg$type,
                         "error" = "alert alert-danger",
                         "success" = "alert alert-success",
                         "warning" = "alert alert-warning",
                         "info" = "alert alert-info",
                         "alert alert-secondary")
    div(class = class_name, msg$text)
  })

  # --- Global Tab Switching Logic ---
  # An observer that prevents switching tabs when an analysis is running, providing a user message
  observeEvent(input$tabs, {
    if (!analysis_running_rv()) {
      message_rv(list(type = "", text = ""))
    } else {
      message_rv(list(text = "Tab switch blocked: An analysis is currently running. Please wait or reset the analysis.", type = "warning"))
    }
  })

  # Listen for a custom message from the UI when a blocked tab is clicked
  observeEvent(input$tab_switch_blocked, {
    if (analysis_running_rv()) {
      message_rv(list(text = "Cannot switch tabs while an analysis is running. Please wait or reset the analysis.", type = "warning"))
    }
  })
  
  # Call the modular server functions for each tab
  mainServer(input, output, session, data_reactive, selected_dir_reactive, message_rv, analysis_running_rv)
  gmmServer(input, output, session, gmm_uploaded_data_rv, gmm_processed_data_rv, gmm_transformation_details_rv, message_rv, analysis_running_rv)
}