# server_parallel.R
# This module contains the logic for the "Parallel Analysis" tab.
# It handles data upload, defining subpopulations, running multiple refineR models,
# and rendering the results for each subpopulation.

# Load all necessary libraries.
library(shiny)
library(readxl)
library(tidyverse)
library(refineR)
library(shinyjs)
library(shinyWidgets)
library(bslib)
library(ggplot2)
library(future)       # Added for future-based parallel processing
library(future.apply) # Added to use future_lapply

# =========================================================================
# UTILITY FUNCTIONS FOR PARALLEL ANALYSIS
# =========================================================================

# Helper function to guess column names based on common keywords
guess_column <- function(cols_available, common_names) {
  for (name in common_names) {
    match_idx <- grep(paste0("^", name, "$"), cols_available, ignore.case = TRUE)
    if (length(match_idx) > 0) {
      return(cols_available[match_idx[1]])
    }
  }
  return("")
}

# Function to filter data based on gender and age
filter_data <- function(data, gender_choice, age_min, age_max, col_gender, col_age) {
  if (col_age == "") {
    stop("Age column not found in data.")
  }

  filtered_data <- data %>%
    filter(!!rlang::sym(col_age) >= age_min & !!rlang::sym(col_age) <= age_max)

  if (col_gender != "" && col_gender %in% names(data)) {
    filtered_data <- filtered_data %>%
      mutate(Gender_Standardized = case_when(
        grepl("male|m|man|jongen(s)?|heren|mannelijk(e)?", !!rlang::sym(col_gender), ignore.case = TRUE) ~ "Male",
        grepl("female|f|vrouw(en)?|v|meisje(s)?|dame|mevr|vrouwelijke", !!rlang::sym(col_gender), ignore.case = TRUE) ~ "Female",
        TRUE ~ "Other"
      ))

    if (gender_choice != "Both") {
      filtered_data <- filtered_data %>%
        filter(Gender_Standardized == case_when(
          gender_choice == "M" ~ "Male",
          gender_choice == "F" ~ "Female"
        ))
    }
  } else {
    filtered_data <- filtered_data %>%
      mutate(Gender_Standardized = "Combined")
  }

  return(filtered_data)
}

# A new, more robust function to parse age ranges
parse_age_ranges <- function(ranges_text, gender) {
  ranges <- strsplit(ranges_text, ",")[[1]]
  parsed_ranges <- list()
  for (range in ranges) {
    parts <- trimws(strsplit(range, "-")[[1]])
    if (length(parts) == 2) {
      age_min <- as.numeric(parts[1])
      age_max <- as.numeric(parts[2])
      if (!is.na(age_min) && !is.na(age_max) && age_min <= age_max) {
        parsed_ranges <- c(parsed_ranges, list(list(gender = gender, age_min = age_min, age_max = age_max)))
      } else {
        warning(paste("Invalid numeric range for", gender, ":", range))
      }
    } else if (trimws(range) != "") {
      warning(paste("Invalid format for", gender, ":", range))
    }
  }
  return(parsed_ranges)
}

# This function is a wrapper for a single refineR analysis, now including bootstrap speed.
run_single_refiner_analysis <- function(subpopulation, data, col_value, col_age, col_gender, model_choice, nbootstrap_value) {
  gender <- subpopulation$gender
  age_min <- subpopulation$age_min
  age_max <- subpopulation$age_max
  label <- paste0(gender, " (", age_min, "-", age_max, ")")

  tryCatch({
    filtered_data <- filter_data(data,
                                 gender_choice = ifelse(gender == "Both", "Both", substr(gender, 1, 1)),
                                 age_min = age_min,
                                 age_max = age_max,
                                 col_gender = col_gender,
                                 col_age = col_age)

    if (nrow(filtered_data) == 0) {
      stop(paste("No data found for subpopulation:", label))
    }
    
    # Run the refineR model
    model <- refineR::findRI(Data = filtered_data[[col_value]],
                             NBootstrap = nbootstrap_value,
                             model = model_choice)

    if (is.null(model) || inherits(model, "try-error")) {
      stop(paste("RefineR model could not be generated for subpopulation:", label))
    }

    list(
      label = label,
      model = model,
      status = "success",
      message = "Analysis complete."
    )

  }, error = function(e) {
    list(
      label = label,
      status = "error",
      message = paste("Error:", e$message)
    )
  })
}

# Main server logic for the parallel tab
parallelServer <- function(input, output, session, parallel_data_rv, parallel_results_rv, parallel_message_rv, analysis_running_rv) {

  # Observer for file upload
  observeEvent(input$parallel_file, {
    req(input$parallel_file)
    tryCatch({
      data <- readxl::read_excel(input$parallel_file$datapath)
      parallel_data_rv(data)
      parallel_message_rv(list(type = "success", text = "Data file uploaded successfully."))

      col_names <- colnames(data)
      all_col_choices_with_none <- c("None" = "", col_names)

      updateSelectInput(session, "parallel_col_value", choices = all_col_choices_with_none, selected = guess_column(col_names, c("HB_value", "Value", "Result", "Measurement", "Waarde")))
      updateSelectInput(session, "parallel_col_age", choices = all_col_choices_with_none, selected = guess_column(col_names, c("leeftijd", "age", "AgeInYears", "Years")))
      updateSelectInput(session, "parallel_col_gender", choices = all_col_choices_with_none, selected = guess_column(col_names, c("geslacht", "gender", "sex", "Gender", "Sex")))
    }, error = function(e) {
      parallel_message_rv(list(type = "error", text = paste("Error loading file:", e$message)))
      parallel_data_rv(NULL)
    })
  })

  # Observer for the Run Parallel Analysis button
  observeEvent(input$run_parallel_btn, {
    if (analysis_running_rv()) {
      parallel_message_rv(list(text = "An analysis is already running. Please wait or reset.", type = "warning"))
      return()
    }

    # Pre-run checks
    req(parallel_data_rv(), input$parallel_col_value, input$parallel_col_age)
    if (input$parallel_col_value == "" || input$parallel_col_age == "") {
      parallel_message_rv(list(text = "Please select the value and age columns.", type = "error"))
      return()
    }
    
    subpopulations <- c(
      parse_age_ranges(input$male_age_ranges, "Male"),
      parse_age_ranges(input$female_age_ranges, "Female")
    )

    if (length(subpopulations) == 0) {
      parallel_message_rv(list(text = "Please enter valid age ranges in the format 'min-max' for at least one gender.", type = "error"))
      return()
    }

    # Prepare for analysis
    parallel_message_rv(list(text = "Starting parallel analysis...", type = "info"))
    analysis_running_rv(TRUE)
    shinyjs::disable("run_parallel_btn")
    shinyjs::runjs("$('#run_parallel_btn').text('Analyzing...');")
    session$sendCustomMessage('analysisStatus', TRUE)

    # Capture input values outside of the future_lapply call
    data_to_analyze <- parallel_data_rv()
    col_value_input <- input$parallel_col_value
    col_age_input <- input$parallel_col_age
    col_gender_input <- input$parallel_col_gender
    model_choice_input <- input$parallel_model_choice
    nbootstrap_value_input <- switch(input$parallel_nbootstrap_speed, "Fast" = 1, "Medium" = 50, "Slow" = 200, 1)

    # Set the parallelization plan based on user input for cores
    plan(multisession, workers = input$cores)

    # Use future_lapply for parallel processing without a progress bar.
    results_list <- future_lapply(subpopulations, function(sub) {
      run_single_refiner_analysis(
        subpopulation = sub,
        data = data_to_analyze,
        col_value = col_value_input,
        col_age = col_age_input,
        col_gender = col_gender_input,
        model_choice = model_choice_input,
        nbootstrap_value = nbootstrap_value_input
      )
    }, future.seed = TRUE)

    parallel_results_rv(results_list)

    # Finalize analysis
    analysis_running_rv(FALSE)
    shinyjs::enable("run_parallel_btn")
    shinyjs::runjs("$('#run_parallel_btn').text('Run Parallel Analysis');")
    session$sendCustomMessage('analysisStatus', FALSE)

    # Check if all tasks failed
    if (all(sapply(parallel_results_rv(), function(r) r$status == "error"))) {
      parallel_message_rv(list(text = "Parallel analysis failed for all subpopulations.", type = "error"))
    } else {
      parallel_message_rv(list(text = "Parallel analysis complete!", type = "success"))
    }
  })

  # Observer for the Reset button
  observeEvent(input$reset_parallel_btn, {
    parallel_data_rv(NULL)
    parallel_results_rv(list())
    parallel_message_rv(list(type = "", text = ""))
    shinyjs::reset("parallel_file")
    updateSelectInput(session, "parallel_col_value", choices = c("None" = ""), selected = "")
    updateSelectInput(session, "parallel_col_age", choices = c("None" = ""), selected = "")
    updateSelectInput(session, "parallel_col_gender", choices = c("None" = ""), selected = "")
    updateRadioButtons(session, "parallel_model_choice", selected = "BoxCox")
    updateTextAreaInput(session, "male_age_ranges", value = "")
    updateTextAreaInput(session, "female_age_ranges", value = "")
  })

  # Dynamic UI to render plots and summaries for each subpopulation
  output$parallel_results_ui <- renderUI({
    results <- parallel_results_rv()
    if (length(results) == 0) {
      return(NULL)
    }

    result_elements <- lapply(seq_along(results), function(i) {
      result <- results[[i]]
      if (result$status == "success") {
        # Split the label to get gender and age range parts
        label_parts <- unlist(strsplit(result$label, " "))
        gender_part <- label_parts[1]
        age_range_part <- gsub("[()]", "", label_parts[2])

        tagList(
          h4(paste0(input$parallel_col_value, " (Gender: ", gender_part, ", Age: ", age_range_part, ")")),
          plotOutput(paste0("parallel_plot_", i)),
          # Add the summary output directly below the plot for this subpopulation
          verbatimTextOutput(paste0("parallel_summary_", i)),
          hr()
        )
      } else {
        div(class = "alert alert-danger", result$message)
      }
    })

    do.call(tagList, result_elements)
  })

  # Dynamic rendering of plots and summaries in the main session
  observe({
    results <- parallel_results_rv()
    if (length(results) > 0) {
      lapply(seq_along(results), function(i) {
        result <- results[[i]]
        if (result$status == "success") {
          output_id_plot <- paste0("parallel_plot_", i)
          output_id_summary <- paste0("parallel_summary_", i)
          model <- result$model

          # Split the label to get gender and age range parts
          label_parts <- unlist(strsplit(result$label, " "))
          gender_part <- label_parts[1]
          age_range_part <- gsub("[()]", "", label_parts[2])
          
          # Create plot reactively in the main session
          output[[output_id_plot]] <- renderPlot({
            req(model)
            
            # Extract key information for title and axis labels
            value_col_name <- input$parallel_col_value
            model_type <- switch(input$parallel_model_choice,
                                 "BoxCox" = " (BoxCox Transformed)",
                                 "modBoxCox" = " (modBoxCox Transformed)")
            
            plot_title <- paste0("Estimated RI for ", value_col_name, model_type, " (Gender: ", gender_part, ", Age: ", age_range_part, ")")
            xlab_text <- paste0(value_col_name, " ", "[", input$parallel_unit_input, "]")
            
            plot(model, showCI = TRUE, RIperc = c(0.025, 0.975), showPathol = FALSE,
                 title = plot_title,
                 xlab = xlab_text)
          })

          # Create summary reactively in the main session
          output[[output_id_summary]] <- renderPrint({
              req(model)
              cat("--- RefineR Summary for ", input$parallel_col_value, " (Gender: ", gender_part, ", Age: ", age_range_part, ") ---\n")
              print(model)
          })
        }
      })
    }
  })
}