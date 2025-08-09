# server_parallel.R
# This module contains the logic for the "Parallel Analysis" tab.
# It handles data upload, defining subpopulations, running multiple refineR models,
# and rendering the results for each subpopulation.

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
# It takes a data frame, gender choice, age range, and column names for gender and age.
# It returns a filtered data frame with a standardized 'Gender_Standardized' column.
filter_data <- function(data, gender_choice, age_min, age_max, col_gender, col_age) {
  if (col_age == "") {
    stop("Age column not found in data.")
  }

  filtered_data <- data %>%
    filter(!!rlang::sym(col_age) >= age_min & !!rlang::sym(col_age) <= age_max)

  # Check if a gender column is selected
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
    # If no gender column is selected, create a dummy 'Combined' gender column
    filtered_data <- filtered_data %>%
      mutate(Gender_Standardized = "Combined")
  }

  return(filtered_data)
}

# This function is a wrapper for a single refineR analysis,
# allowing us to easily apply it to multiple subpopulations.
run_single_refiner_analysis <- function(data, subpopulation, col_value, col_age, col_gender, model_choice) {
  gender <- subpopulation$gender
  age_min <- subpopulation$age_min
  age_max <- subpopulation$age_max
  label <- paste0(gender, " (", age_min, "-", age_max, ")")

  tryCatch({
    # Filter the data for the specific subpopulation
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
                             NBootstrap = 1, # Using fast bootstrap for demo
                             model = model_choice)

    if (is.null(model) || inherits(model, "try-error")) {
      stop(paste("RefineR model could not be generated for subpopulation:", label))
    }

    # Create plot and summary
    plot_title <- paste0("RefineR Analysis for ", label)

    # Store plot and summary
    plot_output <- reactive({
      req(model)
      plot(model, showCI = TRUE, RIperc = c(0.025, 0.975), showPathol = FALSE,
           title = plot_title,
           xlab = paste0(col_value, " (", col_value, ")"))
    })

    summary_output <- reactive({
      req(model)
      print(summary(model))
    })

    list(
      label = label,
      model = model,
      plot_output = plot_output,
      summary_output = summary_output,
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

    # Parse subpopulations from the new inputs
    male_ranges_text <- input$male_age_ranges
    female_ranges_text <- input$female_age_ranges

    subpopulations <- list()

    # Parse male age ranges
    if (male_ranges_text != "") {
      male_ranges <- strsplit(male_ranges_text, ",")[[1]]
      for (range in male_ranges) {
        parts <- trimws(strsplit(range, "-")[[1]])
        if (length(parts) == 2) {
          age_min <- as.numeric(parts[1])
          age_max <- as.numeric(parts[2])
          if (!is.na(age_min) && !is.na(age_max) && age_min <= age_max) {
            subpopulations <- c(subpopulations, list(list(gender = "Male", age_min = age_min, age_max = age_max)))
          }
        }
      }
    }

    # Parse female age ranges
    if (female_ranges_text != "") {
      female_ranges <- strsplit(female_ranges_text, ",")[[1]]
      for (range in female_ranges) {
        parts <- trimws(strsplit(range, "-")[[1]])
        if (length(parts) == 2) {
          age_min <- as.numeric(parts[1])
          age_max <- as.numeric(parts[2])
          if (!is.na(age_min) && !is.na(age_max) && age_min <= age_max) {
            subpopulations <- c(subpopulations, list(list(gender = "Female", age_min = age_min, age_max = age_max)))
          }
        }
      }
    }

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

    # Run analysis with a for loop to correctly manage progress
    results_list <- list()
    total_subpopulations <- length(subpopulations)
    withProgress(message = 'Running Parallel Analysis', value = 0, {
      for (i in 1:total_subpopulations) {
        sub <- subpopulations[[i]]
        # Update progress bar
        incProgress(1/total_subpopulations, detail = paste("Analyzing", sub$gender, "ages", sub$age_min, "to", sub$age_max, "..."))

        # Run the analysis for the current subpopulation
        result <- run_single_refiner_analysis(
          data = parallel_data_rv(),
          subpopulation = sub,
          col_value = input$parallel_col_value,
          col_age = input$parallel_col_age,
          col_gender = input$parallel_col_gender,
          model_choice = input$parallel_model_choice
        )
        results_list[[i]] <- result
      }
    })
    parallel_results_rv(results_list)

    # Finalize analysis
    analysis_running_rv(FALSE)
    shinyjs::enable("run_parallel_btn")
    shinyjs::runjs("$('#run_parallel_btn').text('Run Parallel Analysis');")
    session$sendCustomMessage('analysisStatus', FALSE)
    parallel_message_rv(list(text = "Parallel analysis complete!", type = "success"))
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

  # Dynamic UI to render results for each subpopulation
  output$parallel_results_ui <- renderUI({
    results <- parallel_results_rv()
    if (length(results) == 0) {
      return(NULL)
    }

    result_elements <- lapply(seq_along(results), function(i) {
      result <- results[[i]]
      if (result$status == "success") {
        tagList(
          h4(result$label),
          plotOutput(paste0("parallel_plot_", i)),
          verbatimTextOutput(paste0("parallel_summary_", i)),
          hr()
        )
      } else {
        div(class = "alert alert-danger", result$message)
      }
    })

    do.call(tagList, result_elements)
  })

  # Dynamic rendering of plots and summaries
  observe({
    results <- parallel_results_rv()
    if (length(results) > 0) {
      lapply(seq_along(results), function(i) {
        result <- results[[i]]
        if (result$status == "success") {
          output_id_plot <- paste0("parallel_plot_", i)
          output_id_summary <- paste0("parallel_summary_", i)

          output[[output_id_plot]] <- renderPlot({
            result$plot_output()
          })

          output[[output_id_summary]] <- renderPrint({
            result$summary_output()
          })
        }
      })
    }
  })
}