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

  # First, apply age filtering
  filtered_data <- data %>%
    filter(!!rlang::sym(col_age) >= age_min & !!rlang::sym(col_age) <= age_max)

  # Handle gender filtering
  if (col_gender != "" && col_gender %in% names(data)) {
    # If a gender column is provided and exists, standardize it
    filtered_data <- filtered_data %>%
      mutate(Gender_Standardized = case_when(
        grepl("male|m|man|jongen(s)?|heren|mannelijk(e)?", !!rlang::sym(col_gender), ignore.case = TRUE) ~ "Male",
        grepl("female|f|vrouw(en)?|v|meisje(s)?|dame|mevr|vrouwelijke", !!rlang::sym(col_gender), ignore.case = TRUE) ~ "Female",
        TRUE ~ "Other" # For values that don't match Male/Female
      ))

    # Apply gender-specific filter if needed (i.e., not "Both" genders selected for analysis)
    if (gender_choice != "Both") {
      filtered_data <- filtered_data %>%
        filter(Gender_Standardized == case_when(
          gender_choice == "M" ~ "Male",
          gender_choice == "F" ~ "Female",
          TRUE ~ NA_character_ # Should not happen if gender_choice is M or F
        )) %>%
        # Remove any rows where Gender_Standardized became NA due to a mismatch
        filter(!is.na(Gender_Standardized))
    }
  } else {
    # If no gender column is selected OR found in the data:
    # If the user requested 'Male' or 'Female' specific subpopulations, but no gender column
    # is available to filter by, this is an invalid request for gender-specific data.
    # In such cases, return an empty data frame to indicate no data for this subpopulation.
    if (gender_choice %in% c("M", "F")) {
      return(data[FALSE, ]) # Return an empty data frame with original columns
    } else {
      # If 'Both' (i.e., 'Combined') was requested and no gender column is provided,
      # simply assign 'Combined' as the gender for all data that passed age filtering.
      filtered_data <- filtered_data %>%
        mutate(Gender_Standardized = "Combined")
    }
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
    # Determine the gender_choice string expected by filter_data based on the subpopulation's gender.
    # If subpopulation gender is "Male", filter_data expects "M". If "Female", expects "F". If "Combined", expects "Both".
    filter_gender_choice <- ifelse(gender == "Male", "M", ifelse(gender == "Female", "F", "Both"))

    filtered_data_for_refiner <- filter_data(data,
                                 gender_choice = filter_gender_choice, # Use the derived gender_choice
                                 age_min = age_min,
                                 age_max = age_max,
                                 col_gender = col_gender,
                                 col_age = col_age)

    # Store the unfiltered (but age/gender-selected) data in the result for plotting
    raw_subpopulation_data <- filter_data(data,
                                          gender_choice = filter_gender_choice,
                                          age_min = age_min,
                                          age_max = age_max,
                                          col_gender = col_gender,
                                          col_age = col_age) %>%
                                    rename(Age = !!rlang::sym(col_age), Value = !!rlang::sym(col_value)) %>%
                                    mutate(label = label) # Add the label column here
    
    if (nrow(filtered_data_for_refiner) == 0) {
      stop(paste("No data found for subpopulation:", label, "after filtering."))
    }
    
    # Run the refineR model
    model <- refineR::findRI(Data = filtered_data_for_refiner[[col_value]],
                             NBootstrap = nbootstrap_value,
                             model = model_choice)

    if (is.null(model) || inherits(model, "try-error")) {
      stop(paste("RefineR model could not be generated for subpopulation:", label))
    }
    
    # Get the reference interval values.
    # The `fullDataEst` is used for the point estimate to align with the plot,
    # and the `medianBS` is used to get the more robust confidence intervals.
    ri_data_fulldata <- getRI(model, RIperc = c(0.025, 0.975), pointEst = "fullDataEst")
    ri_data_median <- getRI(model, RIperc = c(0.025, 0.975), pointEst = "medianBS")

    ri_low_fulldata <- ri_data_fulldata$PointEst[ri_data_fulldata$Percentile == 0.025]
    ri_high_fulldata <- ri_data_fulldata$PointEst[ri_data_fulldata$Percentile == 0.975]
    
    ci_low_low <- ri_data_median$CILow[ri_data_median$Percentile == 0.025]
    ci_low_high <- ri_data_median$CIHigh[ri_data_median$Percentile == 0.025]
    ci_high_low <- ri_data_median$CILow[ri_data_median$Percentile == 0.975]
    ci_high_high <- ri_data_median$CIHigh[ri_data_median$Percentile == 0.975]


    list(
      label = label,
      model = model, # Keep full model for individual summary/plot
      raw_data = raw_subpopulation_data, # Store the raw data for density plots
      age_min = age_min,
      age_max = age_max,
      ri_low_fulldata = ri_low_fulldata,
      ri_high_fulldata = ri_high_fulldata,
      ci_low_low = ci_low_low,
      ci_low_high = ci_low_high,
      ci_high_low = ci_high_low,
      ci_high_high = ci_high_high,
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
  
  # Reactive value to store all raw data from successful analyses for plotting
  combined_raw_data_rv <- reactiveVal(tibble())
  
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
      parse_age_ranges(input$female_age_ranges, "Female"),
      parse_age_ranges(input$combined_age_ranges, "Combined")
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

    # Update reactive values with results
    parallel_results_rv(results_list)

    # Gather all raw data from successful analyses for plotting
    raw_data_list <- lapply(results_list, function(r) {
      if (r$status == "success") {
        return(r$raw_data)
      } else {
        return(NULL)
      }
    })
    
    # Combine all data into a single tibble, ignoring NULLs
    combined_raw_data_rv(bind_rows(raw_data_list))

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
    combined_raw_data_rv(tibble()) # Reset the raw data reactive value
    parallel_message_rv(list(type = "", text = ""))
    shinyjs::reset("parallel_file")
    updateSelectInput(session, "parallel_col_value", choices = c("None" = ""), selected = "")
    updateSelectInput(session, "parallel_col_age", choices = c("None" = ""), selected = "")
    updateSelectInput(session, "parallel_col_gender", choices = c("None" = ""), selected = "")
    updateRadioButtons(session, "parallel_model_choice", selected = "BoxCox")
    updateTextAreaInput(session, "male_age_ranges", value = "")
    updateTextAreaInput(session, "female_age_ranges", value = "")
    updateTextAreaInput(session, "combined_age_ranges", value = "")
  })
  
  # Reactive expression to create a single combined table
  combined_summary_table <- reactive({
    results <- parallel_results_rv()
    if (is.null(results) || length(results) == 0) {
      return(NULL)
    }

    # Initialize an empty list to store rows
    table_rows <- list()

    for (result in results) {
      if (result$status == "success") {
        # Create a new row for this subpopulation
        new_row <- tibble(
          Subpopulation = result$label,
          age_min = result$age_min,
          age_max = result$age_max,
          `RI Lower Limit` = round(result$ri_low_fulldata, 3),
          `CI Lower Limit (LowerCI)` = round(result$ci_low_low, 3),
          `CI Lower Limit (UpperCI)` = round(result$ci_low_high, 3),
          `RI Upper Limit` = round(result$ri_high_fulldata, 3),
          `CI Upper Limit (LowerCI)` = round(result$ci_high_low, 3),
          `CI Upper Limit (UpperCI)` = round(result$ci_high_high, 3),
          Model = input$parallel_model_choice,
          Bootstraps = input$parallel_nbootstrap_speed
        )
        
        # Add the new row to our list of rows
        table_rows[[length(table_rows) + 1]] <- new_row
      }
    }

    # Combine all rows into a single tibble
    if (length(table_rows) > 0) {
      bind_rows(table_rows)
    } else {
      NULL
    }
  })

  # Dynamic UI to render the single combined table, followed by individual plots and summaries
  output$parallel_results_ui <- renderUI({
    results <- parallel_results_rv()
    
    # Render the combined table first, if it exists
    combined_table_data <- combined_summary_table()
    
    ui_elements <- tagList()
    
    if (!is.null(combined_table_data)) {
      ui_elements <- tagList(
        ui_elements,
        h4("Combined Summary of Reference Intervals"),
        renderTable(combined_table_data),
        div(class = "spacing-div"),
        hr()
      )
    }

    if (length(results) > 0) {
      # Then, render the individual plots and summaries
      individual_elements <- lapply(seq_along(results), function(i) {
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
            div(class = "spacing-div"),
            hr()
          )
        } else {
          div(class = "alert alert-danger", result$message)
        }
      })
      ui_elements <- tagList(ui_elements, do.call(tagList, individual_elements))
    }

    if (length(ui_elements) > 0) {
      ui_elements
    } else {
      NULL
    }
  })

  # UPDATED: Render the enhanced dumbbell plot with confidence intervals
# UPDATED: Render the enhanced dumbbell plot with confidence intervals
output$combined_dumbbell_plot <- renderPlot({
    plot_data <- combined_summary_table()

    if (is.null(plot_data) || nrow(plot_data) == 0) {
      return(ggplot2::ggplot() + ggplot2::annotate("text", x = 0.5, y = 0.5, label = "No successful reference intervals to plot.", size = 6, color = "grey50"))
    }

    unit_label <- if (!is.null(input$parallel_unit_input) && input$parallel_unit_input != "") {
      paste0("Value [", input$parallel_unit_input, "]")
    } else {
      "Value"
    }

    plot_data <- plot_data %>%
      mutate(gender = str_extract(Subpopulation, "^\\w+"))

    gender_colors <- c("Male" = "steelblue", "Female" = "darkred", "Combined" = "darkgreen")

    ggplot2::ggplot(plot_data, ggplot2::aes(y = Subpopulation)) +
      # Use geom_rect to create a shaded box for the entire CI range
      ggplot2::geom_rect(ggplot2::aes(xmin = `CI Lower Limit (LowerCI)`,
                                      xmax = `CI Upper Limit (UpperCI)`,
                                      ymin = as.numeric(factor(Subpopulation)) - 0.25,
                                      ymax = as.numeric(factor(Subpopulation)) + 0.25,
                                      fill = gender),
                         alpha = 0.2, show.legend = FALSE) +

      # Add a horizontal line for the Reference Interval (RI) itself
      ggplot2::geom_segment(ggplot2::aes(x = `RI Lower Limit`, xend = `RI Upper Limit`, y = Subpopulation, yend = Subpopulation, color = gender),
                            linewidth = 1) +

      # Add points for the RI limits (the start and end of the RI line)
      ggplot2::geom_point(ggplot2::aes(x = `RI Lower Limit`, color = gender), shape = 18, size = 4) +
      ggplot2::geom_point(ggplot2::aes(x = `RI Upper Limit`, color = gender), shape = 18, size = 4) +

      ggplot2::labs(
        title = "Combined Reference Intervals with Confidence Intervals",
        x = unit_label,
        y = "Subpopulation",
        color = "Gender",
        fill = "Gender (95% CI)"
      ) +
      ggplot2::theme_minimal() +
      ggplot2::scale_color_manual(values = gender_colors) +
      ggplot2::scale_fill_manual(values = gender_colors) +
      ggplot2::theme(
        plot.title = ggplot2::element_text(size = 18, face = "bold", hjust = 0.5),
        axis.title = ggplot2::element_text(size = 14),
        axis.text = ggplot2::element_text(size = 12),
        legend.title = ggplot2::element_text(size = 12, face = "bold"),
        legend.text = ggplot2::element_text(size = 10),
        legend.position = "bottom"
      )
  })

  # UPDATED: Render the faceted density plot
  output$combined_density_plot <- renderPlot({
    plot_data <- combined_raw_data_rv()
    results <- parallel_results_rv()
    
    if (is.null(plot_data) || nrow(plot_data) == 0) {
      return(ggplot2::ggplot() + ggplot2::annotate("text", x = 0.5, y = 0.5, label = "No data available for plotting.", size = 6, color = "grey50"))
    }
    
    # Ensure there are at least two subpopulations to facet by
    if (length(unique(plot_data$Gender_Standardized)) < 1 || length(unique(plot_data$label)) < 1) {
       return(ggplot2::ggplot() + ggplot2::annotate("text", x = 0.5, y = 0.5, label = "Insufficient data to create a faceted plot.", size = 6, color = "grey50"))
    }

    unit_label <- if (!is.null(input$parallel_unit_input) && input$parallel_unit_input != "") {
      paste0("Value [", input$parallel_unit_input, "]")
    } else {
      "Value"
    }
    
    # Prepare data for RI lines
    ri_lines <- tibble()
    for (result in results) {
      if (result$status == "success") {
        ri_lines <- bind_rows(ri_lines, tibble(
          label = result$label,
          ri_low = result$ri_low_fulldata,
          ri_high = result$ri_high_fulldata
        ))
      }
    }

    # The raw data has a `Gender_Standardized` column, but `label` is the combined gender + age
    ggplot2::ggplot(plot_data, ggplot2::aes(x = Value, fill = Gender_Standardized)) +
      ggplot2::geom_density(alpha = 0.6) +
      ggplot2::geom_vline(data = ri_lines, ggplot2::aes(xintercept = ri_low), linetype = "dashed", color = "darkred", size = 1) +
      ggplot2::geom_vline(data = ri_lines, ggplot2::aes(xintercept = ri_high), linetype = "dashed", color = "darkred", size = 1) +
      ggplot2::facet_wrap(~label, scales = "free_y") +
      ggplot2::labs(title = "Value Distribution by Subpopulation",
                    x = unit_label,
                    y = "Density",
                    fill = "Gender") +
      ggplot2::theme_minimal() +
      ggplot2::theme(
        plot.title = ggplot2::element_text(size = 18, face = "bold", hjust = 0.5),
        strip.text = ggplot2::element_text(size = 12),
        axis.title = ggplot2::element_text(size = 14),
        axis.text = ggplot2::element_text(size = 12),
        legend.title = ggplot2::element_text(size = 12, face = "bold"),
        legend.text = ggplot2::element_text(size = 10)
      )
  })

  # UPDATED: Render the grouped box plot
  output$combined_box_plot <- renderPlot({
    plot_data <- combined_raw_data_rv()
    
    if (is.null(plot_data) || nrow(plot_data) == 0) {
      return(ggplot2::ggplot() + ggplot2::annotate("text", x = 0.5, y = 0.5, label = "No data available for plotting.", size = 6, color = "grey50"))
    }
    
    unit_label <- if (!is.null(input$parallel_unit_input) && input$parallel_unit_input != "") {
      paste0("Value [", input$parallel_unit_input, "]")
    } else {
      "Value"
    }

    ggplot2::ggplot(plot_data, ggplot2::aes(x = reorder(label, Value, FUN = median), y = Value, fill = Gender_Standardized)) +
      ggplot2::geom_boxplot(alpha = 0.7, outlier.colour = "red", outlier.shape = 8) +
      ggplot2::labs(
        title = "Summary of Value Distribution by Subpopulation",
        x = "Subpopulation",
        y = unit_label,
        fill = "Gender"
      ) +
      ggplot2::coord_flip() +
      ggplot2::theme_minimal() +
      ggplot2::theme(
        plot.title = ggplot2::element_text(size = 18, face = "bold", hjust = 0.5),
        axis.title = ggplot2::element_text(size = 14),
        axis.text = ggplot2::element_text(size = 12),
        legend.title = ggplot2::element_text(size = 12, face = "bold"),
        legend.text = ggplot2::element_text(size = 10)
      )
  })

  # UPDATED: Render the age-stratified reference interval plot
  output$combined_ri_plot <- renderPlot({
    plot_data <- combined_summary_table()
    
    if (is.null(plot_data) || nrow(plot_data) == 0) {
      return(ggplot2::ggplot() + ggplot2::annotate("text", x = 0.5, y = 0.5, label = "No successful reference intervals to plot.", size = 6, color = "grey50"))
    }
    
    unit_label <- if (!is.null(input$parallel_unit_input) && input$parallel_unit_input != "") {
      paste0("Value [", input$parallel_unit_input, "]")
    } else {
      "Value"
    }

    plot_data <- plot_data %>%
      mutate(gender = str_extract(Subpopulation, "^\\w+"))
    
    gender_colors <- c("Male" = "steelblue", "Female" = "darkred", "Combined" = "darkgreen")

    ggplot2::ggplot(plot_data) +
      # Add shaded ribbon for the Confidence Interval
      ggplot2::geom_ribbon(ggplot2::aes(x = age_min, xmax = age_max, ymin = `CI Lower Limit (LowerCI)`, ymax = `CI Upper Limit (UpperCI)`, fill = gender),
                           alpha = 0.2) +
      # Add horizontal line for the Reference Interval (lower limit)
      ggplot2::geom_segment(ggplot2::aes(x = age_min, xend = age_max, y = `RI Lower Limit`, yend = `RI Lower Limit`, color = gender),
                            linewidth = 1.2, linetype = "solid") +
      # Add horizontal line for the Reference Interval (upper limit)
      ggplot2::geom_segment(ggplot2::aes(x = age_min, xend = age_max, y = `RI Upper Limit`, yend = `RI Upper Limit`, color = gender),
                            linewidth = 1.2, linetype = "solid") +
      # Add a point at the end of each segment to mark the age range
      ggplot2::geom_point(ggplot2::aes(x = age_min, y = `RI Lower Limit`, color = gender), size = 2) +
      ggplot2::geom_point(ggplot2::aes(x = age_max, y = `RI Lower Limit`, color = gender), size = 2) +
      ggplot2::geom_point(ggplot2::aes(x = age_min, y = `RI Upper Limit`, color = gender), size = 2) +
      ggplot2::geom_point(ggplot2::aes(x = age_max, y = `RI Upper Limit`, color = gender), size = 2) +
      ggplot2::labs(
        title = "Age-Stratified Reference Intervals by Subpopulation",
        x = "Age",
        y = unit_label,
        color = "Gender",
        fill = "Gender (95% CI)"
      ) +
      ggplot2::scale_x_continuous(limits = c(0, 120)) +
      ggplot2::scale_color_manual(values = gender_colors) +
      ggplot2::scale_fill_manual(values = gender_colors, guide = "none") + # Hide legend for the fill
      ggplot2::theme_minimal() +
      ggplot2::theme(
        plot.title = ggplot2::element_text(size = 18, face = "bold", hjust = 0.5),
        axis.title = ggplot2::element_text(size = 14),
        axis.text = ggplot2::element_text(size = 12),
        legend.title = ggplot2::element_text(size = 12, face = "bold"),
        legend.text = ggplot2::element_text(size = 10),
        legend.position = "bottom"
      )
  })
  
  # Renders the combined text summary for all successful subpopulations
  output$combined_summary <- renderPrint({
    results <- parallel_results_rv()
    if (is.null(results) || length(results) == 0) {
      cat("No parallel analysis results to summarize yet.")
      return(NULL)
    }
    
    cat("--- Combined Summary of Reference Intervals ---\n\n")

    has_successful_results <- FALSE
    for (r in results) {
      if (r$status == "success") {
        has_successful_results <- TRUE
        
        # Use full data estimate for consistency with the plot
        ri_low <- r$ri_low_fulldata
        ri_high <- r$ri_high_fulldata
        
        # Get the medianBS values for the CI summary
        ci_low_low <- r$ci_low_low
        ci_low_high <- r$ci_low_high
        ci_high_low <- r$ci_high_low
        ci_high_high <- r$ci_high_high
        
        cat(paste0("Subpopulation: ", r$label, "\n"))
        cat(paste0("  Estimated RI Lower Limit: ", round(ri_low, 3), "\n"))
        cat(paste0("  Confidence Interval for Lower Limit: [", round(ci_low_low, 3), ", ", round(ci_low_high, 3), "]\n"))
        cat(paste0("  Estimated RI Upper Limit: ", round(ri_high, 3), "\n"))
        cat(paste0("  Confidence Interval for Upper Limit: [", round(ci_high_low, 3), ", ", round(ci_high_high, 3), "]\n"))
        cat(paste0("  Transformation Model: ", input$parallel_model_choice, "\n"))
        if (!is.null(input$parallel_unit_input) && input$parallel_unit_input != "") {
          cat(paste0("  Unit of Measurement: ", input$parallel_unit_input, "\n"))
        }
        cat("\n")
      } else if (r$status == "error") {
        cat(paste0("Subpopulation: ", r$label, " (Analysis Failed)\n"))
        cat(paste0("  Reason: ", r$message, "\n\n"))
      }
    }

    if (!has_successful_results) {
      cat("No successful reference intervals were found to summarize.\n")
    }
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
            
            plot_title <- paste0("Estimated Reference Intervals for ", value_col_name, 
                                 model_type, " (Gender: ", gender_part, ", Age: ", age_range_part, ")")
            
            # Ensure parallel_unit_input is not NULL or empty for xlab_text
            xlab_text <- if (!is.null(input$parallel_unit_input) && input$parallel_unit_input != "") {
              paste0(value_col_name, " ", "[", input$parallel_unit_input, "]")
            } else {
              value_col_name
            }
            
            plot(model, showCI = TRUE, RIperc = c(0.025, 0.975), showPathol = FALSE,
                 title = plot_title,
                 xlab = xlab_text)
          })

          # Create summary reactively in the main session
          output[[output_id_summary]] <- renderPrint({
              req(model)
              cat("--- RefineR Summary for ", input$parallel_col_value, " (Gender: ", gender_part, ", Age: ", age_range_part, ") ---\n")
              # Print the model summary, which uses the default `fullDataEst` point estimate
              print(model)
          })
        }
      })
    }
  })
}