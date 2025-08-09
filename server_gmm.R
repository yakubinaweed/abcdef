# server_gmm.R
# This module contains the logic for the "Subpopulation Detection (GMM)" tab.
# It handles data upload, running the GMM analysis, and rendering the results.

# =========================================================================
# UTILITY FUNCTIONS FOR GMM ANALYSIS
# =========================================================================

# Z-transform a numeric vector (standardization)
# @param x: A numeric vector.
# @return: The standardized numeric vector.
z_transform <- function(x) {
  if (sd(x, na.rm = TRUE) == 0) {
    return(rep(0, length(x)))
  }
  return((x - mean(x, na.rm = TRUE)) / sd(x, na.rm = TRUE))
}

# Function to apply Yeo-Johnson Transformation conditionally
# It checks the skewness of a data vector and applies the transformation if the absolute skewness is above a threshold.
# @param data_vector: A numeric vector to transform.
# @param skewness_threshold: The threshold for applying the transformation.
# @return: A list containing the transformed data and a boolean indicating if a transformation was applied.
apply_conditional_yeo_johnson <- function(data_vector, skewness_threshold = 0.5) {
  transformed_data <- data_vector
  transformation_applied <- FALSE
  skew <- moments::skewness(data_vector, na.rm = TRUE)

  if (abs(skew) > skewness_threshold) {
    tryCatch({
      pt_result <- car::powerTransform(data_vector)
      lambda <- pt_result$lambda
      transformed_data <- car::yjPower(data_vector, lambda)
      transformation_applied <- TRUE
      message(paste("Yeo-Johnson transformation applied (skewness=", round(skew, 2), ")"))
    }, error = function(e) {
      warning(paste("Could not apply Yeo-Johnson transformation:", e$message))
    })
  } else {
    message(paste("Yeo-Johnson transformation not needed (skewness=", round(skew, 2), ")"))
  }

  return(list(transformed_data = transformed_data, transformation_applied = transformation_applied))
}

# Function to run GMM analysis using mclust, always with BIC.
# @param data_mat: A numeric matrix or data frame for clustering.
# @param G_range: A range of component numbers to test (e.g., 2:5).
# @return: An Mclust object representing the best-fit model.
run_gmm_with_criterion <- function(data_mat, G_range = 2:5) {
  if (!is.matrix(data_mat) && !is.data.frame(data_mat)) {
    stop("Input data_mat must be a matrix or data frame for GMM analysis.")
  }
  if (!all(sapply(data_mat, is.numeric))) {
    stop("All columns in data_mat must be numeric.")
  }
  if (any(is.na(data_mat))) {
    stop("Input data_mat contains NA values. Please remove or impute before clustering.")
  }

  tryCatch({
    gmm_model <- mclust::Mclust(data_mat, G = G_range, modelNames = c("EII", "VII", "EEI", "VEI", "EVI", "VVI", "EEE", "EVE", "VEE", "VVV"))
    return(gmm_model)
  }, error = function(e) {
    warning(paste("GMM run failed:", e$message))
    return(NULL)
  })
}

# Function to assign clusters back to the original data frame
# It adds a 'cluster' column to the data frame based on the GMM model's classification.
# @param df: The original data frame.
# @param gmm_model: The Mclust model object with a 'classification' property.
# @return: The data frame with an added 'cluster' column.
assign_clusters <- function(df, gmm_model) {
  if (is.null(gmm_model) || is.null(gmm_model$classification)) {
    warning("GMM model or classification is NULL. Cannot assign clusters.")
    return(df)
  }
  df$cluster <- gmm_model$classification
  return(df)
}

# Function to plot age vs a generic value colored by cluster
# It generates a ggplot scatter plot with confidence ellipses and cluster means.
# @param df: The data frame with 'Age', 'Value', 'Gender', and 'cluster' columns.
# @param value_col_name: The name of the value column for dynamic labeling.
# @param age_col_name: The name of the age column for dynamic labeling.
# @return: A ggplot object.
plot_value_age <- function(df, value_col_name, age_col_name) {
  if (is.null(df) || nrow(df) == 0) {
    return(ggplot2::ggplot() + ggplot2::annotate("text", x = 0.5, y = 0.5, label = "No GMM data available for plotting.", size = 6, color = "grey50"))
  }

  plot_title <- paste(value_col_name, "vs", age_col_name, "by Subpopulation Cluster")
  
  # Calculate cluster means
  cluster_means <- df %>%
    dplyr::group_by(Gender, cluster) %>%
    dplyr::summarise(mean_Age = mean(Age, na.rm = TRUE),
                     mean_Value = mean(Value, na.rm = TRUE),
                     .groups = 'drop')

  ggplot2::ggplot(df, ggplot2::aes(x = Age, y = Value, color = factor(cluster))) +
    # Original design for points
    ggplot2::geom_point(position = ggplot2::position_jitter(width = 0.2, height = 0.2), alpha = 0.6) +
    # Original design for ellipses
    ggplot2::stat_ellipse(geom = "polygon", ggplot2::aes(fill = factor(cluster)), alpha = 0.2, show.legend = FALSE, level = 0.95) +
    # Original design for cluster means
    ggplot2::geom_point(data = cluster_means, ggplot2::aes(x = mean_Age, y = mean_Value), shape = 4, size = 5, color = "red", stroke = 2) +
    ggplot2::facet_wrap(~Gender, labeller = as_labeller(function(x) paste(x, "Population"))) +
    ggplot2::labs(title = plot_title,
                  x = age_col_name, y = value_col_name, color = "Cluster") +
    # Original color palette
    ggplot2::scale_color_brewer(palette = "Set1") +
    ggplot2::scale_fill_brewer(palette = "Set1") +
    # Theme adjustments based on your feedback
    ggplot2::theme_light() +
    ggplot2::theme(
      plot.title = ggplot2::element_text(size = 20, face = "bold", hjust = 0.5, margin = ggplot2::margin(b = 20)),
      strip.text = ggplot2::element_text(size = 14, face = "bold", color = "black"),
      # Light grey box for facet labels with no contour line
      strip.background = ggplot2::element_rect(fill = "#EEEEEE", color = NA),
      axis.title.x = ggplot2::element_text(size = 14, margin = ggplot2::margin(t = 10)),
      axis.title.y = ggplot2::element_text(size = 14, margin = ggplot2::margin(r = 10)),
      axis.text = ggplot2::element_text(size = 11, color = "black"),
      legend.title = ggplot2::element_text(size = 12, face = "bold"),
      legend.text = ggplot2::element_text(size = 10, color = "black"),
      legend.position = "bottom",
      legend.background = ggplot2::element_rect(fill = "white", color = "grey90", size = 0.5, linetype = "solid"),
      # Boxed plot with lighter lines
      panel.border = ggplot2::element_rect(colour = "#CCCCCC", fill = NA, size = 1),
      panel.grid.major = ggplot2::element_line(color = "#F0F0F0", size = 0.5),
      panel.grid.minor = ggplot2::element_blank()
    )
}

# A new, centralized function to run the GMM on a data subset
run_gmm_analysis_on_subset <- function(data_subset, gender_label, value_col_name, age_col_name, message_rv, progress_increment) {
    if (nrow(data_subset) > 0) {
        yj_result <- apply_conditional_yeo_johnson(data_subset$Value)
        data_subset$Value_transformed <- yj_result$transformed_data
        
        data_subset$Value_z <- z_transform(data_subset$Value_transformed)
        data_subset$Age_z <- z_transform(data_subset$Age)
        
        incProgress(progress_increment, detail = paste("Running GMM for", gender_label, "data (BIC)..."))
        
        gmm_model <- tryCatch({
            run_gmm_with_criterion(data_subset %>% dplyr::select(Value = Value_z, Age = Age_z))
        }, error = function(e) {
            message_rv(list(text = paste("Error running BIC GMM for", gender_label, "data:", e$message), type = "error"))
            NULL
        })

        if (is.null(gmm_model)) {
            stop(paste("GMM model for", gender_label, "data could not be generated."))
        }
        
        data_clustered <- assign_clusters(data_subset, gmm_model)
        data_clustered$cluster <- as.factor(data_clustered$cluster)

        return(list(model = gmm_model, clustered_data = data_clustered, transformed_flag = yj_result$transformation_applied))
    } else {
        message_rv(list(text = paste("Error: No", tolower(gender_label), "data found after filtering. Please check the gender column and selection."), type = "error"))
        return(list(model = NULL, clustered_data = NULL, transformed_flag = FALSE))
    }
}


# =========================================================================
# MAIN SERVER LOGIC
# =========================================================================

gmmServer <- function(input, output, session, gmm_uploaded_data_rv, gmm_processed_data_rv, gmm_transformation_details_rv, message_rv, analysis_running_rv) {

  # Reactive value to hold models for BIC criterion
  gmm_models_bic_rv <- reactiveVal(list(male = NULL, female = NULL))

  # Helper function to guess column names (could be moved to a shared utils file)
  guess_column <- function(cols_available, common_names) {
    for (name in common_names) {
      match_idx <- grep(paste0("^", name, "$"), cols_available, ignore.case = TRUE)
      if (length(match_idx) > 0) {
        return(cols_available[match_idx[1]])
      }
    }
    return("")
  }

  # Observer for GMM file upload
  # Reads the uploaded Excel file and updates column selectors based on likely names
  observeEvent(input$gmm_file_upload, {
    req(input$gmm_file_upload)
    tryCatch({
      data <- readxl::read_excel(input$gmm_file_upload$datapath)
      gmm_uploaded_data_rv(data)
      message_rv(list(text = "GMM data uploaded successfully.", type = "success"))

      col_names <- colnames(data)
      # Add "None" as a choice for the gender column
      all_col_choices_with_none <- c("None" = "", col_names)

      updateSelectInput(session, "gmm_value_col", choices = all_col_choices_with_none, selected = guess_column(col_names, c("Value", "Result", "Measurement", "Waarde", "HGB", "hgb", "HB", "hb")))
      updateSelectInput(session, "gmm_age_col", choices = all_col_choices_with_none, selected = guess_column(col_names, c("Age", "age", "leeftijd")))
      updateSelectInput(session, "gmm_gender_col", choices = all_col_choices_with_none, selected = guess_column(col_names, c("Gender", "gender", "Sex", "sex", "geslacht")))

    }, error = function(e) {
      message_rv(list(text = paste("Error reading GMM file:", e$message), type = "error"))
      gmm_uploaded_data_rv(NULL)
    })
  })

  # Renders the gender choice radio buttons only if a gender column is selected
  output$gmm_gender_choice_ui <- renderUI({
    req(input$gmm_gender_col)
    if (input$gmm_gender_col != "") {
      radioButtons(inputId = "gmm_gender_choice", label = "Select Gender Analysis:", choices = c("Male" = "Male", "Female" = "Female", "Both" = "Both"), selected = "Both", inline = TRUE)
    }
  })

  # Observer for GMM analysis button
  # This is the core logic for the GMM tab, running the analysis with progress updates
  observeEvent(input$run_gmm_analysis_btn, {
    # Custom checks for user-friendly error messages
    if (is.null(gmm_uploaded_data_rv())) {
      message_rv(list(text = "Please upload an Excel file first.", type = "error"))
      return(NULL)
    }
    
    # NEW ERROR HANDLING: Check that columns have been selected
    if (input$gmm_value_col == "" || input$gmm_age_col == "") {
      message_rv(list(text = "Please select the columns from the dropdown menus.", type = "error"))
      return(NULL)
    }

    data_check <- gmm_uploaded_data_rv()

    # NEW ERROR HANDLING: Check if selected columns exist in the data
    if (!(input$gmm_value_col %in% colnames(data_check))) {
      message_rv(list(text = paste0("Error: The selected column for Values ('", input$gmm_value_col, "') was not found in your uploaded data. Please select a valid column."), type = "error"))
      return(NULL)
    }
    if (!(input$gmm_age_col %in% colnames(data_check))) {
      message_rv(list(text = paste0("Error: The selected column for Age ('", input$gmm_age_col, "') was not found in your uploaded data. Please select a valid column."), type = "error"))
      return(NULL)
    }
    # NEW ERROR HANDLING: Check if a selected gender column exists in the data
    if (input$gmm_gender_col != "" && !(input$gmm_gender_col %in% colnames(data_check))) {
      message_rv(list(text = paste0("Error: The selected gender column '", input$gmm_gender_col, "' was not found in your uploaded data. Please select a valid column."), type = "error"))
      return(NULL)
    }
    
    # NEW ERROR HANDLING: Check if selected columns are numeric
    if (!is.numeric(data_check[[input$gmm_value_col]]) || !is.numeric(data_check[[input$gmm_age_col]])) {
      message_rv(list(text = paste0("Error: The selected columns for Values (", input$gmm_value_col, ") and/or Age (", input$gmm_age_col, ") must contain numeric data. Please check your file."), type = "error"))
      return(NULL)
    }

    # NEW ERROR HANDLING: Check if the same column is selected for Value and Age
    if (input$gmm_value_col == input$gmm_age_col) {
      message_rv(list(text = "Error: The same column cannot be selected for both Values and Age. Please choose a different column for one of the inputs.", type = "error"))
      return(NULL)
    }

    # End of custom checks

    # Check for gender choice requirement only if a gender column is selected
    if (input$gmm_gender_col != "") {
      req(input$gmm_gender_choice)
    }

    if (analysis_running_rv()) {
      message_rv(list(text = "An analysis is already running. Please wait.", type = "warning"))
      return(NULL)
    }

    # Start by clearing reactive values, which will trigger the UI to update with a blank state
    gmm_processed_data_rv(NULL)
    gmm_models_bic_rv(list(male=NULL, female=NULL))
    message_rv(list(text = "Starting new GMM analysis...", type = "info"))


    analysis_running_rv(TRUE)
    # Disable the analyze button and change its text
    shinyjs::disable("run_gmm_analysis_btn")
    shinyjs::runjs("$('#run_gmm_analysis_btn').text('Analyzing...');")
    session$sendCustomMessage('analysisStatus', TRUE)

    tryCatch({
      withProgress(message = 'Running GMM Analysis', value = 0, {
        incProgress(0.1, detail = "Loading data...")
  
        data <- gmm_uploaded_data_rv()
        value_col <- input$gmm_value_col
        age_col <- input$gmm_age_col
        gender_col <- input$gmm_gender_col
        gender_choice <- if (gender_col == "") "None" else input$gmm_gender_choice
        
        # Define columns to select dynamically to prevent crashing
        value_col_sym <- rlang::sym(value_col)
        age_col_sym <- rlang::sym(age_col)
  
        # Select columns explicitly based on whether a gender column is selected
        if (gender_col != "") {
          gender_col_sym <- rlang::sym(gender_col)
          gmm_data <- data %>%
            dplyr::select(Value = !!value_col_sym, Age = !!age_col_sym, Gender_orig = !!gender_col_sym) %>%
            na.omit()
        } else {
          gmm_data <- data %>%
            dplyr::select(Value = !!value_col_sym, Age = !!age_col_sym) %>%
            na.omit()
        }
  
        if (nrow(gmm_data) == 0) {
          message_rv(list(text = "No complete rows for GMM after NA removal. Check data or selections.", type = "error"))
          return(NULL)
        }
  
        incProgress(0.2, detail = "Splitting data by gender and transforming...")
  
        # Standardize gender column if present
        if (gender_col != "") {
          gmm_data <- gmm_data %>%
            mutate(Gender = case_when(
              grepl("male|m|man|jongen(s)?|heren|mannelijk(e)?", Gender_orig, ignore.case = TRUE) ~ "Male",
              grepl("female|f|vrouw(en)?|v|meisje(s)?|dame|mevr|vrouwelijke", Gender_orig, ignore.case = TRUE) ~ "Female",
              TRUE ~ "Other"
            )) %>%
            filter(Gender %in% c("Male", "Female"))
        } else {
          # If no gender column, create a dummy group
          gmm_data <- gmm_data %>%
            mutate(Gender = "Combined")
        }
        
        # Final check for empty data after filtering
        if (nrow(gmm_data) == 0) {
          message_rv(list(text = "Filtered dataset is empty after gender selection. Please check the data or gender column.", type = "warning"))
          return(NULL)
        }
  
        combined_clustered_data <- tibble()
        male_value_transformed_flag <- FALSE
        female_value_transformed_flag <- FALSE
        combined_gmm_model_bic <- NULL
        male_gmm_model_bic <- NULL
        female_gmm_model_bic <- NULL
        
        # Use a list to store results for easier processing
        results <- list(male = NULL, female = NULL, combined = NULL)
  
        # Process data based on gender selection
        if (gender_col == "" || gender_choice == "Both") {
          if (gender_col == "") {
            message("Running GMM on combined data...")
            results$combined <- run_gmm_analysis_on_subset(
              data_subset = gmm_data,
              gender_label = "Combined",
              value_col_name = value_col,
              age_col_name = age_col,
              message_rv = message_rv,
              progress_increment = 0.6
            )
            if (!is.null(results$combined$model)) {
              combined_gmm_model_bic <- results$combined$model
              male_value_transformed_flag <- results$combined$transformed_flag
              combined_clustered_data <- dplyr::bind_rows(combined_clustered_data, results$combined$clustered_data)
            }
          } else {
            # Process Male data
            male_data <- gmm_data %>% dplyr::filter(Gender == "Male")
            results$male <- run_gmm_analysis_on_subset(
              data_subset = male_data,
              gender_label = "Male",
              value_col_name = value_col,
              age_col_name = age_col,
              message_rv = message_rv,
              progress_increment = 0.3
            )
            if (!is.null(results$male$model)) {
              male_gmm_model_bic <- results$male$model
              male_value_transformed_flag <- results$male$transformed_flag
              combined_clustered_data <- dplyr::bind_rows(combined_clustered_data, results$male$clustered_data)
            }
  
            # Process Female data
            female_data <- gmm_data %>% dplyr::filter(Gender == "Female")
            results$female <- run_gmm_analysis_on_subset(
              data_subset = female_data,
              gender_label = "Female",
              value_col_name = value_col,
              age_col_name = age_col,
              message_rv = message_rv,
              progress_increment = 0.3
            )
            if (!is.null(results$female$model)) {
              female_gmm_model_bic <- results$female$model
              female_value_transformed_flag <- results$female$transformed_flag
              combined_clustered_data <- dplyr::bind_rows(combined_clustered_data, results$female$clustered_data)
            }
          }
        } else if (gender_choice == "Male") {
          male_data <- gmm_data %>% dplyr::filter(Gender == "Male")
          results$male <- run_gmm_analysis_on_subset(
            data_subset = male_data,
            gender_label = "Male",
            value_col_name = value_col,
            age_col_name = age_col,
            message_rv = message_rv,
            progress_increment = 0.6
          )
          if (!is.null(results$male$model)) {
            male_gmm_model_bic <- results$male$model
            male_value_transformed_flag <- results$male$transformed_flag
            combined_clustered_data <- dplyr::bind_rows(combined_clustered_data, results$male$clustered_data)
          }
        } else if (gender_choice == "Female") {
          female_data <- gmm_data %>% dplyr::filter(Gender == "Female")
          results$female <- run_gmm_analysis_on_subset(
            data_subset = female_data,
            gender_label = "Female",
            value_col_name = value_col,
            age_col_name = age_col,
            message_rv = message_rv,
            progress_increment = 0.6
          )
          if (!is.null(results$female$model)) {
            female_gmm_model_bic <- results$female$model
            female_value_transformed_flag <- results$female$transformed_flag
            combined_clustered_data <- dplyr::bind_rows(combined_clustered_data, results$female$clustered_data)
          }
        }
        
        # Update reactive values with the correct models and transformation details
        gmm_models_bic_rv(list(
          combined = combined_gmm_model_bic,
          male = male_gmm_model_bic,
          female = female_gmm_model_bic
        ))
        gmm_transformation_details_rv(list(
          male_value_transformed = male_value_transformed_flag, 
          female_value_transformed = female_value_transformed_flag
        ))
  
        if (nrow(combined_clustered_data) > 0) {
          gmm_processed_data_rv(list(bic = combined_clustered_data))
          message_rv(list(text = "GMM analysis complete!", type = "success"))
        } else {
          message_rv(list(text = "No data available after GMM processing for plotting/summary.", type = "error"))
          gmm_processed_data_rv(NULL)
        }
  
        incProgress(0.1, detail = "Generating plots and summaries...")
      })
    }, error = function(e) {
      # Handle any analysis errors here
      message_rv(list(text = paste("Analysis Error:", e$message), type = "danger"))
      gmm_processed_data_rv(NULL)
    }, finally = {
      analysis_running_rv(FALSE)
      shinyjs::enable("run_gmm_analysis_btn")
      shinyjs::runjs("$('#run_gmm_analysis_btn').text('Analyze');")
      session$sendCustomMessage('analysisStatus', FALSE)
    })
  })

  # Observer for the Reset button on the GMM tab
  observeEvent(input$reset_gmm_analysis_btn, {
    # Reset all reactive values, allowing the UI to update automatically
    gmm_uploaded_data_rv(NULL)
    gmm_processed_data_rv(NULL)
    gmm_transformation_details_rv(list(male_value_transformed = FALSE, female_value_transformed = FALSE))
    gmm_models_bic_rv(list(male = NULL, female = NULL))
    shinyjs::reset("gmm_file_upload")
    message_rv(list(text = "GMM data and results reset.", type = "info"))
    
    updateSelectInput(session, "gmm_value_col", choices = c("None" = ""), selected = "")
    updateSelectInput(session, "gmm_age_col", choices = c("None" = ""), selected = "")
    updateSelectInput(session, "gmm_gender_col", choices = c("None" = ""), selected = "")
  })

  output$gmm_results_ui <- renderUI({
    plot_data_bic <- gmm_processed_data_rv()$bic

    if (is.null(plot_data_bic) || nrow(plot_data_bic) == 0) {
      return(NULL)
    }

    tagList(
      div(class = "output-box",
          h4(class = "gmm-title", "BIC Criterion Results"),
          plotOutput("gmm_bic_plots", height = "400px"),
          div(class = "spacing-div"), # This adds spacing after the BIC plot
          plotOutput("plot_output_gmm_bic", height = "600px"),
          div(class = "spacing-div"), # This adds spacing before the summary
          verbatimTextOutput("gmm_summary_output_bic")
      )
    )
  })
  
  output$gmm_bic_plots <- renderPlot({
    models <- gmm_models_bic_rv()

    # Set new graphical parameters to reduce top margin and adjust title position
    old_par <- par(no.readonly = TRUE)
    on.exit(par(old_par))
    par(mar = c(5.1, 4.1, 1.1, 2.1), mgp = c(2.5, 1, 0))
    
    has_combined_model <- !is.null(models$combined)
    has_male_model <- !is.null(models$male)
    has_female_model <- !is.null(models$female)

    if (has_combined_model) {
      if (!is.null(models$combined) && !inherits(models$combined, "try-error")) {
        plot_title <- paste0("BIC Plot for ", input$gmm_value_col, " and ", input$gmm_age_col, " (Combined)")
        plot(models$combined, what = "BIC", main = plot_title)
      } else {
        return(ggplot2::ggplot() + ggplot2::annotate("text", x = 0.5, y = 0.5, label = "GMM model for combined data was not generated.", size = 6, color = "grey50"))
      }
    } else if (has_male_model || has_female_model) {
      par(mfrow = c(1, 2))
      if (has_male_model && !inherits(models$male, "try-error")) {
        plot_title <- paste0("BIC Plot for ", input$gmm_value_col, " and ", input$gmm_age_col, " (Male)")
        plot(models$male, what = "BIC", main = plot_title)
      } else {
        plot.new()
        text(0.5, 0.5, "GMM model for male data was not generated.")
      }
      if (has_female_model && !inherits(models$female, "try-error")) {
        plot_title <- paste0("BIC Plot for ", input$gmm_value_col, " and ", input$gmm_age_col, " (Female)")
        plot(models$female, what = "BIC", main = plot_title)
      } else {
        plot.new()
        text(0.5, 0.5, "GMM model for female data was not generated.")
      }
      par(mfrow = c(1, 1))
    } else {
      return(ggplot2::ggplot() + ggplot2::annotate("text", x = 0.5, y = 0.5, label = "No GMM models available for plotting.", size = 6, color = "grey50"))
    }
  })

  output$plot_output_gmm_bic <- renderPlot({
    plot_data <- gmm_processed_data_rv()$bic
    if (is.null(plot_data) || nrow(plot_data) == 0) {
      return(ggplot2::ggplot() + ggplot2::annotate("text", x = 0.5, y = 0.5, label = "No GMM data available for plotting.", size = 6, color = "grey50"))
    }
    plot_value_age(plot_data,
                value_col_name = input$gmm_value_col,
                age_col_name = input$gmm_age_col)
  })  

 output$gmm_summary_output_bic <- renderPrint({
    plot_data <- gmm_processed_data_rv()$bic
    models <- gmm_models_bic_rv()
    
    if (is.null(plot_data) || nrow(plot_data) == 0) {
      return("No GMM analysis results to display.")
    }

    cat("--- GMM Analysis Summary (BIC Criterion) ---\n")
    
    if (!is.null(models$combined) && !inherits(models$combined, "try-error")) {
        cat("\n--- Combined Subpopulations ---\n")
        
        print(summary(models$combined))

        num_clusters <- models$combined$G
        for (i in 1:num_clusters) {
            cat(paste0("Cluster ", i, ":\n"))
            cat(paste0("  Proportion: ", round(models$combined$parameters$pro[i], 3), "\n"))
            
            cluster_data <- plot_data %>% dplyr::filter(Gender == "Combined", cluster == i)
            mean_value <- mean(cluster_data$Value, na.rm = TRUE)
            mean_age <- mean(cluster_data$Age, na.rm = TRUE)
            sd_value <- sd(cluster_data$Value, na.rm = TRUE)
            sd_age <- sd(cluster_data$Age, na.rm = TRUE)
            
            cat(paste0("  Mean ", input$gmm_value_col, ": ", round(mean_value, 3), "\n"))
            cat(paste0("  Mean ", input$gmm_age_col, ": ", round(mean_age, 3), "\n"))
            cat(paste0("  Std Dev ", input$gmm_value_col, ": ", round(sd_value, 3), "\n"))
            cat(paste0("  Std Dev ", input$gmm_age_col, ": ", round(sd_age, 3), "\n"))
            
            if (!is.na(sd_age)) {
              lower_age <- round(mean_age - 2 * sd_age, 1)
              upper_age <- round(mean_age + 2 * sd_age, 1)
              cat(paste0("  Estimated ", input$gmm_age_col, " Range (Mean +/- 2SD): [", max(0, lower_age), " to ", upper_age, "] years\n"))
            } else {
              cat(paste0("  Estimated ", input$gmm_age_col, " Range: N/A (Std Dev Age problematic)\n"))
            }
            cat("\n")
        }
    } else {
        if (!is.null(models$male) && !inherits(models$male, "try-error")) {
            cat("\n--- Male Subpopulations ---\n")
            
            print(summary(models$male))

            num_clusters <- models$male$G
            for (i in 1:num_clusters) {
                cat(paste0("Cluster ", i, ":\n"))
                cat(paste0("  Proportion: ", round(models$male$parameters$pro[i], 3), "\n"))
                
                male_cluster_data <- plot_data %>% dplyr::filter(Gender == "Male", cluster == i)
                mean_value <- mean(male_cluster_data$Value, na.rm = TRUE)
                mean_age <- mean(male_cluster_data$Age, na.rm = TRUE)
                sd_value <- sd(male_cluster_data$Value, na.rm = TRUE)
                sd_age <- sd(male_cluster_data$Age, na.rm = TRUE)
                
                cat(paste0("  Mean ", input$gmm_value_col, ": ", round(mean_value, 3), "\n"))
                cat(paste0("  Mean ", input$gmm_age_col, ": ", round(mean_age, 3), "\n"))
                cat(paste0("  Std Dev ", input$gmm_value_col, ": ", round(sd_value, 3), "\n"))
                cat(paste0("  Std Dev ", input$gmm_age_col, ": ", round(sd_age, 3), "\n"))
                
                if (!is.na(sd_age)) {
                  lower_age <- round(mean_age - 2 * sd_age, 1)
                  upper_age <- round(mean_age + 2 * sd_age, 1)
                  cat(paste0("  Estimated ", input$gmm_age_col, " Range (Mean +/- 2SD): [", max(0, lower_age), " to ", upper_age, "] years\n"))
                } else {
                  cat(paste0("  Estimated ", input$gmm_age_col, " Range: N/A (Std Dev Age problematic)\n"))
                }
                cat("\n")
            }
        } else {
            cat("No male subpopulations detected.\n")
        }
        
        if (!is.null(models$female) && !inherits(models$female, "try-error")) {
            cat("\n--- Female Subpopulations ---\n")
            
            print(summary(models$female))

            num_clusters <- models$female$G
            for (i in 1:num_clusters) {
                cat(paste0("Cluster ", i, ":\n"))
                cat(paste0("  Proportion: ", round(models$female$parameters$pro[i], 3), "\n"))
                
                female_cluster_data <- plot_data %>% dplyr::filter(Gender == "Female", cluster == i)
                mean_value <- mean(female_cluster_data$Value, na.rm = TRUE)
                mean_age <- mean(female_cluster_data$Age, na.rm = TRUE)
                sd_value <- sd(female_cluster_data$Value, na.rm = TRUE)
                sd_age <- sd(female_cluster_data$Age, na.rm = TRUE)
                
                cat(paste0("  Mean ", input$gmm_value_col, ": ", round(mean_value, 3), "\n"))
                cat(paste0("  Mean ", input$gmm_age_col, ": ", round(mean_age, 3), "\n"))
                cat(paste0("  Std Dev ", input$gmm_value_col, ": ", round(sd_value, 3), "\n"))
                cat(paste0("  Std Dev ", input$gmm_age_col, ": ", round(sd_age, 3), "\n"))
                
                if (!is.na(sd_age)) {
                  lower_age <- round(mean_age - 2 * sd_age, 1)
                  upper_age <- round(mean_age + 2 * sd_age, 1)
                  cat(paste0("  Estimated ", input$gmm_age_col, " Range (Mean +/- 2SD): [", max(0, lower_age), " to ", upper_age, "] years\n"))
                } else {
                  cat(paste0("  Estimated ", input$gmm_age_col, " Range: N/A (Std Dev Age problematic)\n"))
                }
                cat("\n")
            }
        } else {
            cat("No female subpopulations detected.\n")
        }
    }

    if (gmm_transformation_details_rv()$male_value_transformed || gmm_transformation_details_rv()$female_value_transformed) {
      cat("\nNote: ", input$gmm_value_col, " values were transformed (Yeo-Johnson) for GMM input due to skewness. Reported ", input$gmm_value_col, " values are original.\n")
    }
  })
}