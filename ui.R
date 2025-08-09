library(shiny)
library(bslib)
library(refineR)
library(readxl)
library(moments)
library(shinyjs)
library(shinyWidgets)
library(shinyFiles)
library(ggplot2)

ui <- navbarPage(
  title = "RefineR Reference Interval Estimation",
  id = "tabs",
  # Sets the visual theme and fonts for the Shiny app
  theme = bs_theme(version = 4, base_font = font_google("Inter"), heading_font = font_google("Rethink Sans"), font_scale = 1.1, bootswatch = "default"),

  # First tab for the main RefineR analysis
  tabPanel(
    title = "Main Analysis",
    useShinyjs(),
    tags$head(
      # Includes the custom CSS from the 'www' directory
      includeCSS("www/styles.css")
    ),
    # Custom JavaScript to handle disabling the other tab during analysis
    tags$script(HTML("
      var analysisRunning = false;
      Shiny.addCustomMessageHandler('analysisStatus', function(status) {
        analysisRunning = status;
        if (status) {
          // Disable all tab links that are not currently active
          $('a[data-toggle=\"tab\"]').each(function() {
            if (!$(this).parent().hasClass('active')) {
              $(this).addClass('disabled-tab-link');
            }
          });
        } else {
          // Re-enable all tab links
          $('a.disabled-tab-link').removeClass('disabled-tab-link');
        }
      });
      // Event handler to block clicks on disabled tabs
      $(document).on('click', 'a.disabled-tab-link', function(event) {
        event.preventDefault();
        Shiny.setInputValue('tab_switch_blocked', new Date().getTime());
        return false;
      });
    ")),
    sidebarLayout(
      sidebarPanel(
        style = "padding-right: 15px;",
        # User inputs for data filtering and analysis parameters
        selectInput(inputId = "gender_choice", label = "Select Gender:", choices = c("Male" = "M", "Female" = "F", "Both" = "Both"), selected = "Both"),
        sliderInput(inputId = "age_range", label = "Age Range:", min = 0, max = 100, value = c(0, 100), step = 1),
        fileInput(inputId = "data_file", label = "Upload Data (Excel File)", accept = c(".xlsx")),
        # Dynamic inputs for selecting data columns
        selectInput(inputId = "col_value", label = "Select Column for Values:", choices = c("None" = ""), selected = ""),
        selectInput(inputId = "col_age", label = "Select Column for Age:", choices = c("None" = ""), selected = ""),
        selectInput(inputId = "col_gender", label = "Select Column for Gender:", choices = c("None" = ""), selected = ""),
        radioButtons(inputId = "nbootstrap_speed", label = "Select Computation Speed:", choices = c("Fast", "Medium", "Slow"), selected = "Fast", inline = TRUE),
        
        # New: Radio buttons for model selection (removed "None" option)
        radioButtons(inputId = "model_choice", label = "Select Transformation Model:",
                     choices = c("BoxCox" = "BoxCox",
                                 "modBoxCox" = "modBoxCox"),
                     selected = "BoxCox", inline = TRUE), # Default to Box-Cox
        
        # Action buttons for the analysis
        actionButton("analyze_btn", "Analyze", class = "btn-primary"),
        actionButton("reset_btn", "Reset File", class = "btn-secondary"),
        shinyFiles::shinyDirButton(id = "select_dir_btn", label = "Select Output Directory", title = "Select a directory to save plots", style = "margin-top: 5px;"),
        div(style = "margin-top: 5px; display: flex; align-items: center; justify-content: flex-start; width: 100%;",
            prettySwitch(inputId = "enable_directory", label = "Auto-Save Graph", status = "success", fill = TRUE, inline = TRUE)
        ),
        uiOutput("app_message"), # Placeholder for displaying app messages
        hr(),
        # Inputs for manual reference limits and units for the plot
        numericInput("ref_low", "Reference Lower Limit:", value = NA),
        numericInput("ref_high", "Reference Upper Limit:", value = NA),
        textInput(inputId = "unit_input", label = "Unit of Measurement", value = "mmol/L", placeholder = "ex. g/L")
      ),
      mainPanel(
        # Outputs for the main analysis results
        plotOutput("result_plot"),
        verbatimTextOutput("result_text")
      )
    )
  ),

  # Second tab for Gaussian Mixture Model (GMM) subpopulation detection
  tabPanel(
    title = "Subpopulation Detection (GMM)",
    useShinyjs(),
    p("Gaussian Mixture Models aim to detect hidden subpopulations within your data based on a selected value and age. This tool employs the mclust package, which automatically selects the best model and number of components based on the Bayesian Information Criterion (BIC). For each detected subpopulation, estimated age ranges are provided directly from the model's characteristics, avoiding predefined bins.

Before running the GMM, the data is preprocessed: the selected value's column is conditionally transformed using the Yeo-Johnson method if it shows significant skewness, and both the value and age columns are standardized (z-transformed)."),
    sidebarLayout(
      sidebarPanel(
        fileInput(inputId = "gmm_file_upload", label = "Upload Data (Excel File)", accept = c(".xlsx")),
        hr(),
        # Dynamic inputs for selecting Value, Age, and Gender columns for GMM
        selectInput(inputId = "gmm_value_col", label = "Select Column for Values:", choices = c("None" = ""), selected = ""),
        selectInput(inputId = "gmm_age_col", label = "Select Column for Age:", choices = c("None" = ""), selected = ""),
        selectInput(inputId = "gmm_gender_col", label = "Select Column for Gender:", choices = c("None" = ""), selected = ""),
        hr(),
        # Action buttons for the GMM analysis
        # New radio buttons for gender selection
        uiOutput("gmm_gender_choice_ui"),
        actionButton("run_gmm_analysis_btn", "Analyze", class = "btn-primary"),
        actionButton("reset_gmm_analysis_btn", "Reset File", class = "btn-secondary"),
        # Added a div with a top margin to create spacing
        div(style = "margin-top: 15px;", uiOutput("app_message"))
      ),
      mainPanel(
        # Renders the UI for GMM results dynamically
        uiOutput("gmm_results_ui")
      )
    )
  ),

  # Footer of the application with copyright and a link to the author's GitHub
  footer = tags$footer(
    HTML('© 2025 <a href="https://github.com/yakubinaweed/refineR-reference-interval" target="_blank">Naweed Yakubi</a> • All rights reserved.'),
    style = "
      position: relative;
      bottom: 0;
      width: 100%;
      text-align: center;
      padding: 10px 0;
      color: #777;
      font-size: 0.8em;"
  )
)