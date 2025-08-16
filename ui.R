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
  
  # New tab for Parallel RefineR Analysis
  tabPanel(
    title = "Parallel Analysis",
    useShinyjs(),
    sidebarLayout(
      sidebarPanel(
        style = "padding-right: 15px;",
        p("Enter one or more age ranges for each gender below, separated by commas.
          Example: 0-10, 10-20, 20-30"),
        textAreaInput(inputId = "male_age_ranges",
                      label = "Male Age Ranges:",
                      rows = 3,
                      placeholder = "e.g., 0-10, 10-20"),
        textAreaInput(inputId = "female_age_ranges",
                      label = "Female Age Ranges:",
                      rows = 3,
                      placeholder = "e.g., 0-10, 10-20"),
        textAreaInput(inputId = "combined_age_ranges",
                      label = "Combined Age Ranges (Both Genders):",
                      rows = 3,
                      placeholder = "e.g., 0-10, 10-20"),
        fileInput(inputId = "parallel_file", label = "Upload Data (Excel File)", accept = c(".xlsx")),
        selectInput(inputId = "parallel_col_value", label = "Select Column for Values:", choices = c("None" = ""), selected = ""),
        selectInput(inputId = "parallel_col_age", label = "Select Column for Age:", choices = c("None" = ""), selected = ""),
        selectInput(inputId = "parallel_col_gender", label = "Select Column for Gender:", choices = c("None" = ""), selected = ""),
        radioButtons(inputId = "parallel_model_choice", label = "Select Transformation Model:",
                     choices = c("BoxCox" = "BoxCox", "modBoxCox" = "modBoxCox"),
                     selected = "BoxCox", inline = TRUE),
        hr(),
        radioButtons(inputId = "parallel_nbootstrap_speed", label = "Select Computation Speed:", choices = c("Fast", "Medium", "Slow"), selected = "Fast", inline = TRUE),
        numericInput("cores", "Number of Cores:", value = 2, min = 1),
        textInput(inputId = "parallel_unit_input", label = "Unit of Measurement", value = "", placeholder = "ex. g/L"),
        hr(),
        actionButton("run_parallel_btn", "Run Parallel Analysis", class = "btn-primary"),
        actionButton("reset_parallel_btn", "Reset", class = "btn-secondary"),
        uiOutput("parallel_message")
      ),
      mainPanel(
        # New tabsetPanel for organizing parallel results
        tabsetPanel(
          tabPanel("Individual Results", 
                   uiOutput("parallel_results_ui")
          ),
          tabPanel("Combined Summary",
                   h3("Reference Intervals with Confidence Intervals"),
                   plotOutput("combined_dumbbell_plot"),
                   div(class = "spacing-div"),
                   h3("Age-Stratified Reference Intervals"),
                   plotOutput("combined_ri_plot"),
                   div(class = "spacing-div"),
                   h3("Data Distributions (Density Plots)"),
                   plotOutput("combined_density_plot"),
                   h3("Summary of Data Distributions (Box Plots)"),
                   plotOutput("combined_box_plot"),
                   div(class = "spacing-div"),
                   verbatimTextOutput("combined_summary")
          )
        )
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