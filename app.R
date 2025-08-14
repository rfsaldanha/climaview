# Packages
library(shiny)
library(bslib)
library(dplyr)
library(dbplyr)
library(lubridate)
library(DBI)
library(duckdb)
library(RPostgres)
library(ggplot2)
library(readr)

# Database
database <- "duck"

if (database == "psql") {
  # Connection
  con <- dbConnect(
    RPostgres::Postgres(),
    dbname = "observatorio",
    host = "psql.icict.fiocruz.br",
    port = 5432,
    user = Sys.getenv("weather_user"),
    password = Sys.getenv("weather_password")
  )

  # Schemas
  schema_sensor_772002 <- dbplyr::in_schema(
    "estacoes",
    "tb_estacao_2_sensor_772002"
  )
  schema_sensor_772003 <- dbplyr::in_schema(
    "estacoes",
    "tb_estacao_2_sensor_772003"
  )
  schema_sensor_772004 <- dbplyr::in_schema(
    "estacoes",
    "tb_estacao_2_sensor_772004"
  )
  schema_sensor_772005 <- dbplyr::in_schema(
    "estacoes",
    "tb_estacao_2_sensor_772005"
  )
  schema <- dbplyr::in_schema("estacoes", "tb_estacao_1b")

  # Tables
  wl_temp <- tbl(con, schema_sensor_772005) |>
    select(time = ts, value = temp) |>
    mutate(
      value = (value - 32) / 1.8
    )
  wl_umid <- tbl(con, schema_sensor_772005) |>
    select(time = ts, value = hum)
  wl_press <- tbl(con, schema_sensor_772003) |>
    select(time = ts, value = bar_sea_level) |>
    mutate(
      value = value * 33.864
    )
  wl_rain <- tbl(con, schema_sensor_772005) |>
    select(time = ts, value = rain_rate_last_mm)
  wl_wind <- tbl(con, schema_sensor_772005) |>
    select(time = ts, value = wind_speed_last) |>
    mutate(
      value = value * 1.609344
    )
  pf_temp <- tbl(con, schema) |>
    filter(sensor == 8)
  pf_umid <- tbl(con, schema) |>
    filter(sensor == 11)
  pf_press <- tbl(con, schema) |>
    filter(sensor == 23)
  pf_uv <- tbl(con, schema) |>
    filter(sensor == 19)
  pf_river <- tbl(con, schema) |>
    filter(sensor == 34)
  pf_rain <- tbl(con, schema) |>
    filter(sensor == 35)
  pf_wind <- tbl(con, schema) |>
    filter(sensor == 36)
  pf_gust <- tbl(con, schema) |>
    filter(sensor == 37)
} else {
  # Connection
  con <- dbConnect(
    duckdb(),
    "../dump_estacoes/estacoes.duckdb",
    read_only = TRUE
  )

  # Tables
  wl_temp <- tbl(con, "tb_estacao_2_sensor_772005") |>
    select(time = ts, value = temp) |>
    mutate(
      value = (value - 32) / 1.8
    )
  wl_umid <- tbl(con, "tb_estacao_2_sensor_772005") |>
    select(time = ts, value = hum)
  wl_press <- tbl(con, "tb_estacao_2_sensor_772003") |>
    select(time = ts, value = bar_sea_level) |>
    mutate(
      value = value * 33.864
    )
  wl_rain <- tbl(con, "tb_estacao_2_sensor_772005") |>
    select(time = ts, value = rain_rate_last_mm)
  wl_wind <- tbl(con, "tb_estacao_2_sensor_772005") |>
    select(time = ts, value = wind_speed_last) |>
    mutate(
      value = value * 1.609344
    )
  pf_temp <- tbl(con, "tb_estacao_1b") |>
    filter(sensor == 8) |>
    select(-device, -sensor)
  pf_umid <- tbl(con, "tb_estacao_1b") |>
    filter(sensor == 11) |>
    select(-device, -sensor)
  pf_press <- tbl(con, "tb_estacao_1b") |>
    filter(sensor == 23) |>
    select(-device, -sensor)
  pf_uv <- tbl(con, "tb_estacao_1b") |>
    filter(sensor == 19) |>
    select(-device, -sensor)
  pf_river <- tbl(con, "tb_estacao_1b") |>
    filter(sensor == 34) |>
    select(-device, -sensor)
  pf_rain <- tbl(con, "tb_estacao_1b") |>
    filter(sensor == 35) |>
    select(-device, -sensor)
  pf_wind <- tbl(con, "tb_estacao_1b") |>
    filter(sensor == 36) |>
    select(-device, -sensor)
  pf_gust <- tbl(con, "tb_estacao_1b") |>
    filter(sensor == 37) |>
    select(-device, -sensor)
}

# Interface
ui <- page_navbar(
  title = "Dados meteorológicos",
  theme = bs_theme(bootswatch = "shiny"),
  fillable = FALSE,

  # Logo
  tags$head(
    tags$script(
      HTML(
        '$(document).ready(function() {
             $(".navbar .container-fluid")
               .append("<img id = \'myImage\' src=\'pin_obs_horizontal.png\' align=\'right\' height = \'50px\'>"  );
            });'
      )
    ),
    tags$style(
      HTML(
        '@media (max-width:992px) { #myImage { position: fixed; right: 20%; top: 0.5%; }}'
      )
    )
  ),

  # Translation
  tags$script(
    HTML(
      "
      $(document).ready(function() {
        // Change the text 'Expand' in all tooltips
        $('.card.bslib-card bslib-tooltip > div').each(function() {
          if ($(this).text().includes('Expand')) {
            $(this).text('Expandir');
          }
        });
  
        // Use MutationObserver to change the text 'Close'
        var observer = new MutationObserver(function(mutations) {
          $('.bslib-full-screen-exit').each(function() {
            if ($(this).html().includes('Close')) {
              $(this).html($(this).html().replace('Close', 'Fechar'));
            }
          });
        });
  
        // Observe all elements with the class 'card bslib-card'
        $('.card.bslib-card').each(function() {
          observer.observe(this, { 
            attributes: true, 
            attributeFilter: ['data-full-screen'] 
          });
        });
      });
    "
    )
  ),

  sidebar = sidebar(
    uiOutput(outputId = "date_start_ui"),
    uiOutput(outputId = "date_end_ui")
  ),

  # Tabs
  nav_panel(
    title = "Mocajuba",
    page(
      card(
        plotOutput(outputId = "mocajuba_temp")
      ),
      card(
        plotOutput(outputId = "mocajuba_umid")
      ),
      card(
        plotOutput(outputId = "mocajuba_wind")
      ),
      card(
        plotOutput(outputId = "mocajuba_uv")
      ),
      card(
        plotOutput(outputId = "mocajuba_rain")
      ),
      card(
        plotOutput(outputId = "mocajuba_river")
      ),
      card(
        card_title("Download dos dados"),
        downloadButton(
          outputId = "mocajuba_download_temp",
          label = "Temperatura"
        ),
        downloadButton(
          outputId = "mocajuba_download_umid",
          label = "Umidade"
        ),
        downloadButton(
          outputId = "mocajuba_download_wind",
          label = "Vento e rajada"
        ),
        downloadButton(
          outputId = "mocajuba_download_uv",
          label = "UV"
        ),
        downloadButton(
          outputId = "mocajuba_download_rain",
          label = "Chuva"
        ),
        downloadButton(
          outputId = "mocajuba_download_river",
          label = "Nível do rio"
        )
      )
    )
  )
)

# Server
server <- function(input, output, session) {
  # Start date
  output$date_start_ui <- renderUI({
    dateInput(
      inputId = "date_start",
      label = "Data de início",
      value = today() - 7,
      language = "pt-BR"
    )
  })

  # End date
  output$date_end_ui <- renderUI({
    req(input$date_start)

    dateInput(
      inputId = "date_end",
      label = "Data de fim",
      value = today(),
      language = "pt-BR"
    )
  })

  # Mocajuba temp data
  pf_temp_data <- reactive({
    req(input$date_start)
    req(input$date_end)

    start_date <- as_date(input$date_start, tz = "UTC")
    end_date <- as_date(input$date_end, tz = "UTC")

    pf_temp |>
      filter(time >= start_date & time <= end_date)
  })

  # Mocajuba temp plot
  output$mocajuba_temp <- renderPlot({
    ggplot(pf_temp_data(), aes(x = time, y = value)) +
      geom_line() +
      labs(
        title = "Temperatura",
        subtitle = "Mocajuba, PA",
        caption = "LIS/ICICT/Fiocruz",
        x = "Data e hora",
        y = "Graus celsius"
      ) +
      theme_bw()
  })

  # Mocajuba temp download
  output$mocajuba_download_temp <- downloadHandler(
    filename = function() {
      # Use the selected dataset as the suggested file name
      paste0("mocajuba_temp", ".csv")
    },
    content = function(file) {
      # Write the dataset to the `file` that will be downloaded
      write_csv2(x = pf_temp_data() |> collect(), file = file)
    }
  )

  # Mocajuba umid data
  pf_umid_data <- reactive({
    req(input$date_start)
    req(input$date_end)

    start_date <- as_date(input$date_start, tz = "UTC")
    end_date <- as_date(input$date_end, tz = "UTC")

    pf_umid |>
      filter(time >= start_date & time <= end_date)
  })

  # Mocajuba umid plot
  output$mocajuba_umid <- renderPlot({
    ggplot(pf_umid_data(), aes(x = time, y = value)) +
      geom_line() +
      labs(
        title = "Umidade relativa do ar",
        subtitle = "Mocajuba, PA",
        caption = "LIS/ICICT/Fiocruz",
        x = "Data e hora",
        y = "%"
      ) +
      theme_bw()
  })

  # Mocajuba umid download
  output$mocajuba_download_umid <- downloadHandler(
    filename = function() {
      # Use the selected dataset as the suggested file name
      paste0("mocajuba_umid", ".csv")
    },
    content = function(file) {
      # Write the dataset to the `file` that will be downloaded
      write_csv2(x = pf_umid_data() |> collect(), file = file)
    }
  )

  # Mocajuba wind data
  pf_wind_data <- reactive({
    req(input$date_start)
    req(input$date_end)

    start_date <- as_date(input$date_start, tz = "UTC")
    end_date <- as_date(input$date_end, tz = "UTC")

    pf_wind |>
      filter(time >= start_date & time <= end_date)
  })

  # Mocajuba gust data
  pf_gust_data <- reactive({
    req(input$date_start)
    req(input$date_end)

    start_date <- as_date(input$date_start, tz = "UTC")
    end_date <- as_date(input$date_end, tz = "UTC")

    pf_gust |>
      filter(time >= start_date & time <= end_date)
  })

  # Mocajuba wind and gust plot
  output$mocajuba_wind <- renderPlot({
    res <- bind_rows(
      pf_wind_data() |> mutate(name = "Vento") |> collect(),
      pf_gust_data() |> mutate(name = "Rajada") |> collect()
    )
    ggplot(res, aes(x = time, y = value, col = name)) +
      geom_line() +
      labs(
        title = "Vento e rajada",
        subtitle = "Mocajuba, PA",
        caption = "LIS/ICICT/Fiocruz",
        x = "Data e hora",
        y = "m/s",
        col = ""
      ) +
      theme_bw() +
      theme(legend.position = "bottom", legend.direction = "horizontal")
  })

  # Mocajuba wind download
  output$mocajuba_download_wind <- downloadHandler(
    filename = function() {
      # Use the selected dataset as the suggested file name
      paste0("mocajuba_vento", ".csv")
    },
    content = function(file) {
      # Write the dataset to the `file` that will be downloaded
      write_csv2(
        x = bind_rows(
          pf_wind_data() |> mutate(name = "Vento") |> collect(),
          pf_gust_data() |> mutate(name = "Rajada") |> collect()
        ),
        file = file
      )
    }
  )

  # Mocajuba uv data
  pf_uv_data <- reactive({
    req(input$date_start)
    req(input$date_end)

    start_date <- as_date(input$date_start, tz = "UTC")
    end_date <- as_date(input$date_end, tz = "UTC")

    pf_uv |>
      filter(time >= start_date & time <= end_date)
  })

  # Mocajuba uv plot
  output$mocajuba_uv <- renderPlot({
    ggplot(pf_uv_data(), aes(x = time, y = value)) +
      geom_line() +
      labs(
        title = "Índice de Raios Ultravioletas",
        subtitle = "Mocajuba, PA",
        caption = "LIS/ICICT/Fiocruz",
        x = "Data e hora",
        y = "IUV"
      ) +
      theme_bw()
  })

  # Mocajuba uv download
  output$mocajuba_download_uv <- downloadHandler(
    filename = function() {
      # Use the selected dataset as the suggested file name
      paste0("mocajuba_uv", ".csv")
    },
    content = function(file) {
      # Write the dataset to the `file` that will be downloaded
      write_csv2(x = pf_uv_data() |> collect(), file = file)
    }
  )

  # Mocajuba rain data
  pf_rain_data <- reactive({
    req(input$date_start)
    req(input$date_end)

    start_date <- as_date(input$date_start, tz = "UTC")
    end_date <- as_date(input$date_end, tz = "UTC")

    pf_rain |>
      filter(time >= start_date & time <= end_date)
  })

  # Mocajuba rain plot
  output$mocajuba_rain <- renderPlot({
    ggplot(pf_rain_data(), aes(x = time, y = value)) +
      geom_line() +
      labs(
        title = "Chuva",
        subtitle = "Mocajuba, PA",
        caption = "LIS/ICICT/Fiocruz",
        x = "Data e hora",
        y = "m/s"
      ) +
      theme_bw()
  })

  # Mocajuba umid download
  output$mocajuba_download_rain <- downloadHandler(
    filename = function() {
      # Use the selected dataset as the suggested file name
      paste0("mocajuba_chuva", ".csv")
    },
    content = function(file) {
      # Write the dataset to the `file` that will be downloaded
      write_csv2(x = pf_rain_data() |> collect(), file = file)
    }
  )

  # Mocajuba river data
  pf_river_data <- reactive({
    req(input$date_start)
    req(input$date_end)

    start_date <- as_date(input$date_start, tz = "UTC")
    end_date <- as_date(input$date_end, tz = "UTC")

    pf_river |>
      filter(time >= start_date & time <= end_date)
  })

  # Mocajuba uv plot
  output$mocajuba_river <- renderPlot({
    ggplot(pf_river_data(), aes(x = time, y = value)) +
      geom_line() +
      labs(
        title = "Nível do rio",
        subtitle = "Mocajuba, PA",
        caption = "LIS/ICICT/Fiocruz",
        x = "Data e hora",
        y = "MCA",
        col = ""
      ) +
      theme_bw()
  })

  # Mocajuba river download
  output$mocajuba_download_river <- downloadHandler(
    filename = function() {
      # Use the selected dataset as the suggested file name
      paste0("mocajuba_rio", ".csv")
    },
    content = function(file) {
      # Write the dataset to the `file` that will be downloaded
      write_csv2(x = pf_river_data() |> collect(), file = file)
    }
  )
}

shinyApp(ui, server)
