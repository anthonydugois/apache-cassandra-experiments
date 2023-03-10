suppressMessages({
    library(tibble)
    library(readr)
    library(dplyr)
    library(tidyr)
    library(purrr)
    library(ggplot2)
    library(scales)
    library(tikzDevice)
})

NANOS_TO_MILLIS <- 1 / 1e6

B_TO_KB <- 1 / 1e3
B_TO_MB <- 1 / 1e6
B_TO_GB <- 1 / 1e9

binned <- function(.data, bin_width) {
    floor(.data / bin_width)
}

read_all_csv <- function(.path) {
    all_csv_files <- list.files(path = .path, pattern = "*.csv", full.names = TRUE)
    csv_filenames <- sub("\\.csv$", "", basename(all_csv_files))

    all_csv_files %>%
        set_names(nm = csv_filenames) %>%
        map(~read_csv(., show_col_types = FALSE))
}

summarise_mean <- function(.data, col) {
    .data %>%
        summarise("mean_{{ col }}" := mean({ { col } }),
                  "mean_error_{{ col }}" := 2.262 * sd({ { col } }) / sqrt(n()),
                  "mean_low_{{ col }}" := mean({ { col } }) - 2.262 * sd({ { col } }) / sqrt(n()),
                  "mean_high_{{ col }}" := mean({ { col } }) + 2.262 * sd({ { col } }) / sqrt(n()),
                  "var_{{ col }}" := var({ { col } }),
                  "sd_{{ col }}" := sd({ { col } }),
                  "min_{{ col }}" := min({ { col } }),
                  "max_{{ col }}" := max({ { col } }),
                  "median_{{ col }}" := median({ { col } }),
                  .groups = "drop")
}

update_theme <- function(.plot) {
    .plot +
        theme(panel.spacing = unit(0.025, "inches"),
              legend.title = element_text(size = rel(0.8)),
              legend.text = element_text(size = rel(0.8)),
              legend.margin = margin(0, 0, 0, 0),
              legend.box.margin = margin(0, 0, 0, -3),
              legend.key.size = unit(0.15, "inches"),
              strip.text = element_text(size = rel(0.7), margin = margin(t = 2, b = 2, l = 2, r = 2)),
              axis.title = element_text(size = rel(0.8)),
              axis.text = element_text(size = rel(0.6)))
}
