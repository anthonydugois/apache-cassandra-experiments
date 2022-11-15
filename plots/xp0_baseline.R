suppressPackageStartupMessages({
    library(tibble)
    library(readr)
    library(dplyr)
    library(tidyr)
    library(ggplot2)
    library(tikzDevice)
})

binned <- function(.data, bin_width) {
    floor(.data / bin_width)
}

output_dir <- "archives/xp0_baseline.2022-11-14T19:19:06-light"

df.in <- read_csv(paste0(output_dir, "/input.csv"), col_types = "ccicciiddddccdccccciiic")
df.ts <- read_csv(paste0(output_dir, "/timeseries.csv"), col_types = "ddddddddddddddddccdcic")
df.lat <- read_csv(paste0(output_dir, "/latency.csv"), col_types = "dddddddddddddci")

tikz(file = "plots/output/xp0_ts.tex", width = 6, height = 4)

BIN_WIDTH <- 8
START_TIME <- 200
END_TIME <- 800
TO_MS <- 1 / 1e6

lev.name <- c("xp0_zipf_base_nom", "xp0_zipf_se_nom")
lab.name <- c("Cassandra", "Hector")

df.ts %>%
    inner_join(df.in, by = "id") %>%
    filter(name %in% c("xp0_zipf_base_nom", "xp0_zipf_se_nom")) %>%
    group_by(id, name, run) %>%
    mutate(bin = binned(time, BIN_WIDTH)) %>%
    ungroup() %>%
    pivot_longer(c(mean, p75, p50, p99), names_to = "stat_name", values_to = "stat_value") %>%
    group_by(id, name, run, bin, stat_name) %>%
    summarise(stat_value = mean(stat_value), .groups = "drop") %>%  # Aggregate values in the same bin
    group_by(id, name, bin, stat_name) %>%
    summarise(mean_stat_value = mean(stat_value),
              min_stat_value = min(stat_value),
              max_stat_value = max(stat_value), .groups = "drop") %>%  # Aggregate values between runs
    mutate(time_in_seconds = bin * BIN_WIDTH,
           name = factor(name, levels = lev.name, labels = lab.name)) %>%
    filter(time_in_seconds > START_TIME, time_in_seconds < END_TIME) %>%
    ggplot(mapping = aes(x = time_in_seconds)) %+%
    geom_ribbon(mapping = aes(ymin = min_stat_value * TO_MS,
                              ymax = max_stat_value * TO_MS,
                              fill = name), alpha = 0.2) %+%
    geom_line(mapping = aes(y = mean_stat_value * TO_MS,
                            colour = name), size = 0.75) %+%
    facet_wrap(vars(stat_name), scales = "free") %+%
    coord_cartesian(ylim = c(0, NA)) %+%  # Make sure y axis starts at 0
    scale_x_continuous(name = "Time (s)") %+%
    scale_y_continuous(name = "Response time (ms)") %+%
    scale_colour_discrete(name = "Version") %+%
    scale_fill_discrete(name = "Version") %+%
    theme_bw(base_size = 10) %+%
    theme(panel.spacing = unit(0.2, "inches"),
          axis.title = element_text(size = rel(0.8)),
          axis.title.x = element_text(margin = margin(t = 8)),
          axis.title.y = element_text(margin = margin(r = 8)),
          strip.background = element_rect(fill = "white"),
          strip.text = element_text(size = rel(0.8), margin = margin(t = 3, b = 3)),
          legend.title = element_text(size = rel(0.8)),
          legend.position = "bottom")

dev.off()

tikz(file = "plots/output/xp0_lat.tex", width = 3, height = 3)

TO_MS <- 1 / 1e6

lev.name <- c("xp0_zipf_base_nom", "xp0_zipf_se_nom")
lab.name <- c("Cassandra", "Hector")

df.lat %>%
    inner_join(df.in, by = "id") %>%
    filter(name %in% c("xp0_zipf_base_nom", "xp0_zipf_se_nom")) %>%
    pivot_longer(c(mean, p50, p75, p99), names_to = "stat_name", values_to = "stat_value") %>%
    group_by(id, name, stat_name) %>%
    summarise(mean_stat_value = mean(stat_value),
              min_stat_value = min(stat_value),
              max_stat_value = max(stat_value), .groups = "drop") %>%  # Aggregate values between runs
    mutate(name = factor(name, levels = lev.name, labels = lab.name)) %>%
    ggplot(mapping = aes(x = name)) %+%
    geom_col(mapping = aes(y = mean_stat_value * TO_MS,
                           fill = name), colour = "black", width = 0.5) %+%
    geom_errorbar(mapping = aes(ymin = min_stat_value * TO_MS,
                                ymax = max_stat_value * TO_MS), width = 0.1) %+%
    facet_wrap(vars(stat_name), scales = "free") %+%
    coord_cartesian(ylim = c(0, NA)) %+%  # Make sure y axis starts at 0
    scale_x_discrete(name = "Version") %+%
    scale_y_continuous(name = "Response time (ms)") %+%
    scale_fill_discrete(guide = "none") %+%
    theme_bw(base_size = 10) %+%
    theme(panel.spacing = unit(0.2, "inches"),
          axis.title = element_text(size = rel(0.8)),
          axis.title.x = element_blank(),
          axis.title.y = element_text(margin = margin(r = 8)),
          strip.background = element_rect(fill = "white"),
          strip.text = element_text(size = rel(0.8), margin = margin(t = 3, b = 3)),
          legend.title = element_text(size = rel(0.8)))

dev.off()
