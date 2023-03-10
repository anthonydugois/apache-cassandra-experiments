source("plots/common.R")

df <- read_all_csv("archives/xp0_baseline.2023-01-24T17:14:14-light")

data.latency <- df$latency %>%
    group_by(id, stat_name) %>%
    summarise_mean(stat_value)

format_data <- function(.data) {
    stats.levels <- c("mean", "p50", "p95.0", "p99.0")
    stats.labels <- c("Mean", "Median", "P95", "P99")

    config.levels <- c("xp0/cassandra-base.yaml", "xp0/cassandra-se.yaml")
    config.labels <- c("Vanilla", "Hector")

    rate.levels <- c("fixed=200000", "fixed=500000")
    rate.labels <- c("200K", "500K")

    .data %>%
        inner_join(df$input, by = "id") %>%
        filter(stat_name %in% stats.levels) %>%
        mutate(stat_name = factor(stat_name, levels = stats.levels, labels = stats.labels),
               config_file = factor(config_file, levels = config.levels, labels = config.labels),
               main_rate_limit = factor(main_rate_limit, levels = rate.levels, labels = rate.labels))
}

tikz(file = "plots/output/xp0.latency.tex", width = 3.5, height = 1.5)

plot.xp0.latency <- ggplot(data = format_data(data.latency)) +
    geom_col(mapping = aes(x = config_file,
                           y = mean_stat_value * NANOS_TO_MILLIS,
                           fill = config_file),
             width = 0.7,
             colour = "black") +
    geom_errorbar(mapping = aes(x = config_file,
                                ymin = mean_low_stat_value * NANOS_TO_MILLIS,
                                ymax = mean_high_stat_value * NANOS_TO_MILLIS),
                  width = 0.2) +
    facet_grid(rows = vars(main_rate_limit),
               cols = vars(stat_name),
               scales = "free") +
    coord_cartesian(ylim = c(0, NA)) +
    scale_x_discrete(name = "Version") +
    scale_y_continuous(name = "Latency (ms)") +
    scale_fill_discrete(name = "Version") +
    theme_bw() +
    theme(axis.title.x = element_blank(),
          axis.ticks.x = element_blank(),
          axis.text.x = element_blank())

update_theme(plot.xp0.latency)

dev.off()
