source("plots/common.R")

df <- read_all_csv("archives/xp0_baseline.2023-03-10T19:07:34-light")

data.speed <- df$latency_ts %>%
    group_by(id, run, host_address) %>%
    summarise(count = max(count), duration = max(time), .groups = "drop") %>%
    group_by(id, run) %>%
    summarise(count = sum(count), duration = max(duration), speed = duration / count, .groups = "drop") %>%
    group_by(id) %>%
    summarise_mean(speed)

format_data <- function(.data) {
    config.levels <- c("xp0/cassandra-base.yaml", "xp0/cassandra-se.yaml")
    config.labels <- c("Vanilla", "Hector")

    .data %>%
        inner_join(df$input, by = "id") %>%
        filter(id %in% c("xp0_5", "xp0_6")) %>%
        mutate(config_file = factor(config_file, levels = config.levels, labels = config.labels))
}

tikz(file = "plots/output/xp0.throughput.tex", width = 1.25, height = 1.5)

plot.xp0.throughput <- ggplot(data = format_data(data.speed)) +
    geom_col(mapping = aes(x = config_file,
                           y = 1 / mean_speed,
                           fill = config_file),
             width = 0.7,
             colour = "black") +
    geom_errorbar(mapping = aes(x = config_file,
                                ymin = 1 / mean_high_speed,
                                ymax = 1 / mean_low_speed),
                  width = 0.2) +
    coord_cartesian(ylim = c(0, NA)) +
    scale_x_discrete(name = "Version") +
    scale_y_continuous(name = "Throughput (ops/s)") +
    scale_fill_discrete(name = "Version", guide = "none") +
    theme_bw() +
    theme(axis.title.x = element_blank(),
          axis.ticks.x = element_blank(),
          axis.text.x = element_blank())

update_theme(plot.xp0.throughput)

dev.off()
