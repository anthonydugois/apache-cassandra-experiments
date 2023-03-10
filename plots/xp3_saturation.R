source("plots/common.R")

df <- read_all_csv("archives/xp3_replica_selection.2023-01-25T17:27:26-light")

data.speed <- df$latency_ts %>%
    group_by(id, run, host_address) %>%
    summarise(count = max(count), duration = max(time), .groups = "drop") %>%
    group_by(id, run) %>%
    summarise(count = sum(count), duration = max(duration), speed = duration / count, .groups = "drop")

data.read <- df$dstat_hosts %>%
    group_by(id, run, host_address) %>%
    summarise(read = sum(dsk_sda5__read), duration = max(time), rate = read / duration, .groups = "drop") %>%
    group_by(id, run) %>%
    summarise(rate = mean(rate), .groups = "drop")

format_speed <- function(.data) {
    config.levels <- c("xp3/cassandra-c3.yaml", "xp3/cassandra-ds.yaml", "xp3/cassandra-pa.yaml")
    config.labels <- c("C3", "DS", "PA")

    pop.levels <- c("ApproximatedZipf(600000000,0.1)", "ApproximatedZipf(600000000,1.5)")
    pop.labels <- c("$\\mathrm{Zipf}(0.1)$", "$\\mathrm{Zipf}(1.5)$")

    .data %>%
        inner_join(df$input, by = "id") %>%
        mutate(config_file = factor(config_file, levels = config.levels, labels = config.labels),
               key_dist = factor(key_dist, levels = pop.levels, labels = pop.labels))
}

tikz(file = "plots/output/xp3.throughput.tex", width = 2.75, height = 1.5)

plot.xp3.throughput <- ggplot(data = format_speed(data.speed)) +
    geom_line(mapping = aes(x = run,
                            y = 1 / speed,
                            colour = config_file)) +
    geom_point(mapping = aes(x = run,
                             y = 1 / speed,
                             colour = config_file),
               size = 0.2) +
    facet_wrap(vars(key_dist)) +
    coord_cartesian(ylim = c(0, NA)) +
    scale_x_continuous(name = "Run", breaks = seq(1, 10)) +
    scale_y_continuous(name = "Throughput (ops/s)", breaks = seq(0, 1250000, 250000), labels = scales::scientific) +
    scale_colour_discrete(name = "Strategy", guide = "none") +
    theme_bw()

update_theme(plot.xp3.throughput)

dev.off()

format_read <- function(.data) {
    config.levels <- c("xp3/cassandra-c3.yaml", "xp3/cassandra-ds.yaml", "xp3/cassandra-pa.yaml")
    config.labels <- c("C3", "DS", "PA")

    pop.levels <- c("ApproximatedZipf(600000000,0.1)", "ApproximatedZipf(600000000,1.5)")
    pop.labels <- c("$\\mathrm{Zipf}(0.1)$", "$\\mathrm{Zipf}(1.5)$")

    .data %>%
        inner_join(df$input, by = "id") %>%
        mutate(config_file = factor(config_file, levels = config.levels, labels = config.labels),
               key_dist = factor(key_dist, levels = pop.levels, labels = pop.labels))
}

tikz(file = "plots/output/xp3.read.tex", width = 3, height = 1.5)

plot.xp3.read <- ggplot(data = format_read(data.read)) +
    geom_line(mapping = aes(x = run,
                            y = rate * B_TO_MB,
                            colour = config_file)) +
    geom_point(mapping = aes(x = run,
                             y = rate * B_TO_MB,
                             colour = config_file),
               size = 0.2) +
    facet_wrap(vars(key_dist)) +
    coord_cartesian(ylim = c(0, NA)) +
    scale_x_continuous(name = "Run", breaks = seq(1, 10)) +
    scale_y_continuous(name = "Disk-read (MB/s)") +
    scale_colour_discrete(name = "Strategy") +
    theme_bw()

update_theme(plot.xp3.read)

dev.off()
