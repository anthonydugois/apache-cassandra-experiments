# id,name,repeat,hosts,rf,read_ratio,write_ratio,keys,ops,duration,rampup_rate_limit,main_rate_limit,warmup_rate_limit,key_dist,key_size,value_size_dist,docker_image,config_file,driver_config_file,workload_config_file,clients,client_threads,cycle_per_stride
# xp2_1,xp2_unif_fifo_1,6,15,3,100,0,3000000,,600,fixed=1000,fixed=30000,fixed=10000,"Uniform(1,3000000)",16,"FixedValues(1000,1000,1000,1000000)",adugois1/apache-cassandra-se:latest,xp2/cassandra-ds-fifo-4.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp2_2,xp2_unif_rml_1,6,15,3,100,0,3000000,,600,fixed=1000,fixed=30000,fixed=10000,"Uniform(1,3000000)",16,"FixedValues(1000,1000,1000,1000000)",adugois1/apache-cassandra-se:latest,xp2/cassandra-ds-rml-4.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp2_3,xp2_unif_fifo_2,6,15,3,100,0,3000000,,600,fixed=1000,fixed=40000,fixed=10000,"Uniform(1,3000000)",16,"FixedValues(1000,1000,1000,1000000)",adugois1/apache-cassandra-se:latest,xp2/cassandra-ds-fifo-4.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp2_4,xp2_unif_rml_2,6,15,3,100,0,3000000,,600,fixed=1000,fixed=40000,fixed=10000,"Uniform(1,3000000)",16,"FixedValues(1000,1000,1000,1000000)",adugois1/apache-cassandra-se:latest,xp2/cassandra-ds-rml-4.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp2_5,xp2_unif_fifo_3,6,15,3,100,0,3000000,,600,fixed=1000,fixed=50000,fixed=10000,"Uniform(1,3000000)",16,"FixedValues(1000,1000,1000,1000000)",adugois1/apache-cassandra-se:latest,xp2/cassandra-ds-fifo-4.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp2_6,xp2_unif_rml_3,6,15,3,100,0,3000000,,600,fixed=1000,fixed=50000,fixed=10000,"Uniform(1,3000000)",16,"FixedValues(1000,1000,1000,1000000)",adugois1/apache-cassandra-se:latest,xp2/cassandra-ds-rml-4.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp2_7,xp2_unif_fifo_4,6,15,3,100,0,3000000,,600,fixed=1000,fixed=60000,fixed=10000,"Uniform(1,3000000)",16,"FixedValues(1000,1000,1000,1000000)",adugois1/apache-cassandra-se:latest,xp2/cassandra-ds-fifo-4.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp2_8,xp2_unif_rml_4,6,15,3,100,0,3000000,,600,fixed=1000,fixed=60000,fixed=10000,"Uniform(1,3000000)",16,"FixedValues(1000,1000,1000,1000000)",adugois1/apache-cassandra-se:latest,xp2/cassandra-ds-rml-4.yaml,without-token-map.conf,cql.yaml,5,360,100

source("plots/common.R")

df <- read_all_csv("archives/xp2_rampup_local_scheduling.2023-03-31T19:26:46-light")

data.latency <- df$latency %>%
    group_by(id, stat_name) %>%
    summarise_mean(stat_value)

data.latency.small <- df$small_latency %>%
    group_by(id, stat_name) %>%
    summarise_mean(stat_value)

data.latency.large <- df$large_latency %>%
    group_by(id, stat_name) %>%
    summarise_mean(stat_value)

data.speed <- df$latency_ts %>%
    group_by(id, run, host_address) %>%
    summarise(count = max(count), duration = max(time), .groups = "drop") %>%
    group_by(id, run) %>%
    summarise(count = sum(count), duration = max(duration), speed = duration / count, .groups = "drop") %>%
    group_by(id) %>%
    summarise_mean(speed)

format_speed <- function(.data) {
    config.levels <- c("xp2/cassandra-ds-fifo-4.yaml", "xp2/cassandra-ds-rml-4.yaml")
    config.labels <- c("FIFO", "RML")

    rates <- seq(30000, 60000, 10000)
    rate.levels <- paste0("fixed=", rates)
    rate.labels <- rates * OPSS_TO_KOPSS

    .data %>%
        inner_join(df$input, by = "id") %>%
        mutate(config_file = factor(config_file, levels = config.levels, labels = config.labels),
               main_rate_limit = factor(main_rate_limit, levels = rate.levels, labels = rate.labels))
}

tikz(file = "plots/output/xp2_throughput.icpp.tex", width = 2.8, height = 1.5)

plot.xp2.throughput <- ggplot(data = format_speed(data.speed)) +
    geom_col(mapping = aes(x = main_rate_limit,
                           y = 1 / mean_speed * OPSS_TO_KOPSS,
                           fill = config_file),
             width = 0.6,
             colour = "black",
             position = position_dodge2(padding = 0.2)) +
    geom_errorbar(mapping = aes(x = main_rate_limit,
                                ymin = 1 / mean_high_speed * OPSS_TO_KOPSS,
                                ymax = 1 / mean_low_speed * OPSS_TO_KOPSS,
                                group = config_file),
                  width = 0.2,
                  position = position_dodge(width = 0.6)) +
    coord_cartesian(ylim = c(0, NA)) +
    scale_x_discrete(name = "Fixed rate (Kops/s)") +
    scale_y_continuous(name = "Throughput (Kops/s)") +
    scale_fill_discrete(name = "Strategy") +
    theme_bw()

update_theme_for_latex(plot.xp2.throughput)

dev.off()

format_latency <- function(.data) {
    stats.levels <- c("mean", "p50", "p95.0", "p99.0")
    stats.labels <- c("Mean", "Median", "P95", "P99")

    config.levels <- c("xp2/cassandra-ds-fifo-4.yaml", "xp2/cassandra-ds-rml-4.yaml")
    config.labels <- c("FIFO", "RML")

    rates <- seq(30000, 60000, 10000)
    rate.levels <- paste0("fixed=", rates)
    rate.labels <- rates * OPSS_TO_KOPSS

    .data %>%
        inner_join(df$input, by = "id") %>%
        filter(stat_name %in% stats.levels) %>%
        mutate(stat_name = factor(stat_name, levels = stats.levels, labels = stats.labels),
               config_file = factor(config_file, levels = config.levels, labels = config.labels),
               main_rate_limit = factor(main_rate_limit, levels = rate.levels, labels = rate.labels))
}

tikz(file = "plots/output/xp2_latency.icpp.tex", width = 3.4, height = 2)

plot.xp2.latency <- ggplot(data = format_latency(data.latency)) +
    geom_line(mapping = aes(x = main_rate_limit,
                            y = mean_stat_value * NANOS_TO_MILLIS,
                            colour = config_file,
                            group = config_file),
              alpha = 0.6) +
    geom_point(mapping = aes(x = main_rate_limit,
                             y = mean_stat_value * NANOS_TO_MILLIS,
                             colour = config_file,
                             shape = config_file),
               size = 0.6) +
    facet_wrap(vars(stat_name), scales = "free_y") +
    coord_cartesian(ylim = c(0, NA)) +
    scale_x_discrete(name = "Fixed rate (Kops/s)") +
    scale_y_continuous(name = "Latency (ms)") +
    scale_colour_discrete(name = "Strategy") +
    scale_shape_discrete(name = "Strategy") +
    theme_bw()

update_theme_for_latex(plot.xp2.latency)

dev.off()
