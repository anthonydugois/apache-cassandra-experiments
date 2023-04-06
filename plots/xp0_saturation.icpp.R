# id,name,repeat,hosts,rf,read_ratio,write_ratio,keys,ops,duration,rampup_rate_limit,main_rate_limit,warmup_rate_limit,key_dist,key_size,value_size_dist,docker_image,config_file,driver_config_file,workload_config_file,clients,client_threads,cycle_per_stride
# xp0_1,xp0_low_base,10,15,3,100,0,35000000,,600.0,fixed=10000,fixed=200000,fixed=10000,"ApproximatedZipf(35000000,0.9)",16,"FixedValues(1000,1000,1000,10000,10000,100000)",adugois1/apache-cassandra-base:latest,xp0/cassandra-base.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp0_2,xp0_low_se,10,15,3,100,0,35000000,,600.0,fixed=10000,fixed=200000,fixed=10000,"ApproximatedZipf(35000000,0.9)",16,"FixedValues(1000,1000,1000,10000,10000,100000)",adugois1/apache-cassandra-se:latest,xp0/cassandra-se.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp0_3,xp0_high_base,10,15,3,100,0,35000000,,600.0,fixed=10000,fixed=500000,fixed=10000,"ApproximatedZipf(35000000,0.9)",16,"FixedValues(1000,1000,1000,10000,10000,100000)",adugois1/apache-cassandra-base:latest,xp0/cassandra-base.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp0_4,xp0_high_se,10,15,3,100,0,35000000,,600.0,fixed=10000,fixed=500000,fixed=10000,"ApproximatedZipf(35000000,0.9)",16,"FixedValues(1000,1000,1000,10000,10000,100000)",adugois1/apache-cassandra-se:latest,xp0/cassandra-se.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp0_5,xp0_sat_base,10,15,3,100,0,35000000,100000000.0,,fixed=10000,,fixed=10000,"ApproximatedZipf(35000000,0.9)",16,"FixedValues(1000,1000,1000,10000,10000,100000)",adugois1/apache-cassandra-base:latest,xp0/cassandra-base.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp0_6,xp0_sat_se,10,15,3,100,0,35000000,100000000.0,,fixed=10000,,fixed=10000,"ApproximatedZipf(35000000,0.9)",16,"FixedValues(1000,1000,1000,10000,10000,100000)",adugois1/apache-cassandra-se:latest,xp0/cassandra-se.yaml,without-token-map.conf,cql.yaml,5,360,100

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

tikz(file = "plots/output/xp0_throughput.icpp.tex", width = 2, height = 1.5)

plot.xp0.throughput <- ggplot(data = format_data(data.speed)) +
    geom_col(mapping = aes(x = config_file,
                           y = 1 / mean_speed * OPSS_TO_KOPSS,
                           fill = config_file),
             width = 0.6,
             colour = "black") +
    geom_errorbar(mapping = aes(x = config_file,
                                ymin = 1 / mean_high_speed * OPSS_TO_KOPSS,
                                ymax = 1 / mean_low_speed * OPSS_TO_KOPSS),
                  width = 0.2) +
    coord_cartesian(ylim = c(0, NA)) +
    scale_x_discrete(name = "Version") +
    scale_y_continuous(name = "Throughput (Kops/s)") +
    scale_fill_discrete(name = "Version") +
    theme_bw() +
    theme(axis.title.x = element_blank())

update_theme_for_latex(plot.xp0.throughput)

dev.off()
