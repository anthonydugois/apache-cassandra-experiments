# id,name,repeat,hosts,rf,read_ratio,write_ratio,keys,ops,duration,rampup_rate_limit,main_rate_limit,warmup_rate_limit,key_dist,key_size,value_size_dist,docker_image,config_file,driver_config_file,workload_config_file,clients,client_threads,cycle_per_stride
# xp3_1,xp3_szipf_ds,10,15,3,100,0,600000000,100000000,,fixed=200000,,fixed=10000,"ApproximatedZipf(600000000,0.1)",16,FixedValue(1000),adugois1/apache-cassandra-se:latest,xp3/cassandra-ds.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp3_2,xp3_szipf_pa,10,15,3,100,0,600000000,100000000,,fixed=200000,,fixed=10000,"ApproximatedZipf(600000000,0.1)",16,FixedValue(1000),adugois1/apache-cassandra-se:latest,xp3/cassandra-pa.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp3_3,xp3_szipf_c3,10,15,3,100,0,600000000,100000000,,fixed=200000,,fixed=10000,"ApproximatedZipf(600000000,0.1)",16,FixedValue(1000),adugois1/apache-cassandra-se:latest,xp3/cassandra-c3.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp3_4,xp3_lzipf_ds,10,15,3,100,0,600000000,100000000,,fixed=200000,,fixed=10000,"ApproximatedZipf(600000000,1.5)",16,FixedValue(1000),adugois1/apache-cassandra-se:latest,xp3/cassandra-ds.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp3_5,xp3_lzipf_pa,10,15,3,100,0,600000000,100000000,,fixed=200000,,fixed=10000,"ApproximatedZipf(600000000,1.5)",16,FixedValue(1000),adugois1/apache-cassandra-se:latest,xp3/cassandra-pa.yaml,without-token-map.conf,cql.yaml,5,360,100
# xp3_6,xp3_lzipf_c3,10,15,3,100,0,600000000,100000000,,fixed=200000,,fixed=10000,"ApproximatedZipf(600000000,1.5)",16,FixedValue(1000),adugois1/apache-cassandra-se:latest,xp3/cassandra-c3.yaml,without-token-map.conf,cql.yaml,5,360,100

source("plots/common.R")

df <- read_all_csv("archives/xp3_replica_selection.2023-01-25T17:27:26-light")

data.speed <- df$latency_ts %>%
    group_by(id, run, host_address) %>%
    summarise(count = max(count), duration = max(time), .groups = "drop") %>%
    group_by(id, run) %>%
    summarise(count = sum(count), duration = max(duration), speed = duration / count, .groups = "drop")

data.read.all <- df$dstat_hosts %>%
    group_by(id, run, host_address) %>%
    summarise(read = sum(dsk_sda5__read), duration = max(time), rate = read / duration, .groups = "drop")

data.read.mean <- data.read.all %>%
    group_by(id, run) %>%
    summarise_mean(rate)

RUN_DURATION_MIN <- 10

format_speed <- function(.data) {
    config.levels <- c("xp3/cassandra-ds.yaml", "xp3/cassandra-c3.yaml", "xp3/cassandra-pa.yaml")
    config.labels <- c("DS", "C3", "PA")

    pop.levels <- c("ApproximatedZipf(600000000,0.1)", "ApproximatedZipf(600000000,1.5)")
    pop.labels <- c("$\\mathrm{Zipf}(0.1)$", "$\\mathrm{Zipf}(1.5)$")

    .data %>%
        inner_join(df$input, by = "id") %>%
        mutate(config_file = factor(config_file, levels = config.levels, labels = config.labels),
               key_dist = factor(key_dist, levels = pop.levels, labels = pop.labels))
}

tikz(file = "plots/output/xp3_throughput.icpp.tex", width = 3.25, height = 1.6)

plot.xp3.throughput <- ggplot(data = format_speed(data.speed)) +
    geom_line(mapping = aes(x = run * RUN_DURATION_MIN,
                            y = 1 / speed * OPSS_TO_KOPSS,
                            colour = config_file),
              size = 0.6,
              alpha = 0.6) +
    geom_point(mapping = aes(x = run * RUN_DURATION_MIN,
                             y = 1 / speed * OPSS_TO_KOPSS,
                             colour = config_file,
                             shape = config_file),
               size = 0.6) +
    annotate(geom = "rect",
             xmin = 0,
             xmax = RUN_DURATION_MIN,
             ymin = 0,
             ymax = Inf,
             fill = "black",
             alpha = 0.1) +
    annotate(geom = "text",
             x = 0.5 * RUN_DURATION_MIN,
             y = 625,
             angle = 90,
             size = 1.8,
             colour = "black",
             alpha = 0.8,
             label = "N/A") +
    facet_wrap(vars(key_dist)) +
    coord_cartesian(xlim = c(0, NA),
                    ylim = c(0, NA)) +
    scale_x_continuous(name = "Runtime (min)") +
    scale_y_continuous(name = "Throughput (kops/s)",
                       breaks = seq(0, 1250, 250)) +
    scale_colour_discrete(name = "Strategy") +
    scale_shape_discrete(name = "Strategy") +
    theme_bw()

update_theme_for_latex(plot.xp3.throughput)

dev.off()

format_read <- function(.data) {
    config.levels <- c("xp3/cassandra-ds.yaml", "xp3/cassandra-c3.yaml", "xp3/cassandra-pa.yaml")
    config.labels <- c("DS", "C3", "PA")

    pop.levels <- c("ApproximatedZipf(600000000,0.1)", "ApproximatedZipf(600000000,1.5)")
    pop.labels <- c("$\\mathrm{Zipf}(0.1)$", "$\\mathrm{Zipf}(1.5)$")

    .data %>%
        inner_join(df$input, by = "id") %>%
        mutate(config_file = factor(config_file, levels = config.levels, labels = config.labels),
               key_dist = factor(key_dist, levels = pop.levels, labels = pop.labels))
}

tikz(file = "plots/output/xp3_read.icpp.tex", width = 3.25, height = 1.6)

plot.xp3.read <- ggplot(data = format_read(data.read.mean)) +
    geom_line(data = format_read(data.read.all),
              mapping = aes(x = run * RUN_DURATION_MIN,
                            y = rate * B_TO_MB,
                            colour = config_file,
                            group = interaction(config_file, host_address)),
              size = 0.2,
              alpha = 0.15) +
    geom_line(mapping = aes(x = run * RUN_DURATION_MIN,
                            y = mean_rate * B_TO_MB,
                            colour = config_file),
              size = 0.6,
              alpha = 0.6) +
    geom_point(mapping = aes(x = run * RUN_DURATION_MIN,
                             y = mean_rate * B_TO_MB,
                             colour = config_file,
                             shape = config_file),
               size = 0.6) +
    annotate(geom = "rect",
             xmin = 0,
             xmax = RUN_DURATION_MIN,
             ymin = 0,
             ymax = Inf,
             fill = "black",
             alpha = 0.1) +
    annotate(geom = "text",
             x = 0.5 * RUN_DURATION_MIN,
             y = 225,
             angle = 90,
             size = 1.8,
             colour = "black",
             alpha = 0.8,
             label = "N/A") +
    facet_wrap(vars(key_dist)) +
    coord_cartesian(xlim = c(0, NA),
                    ylim = c(0, NA)) +
    scale_x_continuous(name = "Runtime (min)") +
    scale_y_continuous(name = "Disk-read (MB/s)",
                       breaks = seq(0, 450, 100)) +
    scale_colour_discrete(name = "Strategy") +
    scale_shape_discrete(name = "Strategy") +
    theme_bw()

update_theme_for_latex(plot.xp3.read)

dev.off()
