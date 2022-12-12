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

output_dir <- "archives/xp1_diff_ds_primary.2022-11-10T17:06:14-light"

df.in <- read_csv(paste0(output_dir, "/input.csv"), col_types = "ccicciiddddccdccccciiic")
df.dstat <- read_csv(paste0(output_dir, "/dstat_hosts.csv"), col_types = "dddddddddddddddddddddddcic")
df.lat <- read_csv(paste0(output_dir, "/latency.csv"), col_types = "dddddddddddddci")

print(df.lat %>%
          inner_join(df.in, by = "id") %>%
          filter(name %in% c("xp1_szipf_ds_nom", "xp1_szipf_primary_nom", "xp1_lzipf_ds_nom", "xp1_lzipf_primary_nom")) %>%
          group_by(id, name, key_dist, config_file) %>%
          summarise(p50 = mean(p50),
                    p99 = mean(p99), .groups = "drop") %>%  # Aggregate values between runs
          pivot_longer(c(p50, p99), names_to = "stat_name", values_to = "stat_value"))

# tikz(file = "plots/output/xp1_lat.tex", width = 2.75, height = 1.6)
#
# TO_MS <- 1 / 1e6
#
# lev.key_dist <- c("ApproximatedZipf(1000000000,0.5)", "ApproximatedZipf(1000000000,1.5)")
# lab.key_dist <- c("$\\mathrm{Zipf}(0.5)$", "$\\mathrm{Zipf}(1.5)$")
#
# lev.config_file <- c("cassandra-ds-fifo.yaml", "cassandra-primary-fifo.yaml")
# lab.config_file <- c("DS", "Primary")
#
# df.lat %>%
#     inner_join(df.in, by = "id") %>%
#     filter(name %in% c("xp1_szipf_ds_nom", "xp1_szipf_primary_nom", "xp1_lzipf_ds_nom", "xp1_lzipf_primary_nom")) %>%
#     group_by(id, name, key_dist, config_file) %>%
#     summarise(p99 = mean(p99), .groups = "drop") %>%
#     mutate(key_dist = factor(key_dist, levels = lev.key_dist, labels = lab.key_dist),
#            config_file = factor(config_file, levels = lev.config_file, labels = lab.config_file)) %>%
#     ggplot() %+%
#     geom_col(mapping = aes(x = config_file, y = p99 * TO_MS, fill = config_file),
#              position = position_dodge(), width = 0.5, colour = "black") %+%
#     facet_wrap(vars(key_dist), scales = "free") %+%
#     scale_x_discrete(name = "Scheduling algorithm") %+%
#     scale_y_continuous(name = "Latency (ms)") %+%
#     scale_fill_grey(guide = "none", start = 0.1, end = 0.9) %+%
#     theme_bw(base_size = 10) %+%
#     theme(panel.spacing = unit(0.2, "inches"),
#           axis.title = element_text(size = rel(0.8)),
#           axis.title.x = element_text(margin = margin(t = 8)),
#           axis.title.y = element_text(margin = margin(r = 8)),
#           strip.background = element_rect(fill = "white"),
#           strip.text = element_text(size = rel(0.8), margin = margin(t = 3, b = 3)))
#
# dev.off()
#
# tikz(file = "plots/output/xp1_lat2.tex", width = 3, height = 2)
#
# TO_MS <- 1 / 1e6
#
# lev.key_dist <- c("ApproximatedZipf(1000000000,0.5)", "ApproximatedZipf(1000000000,1.5)")
# lab.key_dist <- c("$\\mathrm{Zipf}(0.5)$", "$\\mathrm{Zipf}(1.5)$")
#
# lev.config_file <- c("cassandra-ds-fifo.yaml", "cassandra-primary-fifo.yaml")
# lab.config_file <- c("DS", "Primary")
#
# df.lat %>%
#     inner_join(df.in, by = "id") %>%
#     filter(name %in% c("xp1_szipf_ds_nom", "xp1_szipf_primary_nom", "xp1_lzipf_ds_nom", "xp1_lzipf_primary_nom")) %>%
#     group_by(id, name, key_dist, config_file) %>%
#     summarise(p50 = mean(p50),
#               p99 = mean(p99), .groups = "drop") %>%  # Aggregate values between runs
#     pivot_longer(c(p50, p99), names_to = "stat_name", values_to = "stat_value") %>%
#     mutate(key_dist = factor(key_dist, levels = lev.key_dist, labels = lab.key_dist),
#            config_file = factor(config_file, levels = lev.config_file, labels = lab.config_file)) %>%
#     ggplot() %+%
#     geom_col(mapping = aes(x = key_dist, y = stat_value * TO_MS, fill = config_file),
#              position = position_dodge(0.6), width = 0.4, colour = "black") %+%
#     facet_wrap(vars(stat_name), scales = "free_y") %+%
#     scale_x_discrete(name = "Popularity bias") %+%
#     scale_y_continuous(name = "Response time (ms)") %+%
#     scale_fill_discrete(name = "Replica selection") %+%
#     theme_bw(base_size = 10) %+%
#     theme(panel.spacing = unit(0.2, "inches"),
#           axis.title = element_text(size = rel(0.8)),
#           axis.title.x = element_text(margin = margin(t = 8)),
#           axis.title.y = element_text(margin = margin(r = 8)),
#           strip.background = element_rect(fill = "white"),
#           strip.text = element_text(size = rel(0.8), margin = margin(t = 3, b = 3)),
#           legend.title = element_text(size = rel(0.8)),
#           legend.position = "bottom",
#           legend.key.size = unit(0.12, "inches"))
#
# dev.off()
#
# tikz(file = "plots/output/xp1_read.tex", width = 3.25, height = 2)
#
# BIN_WIDTH <- 8
# TO_MB <- 1 / 1e6
#
# lev.key_dist <- c("ApproximatedZipf(1000000000,0.5)", "ApproximatedZipf(1000000000,1.5)")
# lab.key_dist <- c("$\\mathrm{Zipf}(0.5)$", "$\\mathrm{Zipf}(1.5)$")
#
# lev.config_file <- c("cassandra-ds-fifo.yaml", "cassandra-primary-fifo.yaml")
# lab.config_file <- c("DS", "Primary")
#
# df.dstat %>%
#     inner_join(df.in, by = "id") %>%
#     filter(name %in% c("xp1_szipf_ds_nom", "xp1_szipf_primary_nom", "xp1_lzipf_ds_nom", "xp1_lzipf_primary_nom")) %>%
#     group_by(id, name, run, host_address, key_dist, config_file) %>%
#     mutate(bin = binned(time, BIN_WIDTH)) %>%
#     ungroup() %>%
#     group_by(id, name, run, bin, host_address, key_dist, config_file) %>%
#     summarise(read = mean(dsk_sda5__read), .groups = "drop") %>%  # Aggregate values in the same bin
#     group_by(id, name, bin, host_address, key_dist, config_file) %>%
#     summarise(mean_read = mean(read),
#               min_read = min(read),
#               max_read = max(read), .groups = "drop") %>%  # Aggregate values between runs
#     group_by(id, name, bin, key_dist, config_file) %>%
#     summarise(median_read = median(mean_read),
#               p25_read = quantile(mean_read, 0.25),
#               p75_read = quantile(mean_read, 0.75), .groups = "drop") %>%  # Aggregate values between hosts
#     mutate(time_in_seconds = bin * BIN_WIDTH,
#            key_dist = factor(key_dist, levels = lev.key_dist, labels = lab.key_dist),
#            config_file = factor(config_file, levels = lev.config_file, labels = lab.config_file)) %>%
#     ggplot(mapping = aes(x = time_in_seconds)) %+%
#     geom_ribbon(mapping = aes(ymin = p25_read * TO_MB,
#                               ymax = p75_read * TO_MB,
#                               fill = config_file), alpha = 0.2) %+%
#     geom_line(mapping = aes(y = median_read * TO_MB, colour = config_file)) %+%
#     facet_wrap(vars(key_dist), scales = "free_x") %+%
#     coord_cartesian(ylim = c(0, NA)) %+%  # Make sure y axis starts at 0
#     scale_x_continuous(name = "Time (s)") %+%
#     scale_y_continuous(name = "Disk read (MB)") %+%
#     scale_colour_discrete(name = "Replica selection") %+%
#     scale_fill_discrete(name = "Replica selection") %+%
#     theme_bw(base_size = 10) %+%
#     theme(panel.spacing = unit(0.2, "inches"),
#           axis.title = element_text(size = rel(0.8)),
#           axis.title.x = element_text(margin = margin(t = 8)),
#           axis.title.y = element_text(margin = margin(r = 8)),
#           strip.background = element_rect(fill = "white"),
#           strip.text = element_text(size = rel(0.8), margin = margin(t = 3, b = 3)),
#           legend.title = element_text(size = rel(0.8)),
#           legend.position = "bottom",
#           legend.key.size = unit(0.12, "inches"))
#
# dev.off()
