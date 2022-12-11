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

output_dir <- "archives/xp2_baseline_sat_vs_pop-light"

df.in <- read_csv(paste0(output_dir, "/input.csv"), col_types = "ccicciiddddccdccccciiic")
df.ts <- read_csv(paste0(output_dir, "/timeseries.csv"), col_types = "ddddddddddddddddccdcic")

df.thrpt <- df.ts %>%
    inner_join(df.in, by = "id") %>%
    filter(name %in% c("xp2_szipf_base_sat", "xp2_szipf_se_sat",
                       "xp2_mzipf_base_sat", "xp2_mzipf_se_sat",
                       "xp2_lzipf_base_sat", "xp2_lzipf_se_sat")) %>%
    group_by(id, name, run, key_dist, config_file) %>%
    summarise(throughput = max(mean_rate), .groups = "drop")

df.diff <- df.thrpt %>%
    pivot_wider(names_from = name, values_from = throughput, id_cols = c(run, key_dist)) %>%
    unite("base", c(xp2_szipf_base_sat, xp2_mzipf_base_sat, xp2_lzipf_base_sat), na.rm = TRUE) %>%
    unite("se", c(xp2_szipf_se_sat, xp2_mzipf_se_sat, xp2_lzipf_se_sat), na.rm = TRUE) %>%
    mutate(base = as.numeric(base),
           se = as.numeric(se),
           diff = (se - base) / base) %>%
    group_by(key_dist) %>%
    summarise(mean_diff = mean(diff))

print(df.diff, n = Inf)

df.xp2 <- df.thrpt %>%
    group_by(id, name, key_dist, config_file) %>%
    summarise(mean_throughput = mean(throughput),
              sd_throughput = sd(throughput),
              cv_throughput = sd_throughput / mean_throughput,
              min_throughput = min(throughput),
              max_throughput = max(throughput), .groups = "drop")

print(df.xp2, n = Inf)

# tikz(file = "plots/output/xp2_thrpt.tex", width = 2.75, height = 2)
#
# lev.key_dist <- c("ApproximatedZipf(1000000000,0.33)",
#                   "ApproximatedZipf(1000000000,0.66)",
#                   "ApproximatedZipf(1000000000,0.99)")
# lab.key_dist <- c("0.33", "0.66", "0.99")
#
# lev.config_file <- c("cassandra-base.yaml", "cassandra-ds-fifo.yaml")
# lab.config_file <- c("Cassandra", "Hector")
#
# df.xp2 %>%
#     mutate(key_dist = factor(key_dist, levels = lev.key_dist, labels = lab.key_dist),
#            config_file = factor(config_file, levels = lev.config_file, labels = lab.config_file)) %>%
#     ggplot(mapping = aes(x = key_dist)) %+%
#     geom_col(mapping = aes(y = mean_throughput, fill = config_file),
#              position = position_dodge(width = 0.6),
#              colour = "black",
#              width = 0.4) %+%
#     geom_errorbar(mapping = aes(ymin = min_throughput, ymax = max_throughput, group = config_file),
#                   position = position_dodge(width = 0.6),
#                   width = 0.1) %+%
#     coord_cartesian(ylim = c(0, NA)) %+%  # Make sure y axis starts at 0
#     scale_x_discrete(name = "Popularity bias") %+%
#     scale_y_continuous(name = "Throughput (reqs/s)") %+%
#     scale_fill_discrete(name = "Version") %+%
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
