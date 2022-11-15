suppressPackageStartupMessages({
    library(tibble)
    library(readr)
    library(dplyr)
    library(tidyr)
    library(ggplot2)
    library(tikzDevice)
})

output_dir <- "archives/xp1_diff_ds_primary.2022-11-10T17:06:14-light"

df.in <- read_csv(paste0(output_dir, "/input.csv"), col_types = "ccicciiddddccdccccciiic")
df.lat <- read_csv(paste0(output_dir, "/latency.csv"), col_types = "dddddddddddddci")

tikz(file = "plots/output/xp1.tex", width = 2.75, height = 1.6)

TO_MS <- 1 / 1e6

lev.key_dist <- c("ApproximatedZipf(1000000000,0.5)", "ApproximatedZipf(1000000000,1.5)")
lab.key_dist <- c("$X\\sim\\mathrm{Zipf}(0.5)$", "$X\\sim\\mathrm{Zipf}(1.5)$")

lev.config_file <- c("cassandra-ds-fifo.yaml", "cassandra-primary-fifo.yaml")
lab.config_file <- c("DS", "Primary")

df.lat %>%
    inner_join(df.in, by = "id") %>%
    filter(name %in% c("xp1_szipf_ds_nom", "xp1_szipf_primary_nom", "xp1_lzipf_ds_nom", "xp1_lzipf_primary_nom")) %>%
    group_by(id, name, key_dist, config_file) %>%
    summarise(p99 = mean(p99), .groups = "drop") %>%
    mutate(key_dist = factor(key_dist, levels = lev.key_dist, labels = lab.key_dist),
           config_file = factor(config_file, levels = lev.config_file, labels = lab.config_file)) %>%
    ggplot() %+%
    geom_col(mapping = aes(x = config_file, y = p99 * TO_MS, fill = config_file),
             position = position_dodge(), width = 0.5, colour = "black") %+%
    facet_wrap(vars(key_dist), scales = "free") %+%
    scale_x_discrete(name = "Scheduling algorithm") %+%
    scale_y_continuous(name = "Latency (ms)") %+%
    scale_fill_grey(guide = "none", start = 0.1, end = 0.9) %+%
    theme_bw(base_size = 10) %+%
    theme(panel.spacing = unit(0.2, "inches"),
          axis.title = element_text(size = rel(0.8)),
          axis.title.x = element_text(margin = margin(t = 8)),
          axis.title.y = element_text(margin = margin(r = 8)),
          strip.background = element_rect(fill = "white"),
          strip.text = element_text(size = rel(0.8), margin = margin(t = 3, b = 3)))

dev.off()
