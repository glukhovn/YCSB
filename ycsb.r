#!/usr/bin/env Rscript
# args[1],...

library(ggplot2)
library(dplyr)

args = commandArgs(trailingOnly=TRUE)

if (length(args) < 1)
  stop("Usage: ./ycsb.r <file.tsv>")

data_file <- args[1]
data <- basename(data_file)

theme_black = function(base_size = 12, base_family = "") {
  theme_grey(base_size = base_size, base_family = base_family) %+replace%
    theme(
      # Specify axis options
      axis.line = element_blank(),
      axis.text.x = element_text(size = base_size*0.8, color = "white", lineheight = 0.9),
      axis.text.y = element_text(size = base_size*0.8, color = "white", lineheight = 0.9),
      axis.ticks = element_line(color = "white", size  =  0.2),
      axis.title.x = element_text(size = base_size, color = "white", margin = margin(0, 10, 0, 0)),
      axis.title.y = element_text(size = base_size, color = "white", angle = 90, margin = margin(0, 10, 0, 0)),
      axis.ticks.length = unit(0.3, "lines"),
      # Specify legend options
      legend.background = element_rect(color = NA, fill = "black"),
      legend.key = element_rect(color = "white",  fill = "black"),
      legend.key.size = unit(1.2, "lines"),
      legend.key.height = NULL,
      legend.key.width = NULL,
      legend.text = element_text(size = base_size*0.8, color = "white"),
      legend.title = element_text(size = base_size*0.8, face = "bold", hjust = 0, color = "white"),
      legend.position = "right",
      legend.text.align = NULL,
      legend.title.align = NULL,
      legend.direction = "vertical",
      legend.box = NULL,
      # Specify panel options
      panel.background = element_rect(fill = "black", color  =  NA),
      panel.border = element_rect(fill = NA, color = "white"),
      panel.grid.major = element_line(color = "grey35"),
      panel.grid.minor = element_line(color = "grey20"),
      panel.margin = unit(0.5, "lines"),
      # Specify facetting options
      strip.background = element_rect(fill = "grey30", color = "grey10"),
      strip.text.x = element_text(size = base_size*0.8, color = "white"),
      strip.text.y = element_text(size = base_size*0.8, color = "white",angle = -90),
      # Specify plot options
      plot.background = element_rect(color = "black", fill = "black"),
      plot.title = element_text(size = base_size*1.2, color = "white",
                                margin = margin(t = 1, r = 1, b = 1, l = 1, unit = "lines")),
      plot.margin = unit(rep(1, 4), "lines")
    )
}

theme_title_mono <-
  theme(plot.title = element_text(family='mono', face="bold", size=20))

theme_legend <-
  theme(legend.text = element_text(size=12),
        axis.text.x = element_text(size=11))

theme_set(
  #  theme_bw(base_size = 20) +
  theme_black(base_size = 20) +
    theme(panel.grid.major = element_line(size = 1),
          legend.text = element_text(size=20),
          axis.text.x = element_text(size=16),
          legend.key.size = unit(1.5, "cm"),
          strip.text.x = element_text(size = 15),
          legend.key = element_rect(color='#000000', fill = '#444444')))

ycsb <- read.csv(file=data_file, header=TRUE, sep='\t')

ycsb <- transform(ycsb, throughput = throughput / 1000)

#ycsb <- transform(ycsb, db=gsub("-astro5", "", db))
#ycsb <- transform(ycsb, db=gsub('pgjsonb', 'pg', db))
#ycsb <- transform(ycsb, config=gsub('bt', 'btree', config))

ycsb <- transform(ycsb, db_config=paste(db, config, sep='-'))

#ycsb <- subset(ycsb, substr(workload, 1, 5) != 'load_')

workloads <- c(
  'workloada',
  'workloadb',
  'workloadc',
  'workloadd',
  'workloade',
  'workloadf'
)

config_colour_values <-
  c('mongo-w1'=hsv(0.3, 0.8, 0.8),
    'mongo-w1-onefield'=hsv(0.3, 0.8, 0.8),
    'mongo-w1j1'=hsv(0.3, 0.8, 0.8),

    'mysql-btree'=hsv(0.05, 0.8, 1),
    'mysql-btree_pk'=hsv(0.05, 0.8, 1),
    'mysql-btree_vanilla'=hsv(0.10, 0.8, 1),
    'mysql-btree_part4'=hsv(0.10, 0.8, 1),
    'mysql-unprepared-btree'=hsv(0.05, 0.8, 1),
    'mysql-btree_pk_uniform'=hsv(0.15, 0.8, 1),
    'mysql-btree_uniform'=hsv(0.15, 0.8, 1),

    'pg-btree'=hsv(0.60, 0.8, 1),
    'pg-btree_no-pglz'=hsv(0.65, 0.8, 1),
    'pg-btree_vanilla'=hsv(0.70, 0.8, 1),
    'pg-unprepared-btree'=hsv(0.60, 0.8, 1),
    'pg-btree_pk_uniform'=hsv(0.80, 0.8, 1),
    'pg-btree_uniform'=hsv(0.80, 0.8, 1)
  )

config_colours <-
  scale_color_manual(values=config_colour_values)

w=1200
h=600

ycsb_plot <- function() {
  file <- paste(data, '.png', sep = '')
  message(file)
  png(file, width=w, height=h)

  plot <-
    ggplot(data=ycsb,
           aes(x=clients, y=throughput), group=db_config) +
      geom_line(aes(color=db_config), size=1.5) +
      geom_point(aes(color=db_config, shape=db), size=3) +
      #config_colours +
      ylab('throughput, kop/s') +
      #ggtitle("") +
      facet_wrap(~workload, ncol=3)

  print(plot)

  dev.off()
}

ycsb_lat_plot <- function(wl, lat, a) {
  file <- paste(data, '-w', wl, '-', lat, '.png', sep = '')
  message(file)

  png(file, width=w, height=h)

  plot <-
    ggplot(data=subset(ycsb,
                       workload == paste('workload', wl, sep = '')),
           #aes(x=throughput, y=lat1p95),
           a,
           group=db_config) +
    geom_path(aes(color=db_config), size=1.5) +
    geom_point(aes(color=db_config, shape=db), size=3) +
    #config_colours +
    ylab('latency, us') +
    #ggtitle("") +
    facet_wrap(~workload, ncol=3);

  print(plot)

  dev.off()
}

ycsb_thr_plot <-function(wl, no_legend = FALSE, no_axis = FALSE, y_label = 'throughput, kop/s', suffix = '') {
  file <- paste(data, '-w', wl, suffix, '.png', sep = '')
  message(file)

  png(file, width=w, height=h)

  plot <-
    ggplot(data=subset(ycsb,
                       substr(workload, 9, 9) == wl |
                       substr(workload, 9, 9) == substr(wl, 1, 1) |
                       substr(workload, 9, 9) == substr(wl, 2, 2) |
                       substr(workload, 9, 9) == substr(wl, 3, 3)),
           #length(grep(substr(workload, 9, 9), wl)) > 0),
           aes(x=clients, y=throughput), group=db_config) +
    geom_line(aes(color=db_config), size=1.5) +
    geom_point(aes(color=db_config), size=3) + #, shape=db
    #scale_color_manual(values=config_colours) +
    #config_colours +
    ylab(y_label)
    #ggtitle("") +

  if (length(wl) > 1)
    plot <- plot + facet_wrap(~workload, ncol=3)
  else
    plot <- plot + xlab(paste('clients', toupper(wl), sep=' '))

  if (no_legend)
    plot <- plot + theme(legend.position="none")

  if (no_axis)
    plot <- plot + theme(axis.title.y=element_blank())

  print(plot)

  dev.off()
}

w=1200
h=600

ycsb_plot()

w=800
h=400

for (wl in c('a', 'b', 'c', 'd', 'e', 'f')) {
  ycsb_thr_plot(wl, no_legend=FALSE, no_axis=FALSE)

  for (lat in c('lat1p95', 'lat2p95')) {
    if (lat == 'lat1p95')
      a <- aes(x=throughput, y=lat1p95)
    else
      a <- aes(x=throughput, y=lat2p95)

    #print(a)

    if (lat == 'lat2p95' && wl %in% c('c', 'd'))
      next

    ycsb_lat_plot(wl, lat, a)
  }
}
