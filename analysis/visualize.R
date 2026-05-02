# analysis/visualize.R
#
# NetWatch — Anomaly Visualization
# Generates ggplot2 heatmaps, time-series plots, and feature distribution
# charts from scored flow telemetry. Outputs publication-quality PNG files.

suppressPackageStartupMessages({
  library(ggplot2)
  library(data.table)
  library(scales)
})

theme_netwatch <- function() {
  theme_minimal(base_size = 12) +
    theme(
      plot.title       = element_text(face = "bold", size = 14),
      plot.subtitle    = element_text(colour = "grey40"),
      panel.grid.minor = element_blank(),
      strip.text       = element_text(face = "bold"),
      legend.position  = "bottom"
    )
}

#' Anomaly score heatmap across flow groups and time.
plot_anomaly_heatmap <- function(dt, out_dir = "output") {
  dt[, window_ts := as.POSIXct(window_start / 1000, origin = "1970-01-01")]

  p <- ggplot(dt, aes(x = window_ts, y = flow_group, fill = if_score)) +
    geom_tile(colour = "white", linewidth = 0.2) +
    scale_fill_gradient2(
      name     = "Anomaly Score",
      low      = "#2166ac",
      mid      = "#f7f7f7",
      high     = "#d73027",
      midpoint = 0.5,
      limits   = c(0, 1),
      labels   = percent_format(accuracy = 1)
    ) +
    geom_tile(
      data = dt[anomaly_flag == TRUE],
      aes(x = window_ts, y = flow_group),
      fill = NA, colour = "#d73027", linewidth = 0.8
    ) +
    scale_x_datetime(date_labels = "%H:%M", date_breaks = "5 min") +
    labs(
      title    = "NetWatch — Flow Anomaly Score Heatmap",
      subtitle = sprintf("Isolation Forest scores (threshold = 0.65) | %d flow groups | red border = flagged",
                         dt[, uniqueN(flow_group)]),
      x = "Window Time (UTC)",
      y = "Flow Group"
    ) +
    theme_netwatch()

  path <- file.path(out_dir, "anomaly_heatmap.png")
  ggsave(path, p, width = 14, height = max(4, dt[, uniqueN(flow_group)] * 0.5 + 2), dpi = 150)
  message("[viz] Saved: ", path)
  invisible(p)
}

#' Packets-per-second time series with CUSUM signal overlay.
plot_velocity_series <- function(dt, out_dir = "output") {
  p <- ggplot(dt, aes(x = as.POSIXct(window_start / 1000, origin = "1970-01-01"),
                       y = pkt_rate, colour = flow_group)) +
    geom_line(linewidth = 0.7, alpha = 0.8) +
    geom_point(
      data = dt[anomaly_flag == TRUE],
      aes(shape = "Anomaly"), size = 3, colour = "#d73027"
    ) +
    scale_colour_brewer(palette = "Set2", name = "Flow Group") +
    scale_shape_manual(values = c("Anomaly" = 4), name = NULL) +
    facet_wrap(~ flow_group, scales = "free_y", ncol = 2) +
    labs(
      title    = "NetWatch — Packet Rate Time Series",
      subtitle = "10-second tumbling windows | ✕ = Isolation Forest anomaly",
      x        = "Time (UTC)",
      y        = "Packets / second"
    ) +
    theme_netwatch() +
    theme(legend.position = "none")

  path <- file.path(out_dir, "velocity_series.png")
  ggsave(path, p, width = 14, height = 8, dpi = 150)
  message("[viz] Saved: ", path)
  invisible(p)
}

#' Feature distribution comparison: normal vs anomalous flows.
plot_feature_distributions <- function(dt, out_dir = "output") {
  features <- c("pkt_rate", "avg_iat_us", "entropy_norm", "syn_ratio", "rst_ratio")
  dt_long <- melt(
    dt[, c(features, "anomaly_flag"), with = FALSE],
    id.vars = "anomaly_flag",
    variable.name = "feature",
    value.name    = "value"
  )
  dt_long[, class := ifelse(anomaly_flag, "Anomalous", "Normal")]

  p <- ggplot(dt_long[is.finite(value)], aes(x = value, fill = class)) +
    geom_density(alpha = 0.55, adjust = 1.5) +
    scale_fill_manual(values = c("Normal" = "#2166ac", "Anomalous" = "#d73027"), name = "Class") +
    facet_wrap(~ feature, scales = "free", ncol = 3) +
    labs(
      title    = "NetWatch — Feature Distributions by Anomaly Class",
      subtitle = "Density estimates | Normal vs Isolation Forest flagged windows",
      x        = "Feature Value",
      y        = "Density"
    ) +
    theme_netwatch()

  path <- file.path(out_dir, "feature_distributions.png")
  ggsave(path, p, width = 14, height = 7, dpi = 150)
  message("[viz] Saved: ", path)
  invisible(p)
}

# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)
scored_csv <- if (length(args) >= 1) args[1] else "output/latest_scored.csv"
out_dir    <- if (length(args) >= 2) args[2] else "output"

if (file.exists(scored_csv)) {
  dt <- fread(scored_csv)
  dir.create(out_dir, showWarnings = FALSE)
  plot_anomaly_heatmap(dt, out_dir)
  plot_velocity_series(dt, out_dir)
  plot_feature_distributions(dt, out_dir)
  message("[viz] All plots generated in: ", out_dir)
} else {
  message("[viz] Scored CSV not found: ", scored_csv, " — skipping plot generation")
}
