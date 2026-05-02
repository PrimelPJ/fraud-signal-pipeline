# analysis/anomaly_model.R
#
# NetWatch — Statistical Anomaly Modeling and CUSUM Calibration
#
# Ingests flow telemetry batches from the Java coordinator, fits
# baseline distributions, runs Isolation Forest scoring for multivariate
# anomaly detection, and recalibrates CUSUM parameters (k, h) by
# pushing updated threshold configs back to the coordinator's REST API.
#
# Dependencies: isotree, data.table, httr2, jsonlite
# Run: Rscript analysis/anomaly_model.R --telemetry-url http://localhost:8081/telemetry

suppressPackageStartupMessages({
  library(isotree)    # Isolation Forest implementation
  library(data.table) # High-performance tabular operations
  library(httr2)      # Modern HTTP client
  library(jsonlite)   # JSON parsing
  library(stats)      # Base distributions
})

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  cfg  <- list(
    telemetry_url  = "http://localhost:8081/telemetry",
    threshold_url  = "http://localhost:8081/api/thresholds",
    poll_interval  = 60L,           # seconds
    window_count   = 100L,          # min windows before fitting baselines
    if_ntrees      = 200L,          # Isolation Forest tree count
    if_sample_size = 256L,          # sub-sample size per tree
    if_threshold   = 0.65,          # anomaly score threshold
    cusum_ard      = 0.01,          # desired ARL (avg run length) for CUSUM design
    output_dir     = "output"
  )

  # Simple key=value arg parsing
  for (arg in args) {
    kv <- strsplit(arg, "=")[[1]]
    if (length(kv) == 2) cfg[[sub("^--", "", kv[1])]] <- kv[2]
  }

  dir.create(cfg$output_dir, showWarnings = FALSE)
  cfg
}

cfg <- parse_args()

# ---------------------------------------------------------------------------
# Telemetry ingestion
# ---------------------------------------------------------------------------

#' Fetch flow telemetry windows from the coordinator REST endpoint.
#'
#' @return data.table with one row per flow window, or NULL on error.
fetch_telemetry <- function(url) {
  tryCatch({
    resp <- request(url) |>
      req_headers("Accept" = "application/json") |>
      req_timeout(10) |>
      req_perform()

    raw <- resp |> resp_body_json(simplifyVector = TRUE)
    dt  <- as.data.table(raw)

    required_cols <- c(
      "flow_group", "window_start", "total_packets",
      "total_bytes", "avg_iat_us", "avg_entropy", "syn_ratio", "rst_ratio"
    )
    missing <- setdiff(required_cols, names(dt))
    if (length(missing) > 0) {
      warning("Telemetry missing columns: ", paste(missing, collapse = ", "))
      return(NULL)
    }

    # Derived features
    dt[, `:=`(
      bytes_per_pkt  = total_bytes  / pmax(total_packets, 1L),
      pkt_rate       = total_packets / 10.0,  # packets per second (10s window)
      entropy_norm   = avg_entropy / 8.0      # normalize to [0,1] (max 8 bits)
    )]

    message(sprintf("[telemetry] Fetched %d windows from %d flow groups",
                    nrow(dt), dt[, uniqueN(flow_group)]))
    dt
  }, error = function(e) {
    warning("[telemetry] Fetch failed: ", conditionMessage(e))
    NULL
  })
}

# ---------------------------------------------------------------------------
# Baseline distribution fitting
# ---------------------------------------------------------------------------

#' Fit per-metric baseline statistics for a given flow group.
#'
#' Baselines are modeled as Gaussian (for IAT, packet rate) or
#' empirical quantiles (for entropy). Returns a named list of
#' (mean, sd) per feature used in CUSUM calibration.
#'
#' @param dt      data.table — telemetry for one flow group
#' @return list with fitted parameters
fit_baseline <- function(dt) {
  fit_gaussian <- function(x) {
    x_clean <- x[is.finite(x)]
    if (length(x_clean) < 5L) return(list(mu = median(x), sigma = mad(x) + 1e-6))
    list(mu = mean(x_clean), sigma = sd(x_clean) + 1e-6)
  }

  list(
    pkt_rate  = fit_gaussian(dt$pkt_rate),
    avg_iat   = fit_gaussian(dt$avg_iat_us),
    entropy   = fit_gaussian(dt$entropy_norm),
    syn_ratio = fit_gaussian(dt$syn_ratio),
    rst_ratio = fit_gaussian(dt$rst_ratio)
  )
}

# ---------------------------------------------------------------------------
# CUSUM parameter calibration
# ---------------------------------------------------------------------------

#' Compute optimal CUSUM k and h from baseline distribution parameters.
#'
#' Uses the Wald approximation for the ARL-based threshold design:
#'   k = delta / (2 * sigma)     [shift to detect = delta = mu_1 - mu_0]
#'   h ≈ 2 * ln(ARL_0 / ARL_1)  [simplified, assumes known shift magnitude]
#'
#' @param baseline  list from fit_baseline()
#' @param delta_sd  expected shift magnitude in standard deviations (default: 2)
#' @param arl0      desired ARL under H0 (default: 500)
#' @return list(k, h)
calibrate_cusum <- function(baseline, delta_sd = 2.0, arl0 = 500.0) {
  sigma <- baseline$pkt_rate$sigma
  mu0   <- baseline$pkt_rate$mu

  delta <- delta_sd * sigma
  k     <- delta / 2.0

  # Approximate h for geometric ARL: h ≈ (2/delta) * log(arl0)
  h <- (2.0 / (delta + 1e-9)) * log(arl0)

  message(sprintf(
    "[cusum_calibrate] mu0=%.2f sigma=%.2f delta=%.2f k=%.4f h=%.4f",
    mu0, sigma, delta, k, h
  ))

  list(k = round(k, 4), h = round(h, 4), baseline_pkt_rate = mu0)
}

# ---------------------------------------------------------------------------
# Isolation Forest anomaly scoring
# ---------------------------------------------------------------------------

FEATURE_COLS <- c("pkt_rate", "avg_iat_us", "entropy_norm", "syn_ratio", "rst_ratio", "bytes_per_pkt")

#' Fit an Isolation Forest model on flow telemetry and score each window.
#'
#' @param dt    data.table with required feature columns
#' @param cfg   configuration list (if_ntrees, if_sample_size, if_threshold)
#' @return data.table with added columns: if_score, anomaly_flag
run_isolation_forest <- function(dt, cfg) {
  feature_dt <- dt[, .SD, .SDcols = FEATURE_COLS]
  feature_dt <- feature_dt[complete.cases(feature_dt)]

  if (nrow(feature_dt) < 20L) {
    message("[iso_forest] Insufficient data for fitting (n=", nrow(feature_dt), ")")
    dt[, `:=`(if_score = NA_real_, anomaly_flag = FALSE)]
    return(dt)
  }

  # Fit Isolation Forest
  model <- isolation.forest(
    X            = as.data.frame(feature_dt),
    ntrees       = as.integer(cfg$if_ntrees),
    sample_size  = as.integer(cfg$if_sample_size),
    ndim         = 1L,           # Standard iForest (not Extended)
    nthreads     = parallel::detectCores(logical = FALSE)
  )

  scores <- predict(model, as.data.frame(feature_dt), type = "score")

  result <- copy(dt[complete.cases(dt[, .SD, .SDcols = FEATURE_COLS])])
  result[, `:=`(
    if_score     = round(scores, 4),
    anomaly_flag = scores >= as.numeric(cfg$if_threshold)
  )]

  n_anomalies <- result[, sum(anomaly_flag)]
  message(sprintf("[iso_forest] Scored %d windows | anomalies=%d (threshold=%.2f)",
                  nrow(result), n_anomalies, as.numeric(cfg$if_threshold)))

  result
}

# ---------------------------------------------------------------------------
# Threshold update push to coordinator
# ---------------------------------------------------------------------------

#' Push recalibrated CUSUM thresholds to the coordinator REST endpoint.
push_thresholds <- function(thresholds_list, url) {
  body <- toJSON(thresholds_list, auto_unbox = TRUE)

  tryCatch({
    resp <- request(url) |>
      req_method("PUT") |>
      req_headers("Content-Type" = "application/json") |>
      req_body_raw(body, type = "application/json") |>
      req_timeout(5) |>
      req_perform()

    if (resp_status(resp) %in% 200:204) {
      message("[thresholds] Successfully pushed ", length(thresholds_list), " group thresholds")
    } else {
      warning("[thresholds] Unexpected status: ", resp_status(resp))
    }
  }, error = function(e) {
    warning("[thresholds] Push failed: ", conditionMessage(e))
  })
}

# ---------------------------------------------------------------------------
# Main analysis loop
# ---------------------------------------------------------------------------

telemetry_history <- data.table()

repeat {
  message("\n[", Sys.time(), "] Running analysis cycle")

  dt_new <- fetch_telemetry(cfg$telemetry_url)

  if (!is.null(dt_new) && nrow(dt_new) > 0) {
    telemetry_history <- rbindlist(list(telemetry_history, dt_new), use.names = TRUE, fill = TRUE)

    # Keep rolling history of last 2000 windows
    if (nrow(telemetry_history) > 2000L) {
      telemetry_history <- tail(telemetry_history, 2000L)
    }
  }

  if (nrow(telemetry_history) < cfg$window_count) {
    message(sprintf("[loop] Collecting baseline data (%d/%d windows)",
                    nrow(telemetry_history), cfg$window_count))
    Sys.sleep(cfg$poll_interval)
    next
  }

  # Per-group baseline fitting and CUSUM calibration
  groups <- telemetry_history[, unique(flow_group)]
  thresholds <- lapply(setNames(groups, groups), function(g) {
    group_dt <- telemetry_history[flow_group == g]
    if (nrow(group_dt) < 10L) return(NULL)
    baseline <- fit_baseline(group_dt)
    calibrate_cusum(baseline)
  })
  thresholds <- Filter(Negate(is.null), thresholds)

  if (length(thresholds) > 0) {
    push_thresholds(thresholds, cfg$threshold_url)
  }

  # Isolation Forest scoring on latest batch
  if (!is.null(dt_new) && nrow(dt_new) >= 20L) {
    scored <- run_isolation_forest(dt_new, cfg)
    anomalies <- scored[anomaly_flag == TRUE]
    if (nrow(anomalies) > 0) {
      message("[anomalies] Detected ", nrow(anomalies), " anomalous windows")
      print(anomalies[, .(flow_group, pkt_rate, entropy_norm, if_score)])
    }

    # Persist scored batch
    out_path <- file.path(cfg$output_dir,
                          sprintf("scored_%s.csv", format(Sys.time(), "%Y%m%d_%H%M%S")))
    fwrite(scored, out_path)
  }

  Sys.sleep(cfg$poll_interval)
}
