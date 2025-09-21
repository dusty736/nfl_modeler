#' Build TeamStrength v0.1 (SRS–EPA Lite)
#'
#' Compute weekly team ratings from play-by-play using a simple,
#' transparent pipeline:
#' (1) filter & weight plays, (2) team-game EPA, (3) EWMA smoothing by week,
#' (4) one-pass schedule correction using lagged opponent strength, and
#' (5) weekly z-scoring across teams.
#'
#' @param pbp A play-by-play data frame with at least:
#'   `game_id, posteam, defteam, epa, home_wp_post, wpa, penalty, no_play`.
#'   Ideally also `season, week`; if missing, supply \code{games}.
#' @param games Optional games/schedule table with
#'   `game_id, season, week, game_type, home_team, away_team`.
#'   Used to attach `season/week` if absent in \code{pbp} and to build the
#'   complete (season, week, team) grid (incl. byes).
#' @param w_min Numeric. Garbage-time weight floor. Default \code{0.25}.
#' @param H Numeric. Recency half-life in weeks for EWMA. Default \code{4}.
#' @param beta Numeric. Schedule correction weight \eqn{\beta}. Default \code{0.7}.
#' @param min_eff_plays Numeric. Minimum effective weighted plays per team-game
#'   for inclusion. Default \code{20}.
#' @param keep_components Logical. Also compute offense/defense component ratings
#'   (same scaling). Default \code{FALSE}.
#'
#' @return A tibble with one row per \code{season, week, team} containing:
#' \itemize{
#'   \item \code{rating_net} — primary weekly z-score (0 = league average)
#'   \item \code{rating_off, rating_def} — optional component z-scores
#'   \item \code{net_epa_smooth} — pre-scaling smoothed net EPA/play
#'   \item \code{sos} — schedule strength index used (lagged opponent avg)
#'   \item \code{n_plays_eff} — cumulative effective plays to date
#'   \item \code{params_version}, \code{run_timestamp}
#' }
#'
#' @details
#' **Weights.** Pre-snap home WP reconstructed as
#' \code{home_wp_pre = clip(home_wp_post - wpa, 0, 1)}.
#' Competitiveness weight \code{w = max(w_min, 1 - 2*abs(home_wp_pre - 0.5))}.
#'
#' **EWMA.** Uses \code{alpha = 1 - 2^(-1/H)} via the recursion
#' \code{s_t = alpha * x_t + (1 - alpha) * s_{t-1}} with NA-carry on byes.
#'
#' **Schedule correction.** \code{sos_t} is the cumulative mean of opponents'
#' smoothed net ratings **lagged by one week** at the time of each game;
#' \code{adj_net_t = net_epa_smooth_t - beta * sos_t}.
#'
#' **Scaling.** Within each \code{season, week}: z-score across all teams
#' with non-missing \code{adj_net_t}.
#'
#' Season types included: REG and POST. PRE is excluded.
#'
#' @examples
#' \dontrun{
#' ratings <- build_team_strength_v01(pbp, games, keep_components = FALSE)
#' }
#' @export
build_team_strength_v01 <- function(
    pbp,
    games = NULL,
    w_min = 0.25,
    H = 4,
    beta = 0.7,
    min_eff_plays = 20,
    keep_components = FALSE
) {
  .require_pkgs(c("dplyr","tidyr","tibble","rlang"))
  
  pbp <- .ts_v01_attach_season_week(pbp, games)
  
  # 1) Filter & weight plays
  pbp_w <- .ts_v01_filter_weight_plays(pbp, w_min = w_min)
  
  # 2) Team-game aggregates (off, def, net); exclude low-coverage games
  tg <- .ts_v01_team_game_aggregates(
    pbp_w,
    min_eff_plays = min_eff_plays,
    keep_components = keep_components
  )
  
  # 2b) Team-week aggregates (handle rare multi-game weeks; keep byes as NA rows)
  tw <- .ts_v01_team_week_from_team_game(
    tg,
    games = games,
    keep_components = keep_components
  )
  
  # 3) EWMA (half-life H) with NA-carry on bye weeks
  alpha <- 1 - 2^(-1 / H)
  tw_sm <- .ts_v01_ewma_by_team_week(
    tw,
    alpha = alpha,
    keep_components = keep_components
  )
  
  # 4) Schedule correction (lag opponent's smoothed net by 1 week; cumulative mean)
  tw_adj <- .ts_v01_schedule_adjust(
    tw_sm,
    tg,               # to know which weeks each team actually played + opponent
    beta = beta,
    keep_components = keep_components
  )
  
  # 5) Weekly scaling to z-scores
  ratings <- .ts_v01_scale_weekly(
    tw_adj,
    keep_components = keep_components
  )
  
  # Final adornments
  ratings$params_version <- sprintf("v0.1_wmin%.2f_H%.0f_beta%.1f", w_min, H, beta)
  ratings$run_timestamp  <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  
  dplyr::arrange(ratings, season, week, team)
}

# ---- helpers (internal) -----------------------------------------------------

# Replace the whole function with this
.ts_v01_attach_season_week <- function(pbp, games = NULL) {
  # only require columns truly needed here
  needed <- c("game_id","posteam","defteam","epa","home_wp_post","wpa")
  miss <- setdiff(needed, names(pbp))
  if (length(miss)) rlang::abort(paste0("pbp is missing required columns: ", paste(miss, collapse=", ")))
  
  has_sw <- all(c("season","week") %in% names(pbp))
  if (has_sw) return(pbp)
  
  if (is.null(games)) {
    rlang::abort("pbp lacks `season`/`week` and `games` was not provided.")
  }
  
  sw <- games %>%
    dplyr::select(game_id, season, week, game_type) %>%
    dplyr::mutate(game_type = as.character(game_type))
  
  pbp %>%
    dplyr::left_join(sw, by = "game_id") %>%
    dplyr::filter(game_type %in% c("REG","POST")) %>%
    dplyr::select(-game_type)
}


# Replace the whole function with this
.ts_v01_filter_weight_plays <- function(pbp, w_min = 0.25) {
  # default-missing boolean flags to FALSE
  for (nm in c("qb_kneel","qb_spike","penalty","no_play","play_deleted","aborted_play")) {
    if (!nm %in% names(pbp)) pbp[[nm]] <- FALSE
  }
  # derive a conservative 'no_play' if it's missing: deleted/aborted count as no-play
  pbp$no_play <- isTRUE(pbp$no_play) | isTRUE(pbp$play_deleted) | isTRUE(pbp$aborted_play)
  
  # reconstruct pre-snap home WP and competitiveness weight
  pbp <- pbp %>%
    dplyr::mutate(
      home_wp_pre_raw = home_wp_post - wpa,
      home_wp_pre = pmin(pmax(home_wp_pre_raw, 0), 1),
      w_comp = pmax(w_min, 1 - 2 * abs(home_wp_pre - 0.5))
    )
  
  pbp %>%
    dplyr::filter(
      is.finite(epa),
      !qb_kneel, !qb_spike,
      !penalty,  !no_play,
      is.finite(home_wp_post), is.finite(wpa)
    ) %>%
    dplyr::mutate(w = w_comp)
}

#' @keywords internal
.ts_v01_team_game_aggregates <- function(pbp_w, min_eff_plays = 20, keep_components = FALSE) {
  # opponent mapping from offense perspective
  opp_map <- pbp_w %>%
    dplyr::filter(!is.na(defteam), !is.na(posteam)) %>%
    dplyr::group_by(game_id, team = posteam) %>%
    dplyr::summarise(opponent = dplyr::first(defteam), .groups = "drop")
  
  off <- pbp_w %>%
    dplyr::group_by(season, week, game_id, team = posteam) %>%
    dplyr::summarise(
      off_epa_game = stats::weighted.mean(epa, w),
      w_off = sum(w),
      .groups = "drop"
    )
  
  def_allowed <- pbp_w %>%
    dplyr::group_by(season, week, game_id, team = defteam) %>%
    dplyr::summarise(
      opp_off_epa_game = stats::weighted.mean(epa, w),
      w_def = sum(w),
      .groups = "drop"
    ) %>%
    dplyr::mutate(def_epa_game = - opp_off_epa_game)
  
  tg <- off %>%
    dplyr::full_join(def_allowed, by = c("season","week","game_id","team")) %>%
    dplyr::left_join(opp_map, by = c("game_id","team")) %>%
    dplyr::mutate(
      w_off = dplyr::coalesce(w_off, 0),
      w_def = dplyr::coalesce(w_def, 0),
      n_plays_eff_game = w_off + w_def
    ) %>%
    dplyr::filter(n_plays_eff_game >= min_eff_plays)
  
  tg <- tg %>%
    dplyr::mutate(
      net_game = off_epa_game + def_epa_game
    )
  
  if (!keep_components) {
    tg <- dplyr::select(tg, season, week, game_id, team, opponent,
                        net_game, n_plays_eff_game)
  }
  tg
}

#' @keywords internal
.ts_v01_team_week_from_team_game <- function(tg, games = NULL, keep_components = FALSE) {
  # Base weekly grid from games to include byes
  if (!is.null(games)) {
    grid <- games %>%
      dplyr::filter(game_type %in% c("REG","POST")) %>%
      dplyr::select(season, week, home = home_team, away = away_team) %>%
      tidyr::pivot_longer(c("home","away"), values_to = "team", names_to = "side") %>%
      dplyr::select(season, week, team) %>%
      dplyr::distinct()
  } else {
    grid <- tg %>%
      dplyr::select(season, week, team) %>%
      dplyr::distinct()
  }
  
  # Aggregate multiple games in same week (rare) using effective-play weights
  agg_funs <- list(
    net_week = ~ stats::weighted.mean(.x, w = .env$wts)
  )
  if (keep_components) {
    agg_funs$off_week <- ~ stats::weighted.mean(.x, w = .env$wts)
    agg_funs$def_week <- ~ stats::weighted.mean(.x, w = .env$wts)
  }
  
  if (keep_components) {
    tg_local <- tg %>% dplyr::mutate(wts = n_plays_eff_game)
    tw <- tg_local %>%
      dplyr::group_by(season, week, team) %>%
      dplyr::summarise(
        net_week = stats::weighted.mean(net_game, wts),
        off_week = stats::weighted.mean(off_epa_game, wts),
        def_week = stats::weighted.mean(def_epa_game, wts),
        n_plays_eff_week = sum(n_plays_eff_game),
        .groups = "drop"
      )
  } else {
    tw <- tg %>%
      dplyr::group_by(season, week, team) %>%
      dplyr::summarise(
        net_week = stats::weighted.mean(net_game, n_plays_eff_game),
        n_plays_eff_week = sum(n_plays_eff_game),
        .groups = "drop"
      )
  }
  
  # Join to full grid (so byes appear with NA net_week)
  grid %>%
    dplyr::left_join(tw, by = c("season","week","team")) %>%
    dplyr::arrange(season, team, week)
}

#' @keywords internal
.ts_v01_ewma_by_team_week <- function(tw, alpha, keep_components = FALSE) {
  by_team <- dplyr::group_by(tw, season, team)
  smooth_one <- function(x) {
    if (length(x) == 0) return(x)
    out <- rep(NA_real_, length(x))
    first_idx <- which(is.finite(x))[1]
    if (is.na(first_idx)) return(out)
    out[first_idx] <- x[first_idx]
    if (length(x) > first_idx) {
      for (i in (first_idx+1):length(x)) {
        if (is.finite(x[i])) {
          out[i] <- alpha * x[i] + (1 - alpha) * out[i-1]
        } else {
          out[i] <- out[i-1] # carry on bye/no-game
        }
      }
    }
    out
  }
  
  if (keep_components) {
    by_team %>%
      dplyr::mutate(
        net_epa_smooth = smooth_one(net_week),
        off_epa_smooth = smooth_one(off_week),
        def_epa_smooth = smooth_one(def_week),
        n_plays_eff = cumsum(replace_na(n_plays_eff_week, 0))
      ) %>%
      dplyr::ungroup()
  } else {
    by_team %>%
      dplyr::mutate(
        net_epa_smooth = smooth_one(net_week),
        n_plays_eff = cumsum(replace_na(n_plays_eff_week, 0))
      ) %>%
      dplyr::ungroup()
  }
}

#' @keywords internal
.ts_v01_schedule_adjust <- function(tw_sm, tg, beta = 0.7, keep_components = FALSE) {
  # (season, week, team, opponent) for each INCLUDED team-game
  sched <- tg %>%
    dplyr::select(season, week, team, opponent) %>%
    dplyr::arrange(season, team, week)
  
  # Opponent's smoothed NET, lagged by one week relative to the game week
  opp_sm <- tw_sm %>%
    dplyr::select(season, week, team = team,
                  opp_net_smooth = net_epa_smooth) %>%
    dplyr::mutate(week = week + 1L)
  
  sched <- sched %>%
    dplyr::left_join(opp_sm, by = c("season","week","opponent" = "team")) %>%
    # Week-1 neutral prior: if lagged opp strength is missing, use 0 (EPA/play)
    dplyr::mutate(opp_net_smooth_lag = dplyr::coalesce(opp_net_smooth, 0))
  
  # Cumulative mean across *played* games; counts week 1 as a 0-prior observation
  sos_by_game <- sched %>%
    dplyr::group_by(season, team) %>%
    dplyr::arrange(week, .by_group = TRUE) %>%
    dplyr::mutate(
      cum_sum = cumsum(opp_net_smooth_lag),
      cum_n   = cumsum(1L),
      sos_game_cum = cum_sum / cum_n
    ) %>%
    dplyr::select(season, team, week, sos_game_cum) %>%
    dplyr::ungroup()
  
  # Join back to full weekly table and carry forward over byes
  tw <- tw_sm %>%
    dplyr::left_join(sos_by_game, by = c("season","team","week")) %>%
    dplyr::group_by(season, team) %>%
    dplyr::arrange(week, .by_group = TRUE) %>%
    dplyr::mutate(
      sos = {
        s <- sos_game_cum
        for (i in seq_along(s)) if (i > 1 && !is.finite(s[i])) s[i] <- s[i-1]
        # If the very first rated week were somehow NA, make it 0 (paranoia guard)
        if (!is.finite(s[1])) s[1] <- 0
        s
      },
      adj_net = net_epa_smooth - beta * sos
    )
  
  if (keep_components) {
    tw <- tw %>%
      dplyr::mutate(
        adj_off = off_epa_smooth - beta * sos,
        adj_def = def_epa_smooth - beta * sos
      )
  }
  
  dplyr::ungroup(tw)
}

#' @keywords internal
.ts_v01_scale_weekly <- function(tw_adj, keep_components = FALSE) {
  scale_fun <- function(x) {
    m <- mean(x, na.rm = TRUE)
    s <- stats::sd(x, na.rm = TRUE)
    if (!is.finite(s) || s == 0) return(rep(0, length(x)))
    (x - m) / s
  }
  
  out <- tw_adj %>%
    dplyr::group_by(season, week) %>%
    dplyr::mutate(
      rating_net = scale_fun(adj_net)
    ) %>%
    dplyr::ungroup()
  
  if (keep_components) {
    out <- out %>%
      dplyr::group_by(season, week) %>%
      dplyr::mutate(
        rating_off = scale_fun(adj_off),
        rating_def = scale_fun(adj_def)
      ) %>%
      dplyr::ungroup()
  }
  
  cols_keep <- c("season","week","team",
                 "rating_net","net_epa_smooth","sos","n_plays_eff")
  if (keep_components) cols_keep <- c(cols_keep, "rating_off","rating_def")
  
  out %>%
    dplyr::select(dplyr::all_of(cols_keep))
}

#' @keywords internal
.require_pkgs <- function(pkgs) {
  missing <- pkgs[!vapply(pkgs, requireNamespace, FUN.VALUE = logical(1), quietly = TRUE)]
  if (length(missing)) {
    rlang::abort(paste0("Missing required packages: ", paste(missing, collapse = ", ")))
  }
}

# small helper to avoid R CMD check NOTE for replace_na
replace_na <- function(x, value) {
  x[is.na(x)] <- value
  x
}

