#' Weekly pre-game team ranks (REG+POST, ignore season_type) + opponent ranks
#' (now auto-derives def_*_allowed from opponent offense if missing)
#' @export
rank_team_stats_weekly <- function(
    df,
    ranking_stats,
    mean_stats = c(
      "passing_epa","early_epa_per_play","rushing_epa",
      "avg_drive_depth_into_opp","avg_start_yardline_100","fg_pct",
      "def_pass_epa_allowed","def_rushing_epa_allowed","def_avg_drive_depth_allow"
    ),
    invert_stats = c(
      "interceptions","sacks","rushing_fumbles",
      "def_penalty",
      "def_passing_yards_allowed","def_passing_tds_allowed","def_passing_first_downs_allowed","def_pass_epa_allowed",
      "def_rushing_yards_allowed","def_rushing_tds_allowed","def_rushing_first_downs_allowed","def_rushing_epa_allowed",
      "def_avg_drive_depth_allow","points_allowed"
    ),
    keep_agg_value = TRUE,
    stat_type_keep = "base"
) {
  needed <- c("team","season","week","opponent","stat_type","stat_name","value")
  miss <- setdiff(needed, names(df))
  if (length(miss) > 0) stop(sprintf("Missing required columns: %s", paste(miss, collapse = ", ")))
  
  # ---------------------------
  # NEW: ensure def_*_allowed exist
  # ---------------------------
  # offense -> defense-allowed map (only the ones we rank)
  off_to_allowed <- c(
    "passing_yards"            = "def_passing_yards_allowed",
    "passing_tds"              = "def_passing_tds_allowed",
    "passing_first_downs"      = "def_passing_first_downs_allowed",
    "passing_epa"              = "def_pass_epa_allowed",
    "avg_drive_depth_into_opp" = "def_avg_drive_depth_allow",
    "drives"                   = "def_drives_allowed",
    "carries"                  = "def_carries_allowed",
    "rushing_yards"            = "def_rushing_yards_allowed",
    "rushing_tds"              = "def_rushing_tds_allowed",
    "rushing_first_downs"      = "def_rushing_first_downs_allowed",
    "rushing_epa"              = "def_rushing_epa_allowed"
  )
  
  # Which allowed stats do we actually need?
  needed_allowed <- intersect(ranking_stats, unname(off_to_allowed))
  have_allowed   <- intersect(needed_allowed, unique(df$stat_name))
  missing_allowed <- setdiff(needed_allowed, have_allowed)
  
  if (length(missing_allowed) > 0) {
    # Back-map missing allowed -> corresponding offense names
    inv_map <- setNames(names(off_to_allowed), off_to_allowed)
    needed_off_for_missing <- unique(inv_map[missing_allowed])
    
    # Build mirrored rows using opponent (ignore season_type entirely)
    src <- df |>
      dplyr::filter(
        stat_type == stat_type_keep,
        stat_name %in% needed_off_for_missing,
        !is.na(opponent), opponent != "", team != opponent
      )
    
    def_allowed <- src |>
      dplyr::transmute(
        team        = opponent,
        season      = season,
        season_type = dplyr::coalesce(.data$season_type, "REG"),  # kept but ignored later
        week        = week,
        opponent    = team,
        stat_type   = stat_type,
        stat_name   = unname(off_to_allowed[stat_name]),
        value       = value,
        game_id     = dplyr::coalesce(.data$game_id, paste(season, sprintf("%02d", week), opponent, team, sep = "_"))
      )
    
    # De-dup against any existing rows (paranoid-safe)
    key_cols <- c("season","week","team","stat_type","stat_name","game_id")
    existing_keys <- df |> dplyr::select(dplyr::any_of(key_cols)) |> dplyr::distinct()
    def_allowed_new <- def_allowed |> dplyr::anti_join(existing_keys, by = key_cols)
    
    # Append
    df <- dplyr::bind_rows(df, def_allowed_new)
  }
  
  # ---- Scope & target stats ----
  d0 <- df |>
    dplyr::filter(stat_type == stat_type_keep, stat_name %in% ranking_stats) |>
    dplyr::select(team, season, week, opponent, stat_name, value)
  
  mean_stats <- intersect(mean_stats, ranking_stats)
  sum_stats  <- setdiff(ranking_stats, mean_stats)
  
  # ---- Collapse duplicates ----
  d_mean <- d0 |>
    dplyr::filter(stat_name %in% mean_stats) |>
    dplyr::group_by(team, season, week, stat_name) |>
    dplyr::summarise(value = mean(value, na.rm = TRUE), .groups = "drop")
  
  d_sum <- d0 |>
    dplyr::filter(stat_name %in% sum_stats) |>
    tidyr::replace_na(list(value = 0)) |>
    dplyr::group_by(team, season, week, stat_name) |>
    dplyr::summarise(value = sum(value, na.rm = TRUE), .groups = "drop")
  
  d <- dplyr::bind_rows(d_mean, d_sum) |>
    dplyr::arrange(season, stat_name, team, week)
  
  # ---- Accumulate through previous week ----
  do_means <- function(x) {
    x |>
      dplyr::group_by(season, team, stat_name) |>
      dplyr::mutate(
        cum_sum_prev = dplyr::lag(cumsum(tidyr::replace_na(value, 0)), default = 0),
        cum_n_prev   = dplyr::lag(cumsum(ifelse(is.na(value), 0L, 1L)), default = 0L),
        agg_value_prev = dplyr::if_else(cum_n_prev > 0, cum_sum_prev / cum_n_prev, 0)
      ) |>
      dplyr::ungroup()
  }
  do_sums <- function(x) {
    x |>
      dplyr::group_by(season, team, stat_name) |>
      dplyr::mutate(
        agg_value_prev = cumsum(dplyr::lag(tidyr::replace_na(value, 0), default = 0))
      ) |>
      dplyr::ungroup()
  }
  
  d_means_acc <- d |> dplyr::filter(stat_name %in% mean_stats) |> do_means()
  d_sums_acc  <- d |> dplyr::filter(stat_name %in% sum_stats)  |> do_sums()
  
  d_acc <- dplyr::bind_rows(d_means_acc, d_sums_acc) |>
    dplyr::select(season, week, team, stat_name, agg_value_prev) |>
    dplyr::group_by(season, week, team, stat_name) |>
    dplyr::summarise(agg_value_prev = dplyr::first(agg_value_prev), .groups = "drop")
  
  # ---- Full grid per season (no m2m warnings) ----
  season_max <- df |>
    dplyr::group_by(season) |>
    dplyr::summarise(max_week = max(week, na.rm = TRUE), .groups = "drop")
  
  teams_by_season <- df |>
    dplyr::distinct(season, team)
  
  build_grid_one <- function(s, max_w, teams, stats) {
    weeks <- seq_len(max_w)
    expand.grid(season = s, week = weeks, team = teams, stat_name = stats, stringsAsFactors = FALSE)
  }
  
  grids <- lapply(seq_len(nrow(season_max)), function(i) {
    s  <- season_max$season[i]
    mw <- season_max$max_week[i]
    tms <- teams_by_season$team[teams_by_season$season == s]
    build_grid_one(s, mw, tms, unique(ranking_stats))
  })
  
  full_grid <- dplyr::bind_rows(grids) |> dplyr::arrange(season, stat_name, team, week)
  
  # ---- Attach agg_value_prev & LOCF ----
  d_full <- full_grid |>
    dplyr::left_join(d_acc, by = c("season","week","team","stat_name")) |>
    dplyr::group_by(season, team, stat_name) |>
    tidyr::fill(agg_value_prev, .direction = "down") |>
    dplyr::mutate(agg_value_prev = dplyr::if_else(week == 1 & is.na(agg_value_prev), 0, agg_value_prev)) |>
    dplyr::ungroup()
  
  # ---- Rank ----
  ranks <- d_full |>
    dplyr::group_by(season, week, stat_name) |>
    dplyr::mutate(
      rank_desc = dplyr::min_rank(dplyr::desc(agg_value_prev)),
      rank_asc  = dplyr::min_rank(agg_value_prev),
      rank      = ifelse(stat_name %in% invert_stats, rank_asc, rank_desc),
      n_teams   = dplyr::n()
    ) |>
    dplyr::ungroup() |>
    dplyr::select(season, week, team, stat_name, rank, n_teams,
                  dplyr::any_of(if (keep_agg_value) "agg_value_prev" else character(0)))
  
  # ---- Opponent & opp_rank ----
  sched <- df |>
    dplyr::select(season, week, team, opponent) |>
    dplyr::distinct() |>
    dplyr::group_by(season, week, team) |>
    dplyr::summarise(opponent = dplyr::first(opponent), .groups = "drop") |>
    dplyr::filter(!is.na(opponent), opponent != "", opponent != team)
  
  ranks_w_opp <- ranks |>
    dplyr::left_join(sched, by = c("season","week","team")) |>
    dplyr::left_join(
      ranks |>
        dplyr::select(season, week, stat_name, team, rank) |>
        dplyr::rename(opponent = team, opp_rank = rank),
      by = c("season","week","stat_name","opponent")
    ) |>
    dplyr::select(season, week, team, opponent, stat_name, rank, opp_rank, n_teams,
                  dplyr::any_of(if (keep_agg_value) "agg_value_prev" else character(0)))
  
  ranks_w_opp
}
