#' Build pregame, game-level modelling dataset (2022–2024) with as-of features
#'
#' @description
#' Creates a **one-row-per-game** matrix for pre-game win modelling. Features include:
#' - team strength (as-of): `rating_net`, `sos`, reliability (`n_plays_eff`) for home/away + `diff_*`
#' - injuries (T−30m): position-weighted indices for OL/WRTE/RB/DL/LB/DB, home/away + diffs
#' - weather/context: `temp`, `wind`, `weather_text`, `roof`, `surface`, `dome_flag`, wind/temp buckets, `precip_flag`
#' - target: `home_win` (historical only, if scores available)
#' - minimal Week-1 **QB priors** (if tables present), with `used_prior_*` flags
#'
#' All season-to-date team features are computed **as of kickoff** via week fences
#' (REG: use games through week N−1; POST: uses all REG weeks).
#'
#' @param con DBI connection to Postgres.
#' @param seasons integer vector of seasons to include. Default: 2022:2024.
#' @param schema character schema name. Default: "prod".
#' @param games_table character table name for schedules/games. Default: "games_tbl".
#'   Expected columns (any subset): `game_id`, `season`, `week`, `game_type` or `season_type`,
#'   `home_team`, `away_team`, `start_time_utc` (or `kickoff_utc` or `gameday`+`gametime`),
#'   `stadium_id`, `roof`, `surface`, `temp`, `wind`, `weather`, `spread_line`,
#'   and optionally `home_score`, `away_score`.
#' @param team_strength_table character table with weekly team ratings. Default: "team_strength_weekly_tbl".
#'   Expected: `season`, `week`, `team`, `rating_net`, `sos`, `n_plays_eff`.
#' @param injuries_table character table with T−30m injuries. Default: "injuries_tbl".
#'   Expected minimally: `game_id`, `team`, `position_group`, `status` and optionally `snapshot_at`.
#' @param injury_weights named numeric of status weights. Default: c(Out=1.0, Doubtful=0.7, Questionable=0.3).
#' @param depth_charts_qb_table optional table for starters. Default: "depth_charts_qb_team_tbl".
#'   Expected: `season`, `week`, `team`, `player_id`, `is_starter` (or similar boolean/flag).
#' @param career_stats_qb_table optional table for QB career stats. Default: "career_stats_qb_tbl".
#'   Expected (any subset): `player_id`, `attempts`, `passing_yards`, `passing_tds`, `interceptions`,
#'   `sacks`, `sack_yards`. Used to compute ANY/A prior.
#' @return A tibble with one row per game and engineered features suitable for LR / RF / XGB.
#' @export
#'
#' @examples
#' \dontrun{
#' ds <- build_pregame_dataset(
#'   con = con,
#'   seasons = 2022:2024,
#'   schema = "prod"
#' )
#' }
build_pregame_dataset <- function(
    con,
    seasons = 2022:2024,
    schema  = "prod",
    games_table = "games_tbl",
    team_strength_table = "team_strength_weekly_tbl",
    injuries_table = "injuries_tbl",
    injury_weights = c(Out = 1.0, Doubtful = 0.7, Questionable = 0.3),
    depth_charts_qb_table = "depth_charts_qb_team_tbl",
    career_stats_qb_table = "career_stats_qb_tbl"
) {
  # deps
  requireNamespace("dplyr", quietly = TRUE)
  requireNamespace("dbplyr", quietly = TRUE)
  requireNamespace("tidyr", quietly = TRUE)
  requireNamespace("stringr", quietly = TRUE)
  requireNamespace("lubridate", quietly = TRUE)
  library(dplyr); library(dbplyr); library(tidyr); library(stringr); library(lubridate)
  
  in_sch <- function(tbl) dbplyr::in_schema(schema, tbl)
  tbl_exists <- function(tbl) DBI::dbExistsTable(con, DBI::Id(schema = schema, table = tbl))
  
  # ---- TABLE NAMES ----
  pbp_off_tbl_name   <- "participation_offense_pbp_tbl"
  pbp_def_tbl_name   <- "participation_defense_pbp_tbl"
  snap_week_tbl_name <- "snapcount_weekly_tbl"
  contracts_qb_tbl_name <- "contracts_qb_tbl"
  starters_tbl_name  <- "depth_charts_starters_tbl"
  pos_stability_tbl_name <- "depth_charts_position_stability_tbl"
  off_team_stats_week_tbl_name <- "off_team_stats_week_tbl"
  def_team_stats_week_tbl_name <- "def_team_stats_week_tbl"
  
  # Utility: safe table opener
  safe_tbl <- function(tbl_nm) {
    if (DBI::dbExistsTable(con, DBI::Id(schema = schema, table = tbl_nm))) {
      tbl(con, dbplyr::in_schema(schema, tbl_nm))
    } else {
      NULL
    }
  }
  
  # ---- helpers ----
  impute_weather <- function(df) {
    df0 <- df %>%
      dplyr::mutate(
        stid   = dplyr::coalesce(as.character(.data$stadium_id), as.character(.data$stadium)),
        mo     = lubridate::month(.data$kickoff),
        is_dome = tolower(dplyr::coalesce(.data$roof, "")) %in% c("dome","closed"),
        temp_num = suppressWarnings(as.numeric(.data$temp)),
        wind_num = suppressWarnings(as.numeric(.data$wind))
      )
    
    ref <- df0 %>%
      dplyr::group_by(.data$stid, .data$roof, .data$mo) %>%
      dplyr::summarise(
        temp_med = stats::median(.data$temp_num, na.rm = TRUE),
        wind_med = stats::median(.data$wind_num, na.rm = TRUE),
        .groups = "drop"
      )
    
    df1 <- df0 %>%
      dplyr::left_join(ref, by = c("stid","roof","mo")) %>%
      dplyr::mutate(
        temp_miss = as.integer(is.na(.data$temp_num)),
        wind_miss = as.integer(is.na(.data$wind_num)),
        temp_num  = dplyr::case_when(
          .data$is_dome ~ 70,
          is.na(.data$temp_num) ~ temp_med,
          TRUE ~ .data$temp_num
        ),
        wind_num  = dplyr::case_when(
          .data$is_dome ~ 0,
          is.na(.data$wind_num) ~ wind_med,
          TRUE ~ .data$wind_num
        )
      ) %>%
      dplyr::mutate(
        temp = temp_num,
        wind = wind_num
      ) %>%
      dplyr::select(-stid,-mo,-temp_num,-wind_num,-temp_med,-wind_med)
    
    df1 %>%
      dplyr::mutate(
        wind_bucket = cut(as.numeric(.data$wind),
                          breaks = c(-Inf,5,10,15,20,Inf),
                          labels = c("0-5","6-10","11-15","16-20","21+")),
        temp_bucket = cut(as.numeric(.data$temp),
                          breaks = c(-Inf,32,49,79,Inf),
                          labels = c("≤32","33-49","50-79","80+"))
      )
  }
  
  pick_first_col <- function(df, candidates) {
    cols <- intersect(candidates, names(df))
    if (!length(cols)) return(rep(NA_character_, nrow(df)))
    out <- df[[cols[1]]]
    if (length(cols) > 1) for (nm in cols[-1]) out <- dplyr::coalesce(out, df[[nm]])
    out
  }
  
  parse_kickoff_est <- function(x) {
    if (inherits(x, "POSIXt")) return(x)
    suppressWarnings(
      lubridate::parse_date_time(
        x,
        orders = c("Y-m-d H:M:S","Y-m-d H:M","m/d/Y H:M","Ymd HMS","Ymd HM"),
        tz = "America/New_York"
      )
    )
  }
  
  # --- S1. Base games/schedules ---
  games_raw <- tbl(con, dbplyr::in_schema(schema, games_table)) %>%
    dplyr::filter(.data$season %in% !!seasons) %>%
    dplyr::select(dplyr::any_of(c(
      "game_id","season","week","game_type","season_type",
      "home_team","away_team",
      "kickoff",
      "stadium","stadium_id","roof","surface","temp","wind",
      "home_score","away_score",
      "spread_line"
    ))) %>%
    dplyr::collect()
  
  if (!"season_type" %in% names(games_raw) && "game_type" %in% names(games_raw)) {
    games_raw <- games_raw %>%
      dplyr::mutate(season_type = ifelse(toupper(.data$game_type) == "REG", "REG", "POST"))
  }
  
  retractable_ids <- c("IND00","ATL97","PHO00","DAL00","HOU00")
  
  games_base <- games_raw %>%
    dplyr::mutate(
      season_type = if (!"season_type" %in% names(.)) ifelse(toupper(game_type)=="REG","REG","POST") else season_type,
      kickoff = if (inherits(kickoff, "POSIXt")) kickoff else
        suppressWarnings(lubridate::parse_date_time(
          kickoff,
          orders = c("Y-m-d H:M:S","Y-m-d H:M","m/d/Y H:M","Ymd HMS","Ymd HM"),
          tz = "America/New_York"
        )),
      roof_clean = tolower(dplyr::coalesce(roof, "")),
      roof_state = dplyr::case_when(
        roof_clean %in% c("dome","closed") ~ "indoors",
        roof_clean == "open" & stadium_id %in% retractable_ids ~ "retractable_open",
        roof_clean %in% c("open","outdoors") ~ "outdoors",
        TRUE ~ "unknown"
      ),
      dome_flag = roof_state == "indoors"
    )
  
  games_base <- games_base %>%
    dplyr::mutate(
      temp_num = suppressWarnings(as.numeric(gsub("[^0-9.-]","", as.character(temp)))),
      wind_num = suppressWarnings(as.numeric(gsub("[^0-9.-]","", as.character(wind)))),
      temp_miss = as.integer(is.na(temp_num)),
      wind_miss = as.integer(is.na(wind_num)),
      mo = lubridate::month(kickoff)
    )
  
  ref_stad_mo <- games_base %>%
    dplyr::group_by(stadium_id, roof_state, mo) %>%
    dplyr::summarise(
      temp_med = stats::median(temp_num, na.rm = TRUE),
      wind_med = stats::median(wind_num, na.rm = TRUE),
      .groups = "drop"
    )
  ref_roof_mo <- games_base %>%
    dplyr::group_by(roof_state, mo) %>%
    dplyr::summarise(
      temp_med = stats::median(temp_num, na.rm = TRUE),
      wind_med = stats::median(wind_num, na.rm = TRUE),
      .groups = "drop"
    )
  ref_mo <- games_base %>%
    dplyr::group_by(mo) %>%
    dplyr::summarise(
      temp_med = stats::median(temp_num, na.rm = TRUE),
      wind_med = stats::median(wind_num, na.rm = TRUE),
      .groups = "drop"
    )
  
  games_base <- games_base %>%
    dplyr::left_join(ref_stad_mo, by = c("stadium_id","roof_state","mo")) %>%
    dplyr::rename(temp_med_stad = temp_med, wind_med_stad = wind_med) %>%
    dplyr::left_join(ref_roof_mo, by = c("roof_state","mo")) %>%
    dplyr::rename(temp_med_roof = temp_med, wind_med_roof = wind_med) %>%
    dplyr::left_join(ref_mo, by = "mo") %>%
    dplyr::rename(temp_med_mo = temp_med, wind_med_mo = wind_med) %>%
    dplyr::mutate(
      temp = dplyr::case_when(
        roof_state == "indoors" ~ 70,
        !is.na(temp_num) ~ temp_num,
        !is.na(temp_med_stad) ~ temp_med_stad,
        !is.na(temp_med_roof) ~ temp_med_roof,
        !is.na(temp_med_mo) ~ temp_med_mo,
        TRUE ~ 70
      ),
      wind = dplyr::case_when(
        roof_state == "indoors" ~ 0,
        !is.na(wind_num) ~ wind_num,
        !is.na(wind_med_stad) ~ wind_med_stad,
        !is.na(wind_med_roof) ~ wind_med_roof,
        !is.na(wind_med_mo) ~ wind_med_mo,
        TRUE ~ 0
      )
    ) %>%
    dplyr::mutate(
      wind_bucket = cut(as.numeric(wind), breaks = c(-Inf,5,10,15,20,Inf),
                        labels = c("0-5","6-10","11-15","16-20","21+")),
      temp_bucket = cut(as.numeric(temp), breaks = c(-Inf,32,49,79,Inf),
                        labels = c("≤32","33-49","50-79","80+"))
    ) %>%
    dplyr::select(-temp_num, -wind_num, -temp_med_stad, -wind_med_stad,
                  -temp_med_roof, -wind_med_roof, -temp_med_mo, -wind_med_mo, -mo, -roof_clean)
  
  games_base <- games_base %>% impute_weather()
  
  # Compute REG-season last week (for POST fences)
  reg_last_week_by_season <- games_base %>%
    dplyr::filter(.data$season_type == "REG") %>%
    dplyr::group_by(.data$season) %>%
    dplyr::summarise(reg_last_week = max(.data$week, na.rm = TRUE), .groups = "drop")
  
  #---------------------------------------
  # S2. Team strength (as-of) home & away
  #---------------------------------------
  if (!tbl_exists(team_strength_table)) stop("Missing team_strength table: ", schema, ".", team_strength_table)
  
  ts_wk <- tbl(con, in_sch(team_strength_table)) %>%
    select(season, week, team, rating_net, net_epa_smooth) %>%
    collect() %>%
    mutate(week = as.integer(week)) %>%
    rename(ts_week = week)
  
  get_ts_asof <- function(games_df, side = c("home","away")) {
    side <- match.arg(side)
    key_team <- paste0(side, "_team")
    games_df %>%
      mutate(week = as.integer(week)) %>%
      left_join(
        ts_wk,
        by = dplyr::join_by(season == season, !!rlang::sym(key_team) == team),
        relationship = "many-to-many"
      ) %>%
      dplyr::filter(if_else(season_type == "REG", ts_week < week, TRUE)) %>%
      dplyr::group_by(game_id) %>%
      dplyr::slice_max(order_by = ts_week, n = 1, with_ties = FALSE) %>%
      dplyr::ungroup() %>%
      dplyr::transmute(
        game_id,
        !!paste0(side, "_rating_net")      := rating_net,
        !!paste0(side, "_net_epa_smooth")  := net_epa_smooth
      )
  }
  
  ts_home <- get_ts_asof(games_base, "home")
  ts_away <- get_ts_asof(games_base, "away")
  
  games_ts <- games_base %>%
    left_join(ts_home, by = "game_id") %>%
    left_join(ts_away, by = "game_id") %>%
    mutate(
      diff_rating_net      = home_rating_net - away_rating_net,
      diff_net_epa_smooth  = home_net_epa_smooth - away_net_epa_smooth
    )
  
  games_ts <- games_ts %>%
    dplyr::mutate(
      used_ts_prior_home = as.integer(is.na(.data$home_rating_net) | is.na(.data$home_net_epa_smooth)),
      used_ts_prior_away = as.integer(is.na(.data$away_rating_net) | is.na(.data$away_net_epa_smooth)),
      home_rating_net     = dplyr::coalesce(.data$home_rating_net, 0),
      away_rating_net     = dplyr::coalesce(.data$away_rating_net, 0),
      home_net_epa_smooth = dplyr::coalesce(.data$home_net_epa_smooth, 0),
      away_net_epa_smooth = dplyr::coalesce(.data$away_net_epa_smooth, 0),
      diff_rating_net     = .data$home_rating_net - .data$away_rating_net,
      diff_net_epa_smooth = .data$home_net_epa_smooth - .data$away_net_epa_smooth
    )
  
  #-----------------------------
  # S3. Injuries (weekly, by position) — counts (status-weighted optional)
  #-----------------------------
  add_injury_indices <- function(df) {
    if (!tbl_exists(injuries_table)) {
      message("Injuries table not found: ", injuries_table, " — skipping injury features.")
      return(df)
    }
    
    inj_raw <- tbl(con, in_sch(injuries_table)) %>%
      dplyr::filter(.data$season %in% !!seasons) %>%
      dplyr::select(dplyr::any_of(c(
        "season","week","team",
        "position","position_group","status",
        "position_injuries"   # optional legacy count column
      ))) %>%
      dplyr::collect() %>%
      dplyr::mutate(
        season   = as.integer(.data$season),
        week     = as.integer(.data$week),
        team     = as.character(.data$team)
      )
    
    # Ensure position_group exists even if the source table doesn't have it
    if (!"position_group" %in% names(inj_raw)) {
      inj_raw <- inj_raw %>% dplyr::mutate(position_group = NA_character_)
    }
    
    # Normalise to a single pos_group column
    inj_norm <- inj_raw %>%
      dplyr::mutate(
        position = toupper(dplyr::coalesce(.data$position, "")),
        pos_group = dplyr::coalesce(
          toupper(.data$position_group),
          dplyr::case_when(
            position %in% c("T","G","C","OT","OG","OC") ~ "OL",
            position %in% c("WR")                       ~ "WR",
            position %in% c("TE")                       ~ "TE",
            position %in% c("RB","FB")                  ~ "RB",
            position %in% c("DE","DT","NT","DL")        ~ "DL",
            position %in% c("LB","ILB","OLB")           ~ "LB",
            position %in% c("CB","S","FS","SS","DB")    ~ "DB",
            TRUE ~ NA_character_
          )
        )
      ) %>%
      dplyr::filter(!is.na(.data$pos_group))
    
    has_status <- "status" %in% names(inj_norm)
    
    if (has_status) {
      # Map statuses → weights using the injury_weights vector; unknown statuses → 0
      status_key <- tibble::tibble(
        status = names(injury_weights),
        w = as.numeric(injury_weights)
      )
      inj_w <- inj_norm %>%
        dplyr::mutate(status = as.character(.data$status)) %>%
        dplyr::left_join(status_key, by = "status") %>%
        dplyr::mutate(w = dplyr::coalesce(.data$w, 0)) %>%
        dplyr::group_by(.data$season, .data$week, .data$team, .data$pos_group) %>%
        dplyr::summarise(inj_val = sum(.data$w, na.rm = TRUE), .groups = "drop")
    } else {
      # Fallback: use provided counts
      inj_w <- inj_norm %>%
        dplyr::mutate(position_injuries = as.numeric(.data$position_injuries)) %>%
        dplyr::group_by(.data$season, .data$week, .data$team, .data$pos_group) %>%
        dplyr::summarise(inj_val = sum(.data$position_injuries, na.rm = TRUE), .groups = "drop")
    }
    
    ipw <- inj_w %>%
      tidyr::pivot_wider(
        id_cols    = c(.data$season, .data$week, .data$team),
        names_from = .data$pos_group,
        values_from = .data$inj_val,
        names_glue = "inj_{pos_group}_count",  # keep original column names
        values_fill = 0
      )
    
    out <- df %>%
      dplyr::mutate(week = as.integer(.data$week)) %>%
      dplyr::left_join(ipw, by = dplyr::join_by(season == season, week == week, home_team == team)) %>%
      dplyr::rename_with(~ paste0("home_", .x), dplyr::starts_with("inj_")) %>%
      dplyr::left_join(ipw, by = dplyr::join_by(season == season, week == week, away_team == team)) %>%
      dplyr::rename_with(~ paste0("away_", .x), dplyr::starts_with("inj_")) %>%
      dplyr::mutate(
        dplyr::across(dplyr::starts_with("home_inj_"), ~ dplyr::coalesce(.x, 0)),
        dplyr::across(dplyr::starts_with("away_inj_"), ~ dplyr::coalesce(.x, 0))
      )
    
    for (g in c("OL","WR","TE","RB","DL","LB","DB")) {
      h <- paste0("home_inj_", g, "_count")
      a <- paste0("away_inj_", g, "_count")
      d <- paste0("diff_inj_", g, "_count")
      if (h %in% names(out) && a %in% names(out)) out[[d]] <- out[[h]] - out[[a]]
    }
    
    out
  }
  
  games_ts_inj <- add_injury_indices(games_ts)
  
  #--------------------------------------
  # S3b. Snapcounts (season-to-date)
  #--------------------------------------
  add_snapcounts_s2d <- function(df) {
    snwk_tbl <- safe_tbl(snap_week_tbl_name)
    if (is.null(snwk_tbl)) {
      message("snapcount_weekly_tbl not found; skipping snapcount features.")
      return(df)
    }
    
    snwk <- snwk_tbl %>%
      dplyr::filter(.data$season %in% !!seasons) %>%
      dplyr::select(dplyr::any_of(c("season","week","team","offense_snaps","defense_snaps"))) %>%
      dplyr::mutate(
        week          = as.integer(.data$week),
        offense_snaps = as.numeric(.data$offense_snaps),
        defense_snaps = as.numeric(.data$defense_snaps)
      ) %>%
      dplyr::group_by(.data$season, .data$week, .data$team) %>%
      dplyr::summarise(
        team_off_snaps = max(.data$offense_snaps, na.rm = TRUE),
        team_def_snaps = max(.data$defense_snaps, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::collect()
    
    calc_side <- function(gdf, side = c("home","away")) {
      side <- match.arg(side)
      key_team <- rlang::sym(paste0(side, "_team"))
      
      gdf %>%
        dplyr::left_join(
          snwk %>% dplyr::rename(snap_week = week),
          by = dplyr::join_by(season == season, !!key_team == team),
          relationship = "many-to-many"
        ) %>%
        dplyr::group_by(.data$game_id, .data$season, .data$week) %>%
        dplyr::summarise(
          !!paste0(side, "_off_snaps_pg") := {
            idx <- snap_week < first(.data$week)
            if (sum(idx, na.rm = TRUE) == 0) 0 else mean(team_off_snaps[idx], na.rm = TRUE)
          },
          !!paste0(side, "_def_snaps_pg") := {
            idx <- snap_week < first(.data$week)
            if (sum(idx, na.rm = TRUE) == 0) 0 else mean(team_def_snaps[idx], na.rm = TRUE)
          },
          .groups = "drop"
        )
    }
    
    home <- calc_side(df, "home")
    away <- calc_side(df, "away")
    
    df %>%
      dplyr::left_join(home, by = c("game_id","season","week")) %>%
      dplyr::left_join(away, by = c("game_id","season","week")) %>%
      dplyr::mutate(
        dplyr::across(dplyr::all_of(c(
          "home_off_snaps_pg","home_def_snaps_pg",
          "away_off_snaps_pg","away_def_snaps_pg"
        )), ~ dplyr::coalesce(.x, 0)),
        diff_off_snaps_pg = .data$home_off_snaps_pg - .data$away_off_snaps_pg,
        diff_def_snaps_pg = .data$home_def_snaps_pg - .data$away_def_snaps_pg
      )
  }
  
  #--------------------------------------
  # S3c. QB APY % of Team Cap (contracts_qb_tbl)
  #--------------------------------------
  add_qb_cap_pct <- function(df) {
    starters <- safe_tbl(starters_tbl_name)
    cq       <- safe_tbl(contracts_qb_tbl_name)
    
    if (is.null(starters) || is.null(cq)) {
      message("Starters or contracts_qb table missing; skipping QB cap% features.")
      return(df)
    }
    
    qb_starters <- starters %>%
      dplyr::filter(season %in% !!seasons) %>%
      dplyr::select(season, week, team, position, gsis_id) %>%
      dplyr::collect() %>%
      dplyr::mutate(
        season   = as.integer(season),
        week     = as.integer(week),
        position = toupper(position),
        gsis_id  = as.character(gsis_id)
      ) %>%
      dplyr::filter(position == "QB") %>%
      dplyr::distinct(season, week, team, gsis_id)
    
    qb_contracts_raw <- cq %>%
      dplyr::select(
        dplyr::any_of(c("gsis_id")),
        dplyr::any_of(c("year_signed")),
        dplyr::any_of(c("apy_cap_pct"))
      ) %>%
      dplyr::collect() %>%
      dplyr::filter(!is.na(gsis_id))
    
    season_meds <- qb_contracts_raw %>%
      dplyr::filter(!is.na(year_signed), !is.na(apy_cap_pct)) %>%
      dplyr::group_by(year_signed) %>%
      dplyr::summarise(season_median_apy = stats::median(apy_cap_pct), .groups = "drop")
    
    overall_med <- stats::median(qb_contracts_raw$apy_cap_pct, na.rm = TRUE)
    
    attach_side <- function(gdf, side = c("home","away")) {
      side <- match.arg(side)
      key_team <- rlang::sym(paste0(side, "_team"))
      
      gdf %>%
        dplyr::left_join(
          qb_starters,
          by = dplyr::join_by(season == season, week == week, !!key_team == team),
          relationship = "many-to-many"
        ) %>%
        dplyr::mutate(gsis_id = as.character(gsis_id)) %>%
        dplyr::left_join(
          qb_contracts_raw %>% dplyr::select(gsis_id, apy_cap_pct),
          by = "gsis_id",
          relationship = "many-to-many"
        ) %>%
        dplyr::left_join(season_meds, by = c(season = "year_signed")) %>%
        dplyr::mutate(
          apy_cap_pct_filled = dplyr::if_else(
            is.na(apy_cap_pct),
            dplyr::coalesce(season_median_apy, overall_med),
            apy_cap_pct
          )
        ) %>%
        dplyr::group_by(game_id) %>%
        dplyr::summarise(
          !!paste0(side, "_qb_apy_pct_cap") := {
            x <- suppressWarnings(max(apy_cap_pct_filled, na.rm = TRUE))
            if (!is.finite(x)) NA_real_ else x
          },
          .groups = "drop"
        )
    }
    
    home <- attach_side(df, "home")
    away <- attach_side(df, "away")
    
    df %>%
      dplyr::left_join(home, by = "game_id") %>%
      dplyr::left_join(away, by = "game_id") %>%
      dplyr::mutate(diff_qb_apy_pct_cap = home_qb_apy_pct_cap - away_qb_apy_pct_cap)
  }
  
  #--------------------------------------
  # S3d. Position-group stability (as-of)
  #--------------------------------------
  add_position_stability <- function(df) {
    ps <- safe_tbl("depth_charts_position_stability_tbl")
    if (is.null(ps)) {
      message("Position stability table not found; skipping stability features.")
      return(df)
    }
    
    stab <- ps %>%
      dplyr::select(season, week, team, position, position_group_score) %>%
      dplyr::filter(season %in% !!seasons) %>%
      dplyr::collect() %>%
      dplyr::mutate(
        week = as.integer(week),
        position = toupper(position),
        pos_group = dplyr::case_when(
          position %in% c("T","G","C","OT","OG","OC") ~ "OL",
          position %in% c("WR")                       ~ "WR",
          position %in% c("TE")                       ~ "TE",
          position %in% c("RB","FB")                  ~ "RB",
          position %in% c("CB","S","FS","SS","DB")    ~ "DB",
          position %in% c("DE","DT","NT","DL")        ~ "DL",
          position %in% c("LB","ILB","OLB")           ~ "LB",
          TRUE ~ NA_character_
        )
      ) %>%
      dplyr::filter(!is.na(pos_group)) %>%
      dplyr::group_by(season, week, team, pos_group) %>%
      dplyr::summarise(position_group_score = mean(position_group_score, na.rm = TRUE), .groups = "drop")
    
    attach_side <- function(gdf, side = c("home","away")) {
      side <- match.arg(side)
      key_team <- rlang::sym(paste0(side, "_team"))
      
      joined <- gdf %>%
        dplyr::left_join(
          stab %>% dplyr::rename(stab_week = week),
          by = dplyr::join_by(season == season, !!key_team == team),
          relationship = "many-to-many"
        )
      
      picked <- joined %>%
        dplyr::filter(stab_week < week) %>%
        dplyr::group_by(game_id, pos_group) %>%
        dplyr::slice_max(order_by = stab_week, n = 1, with_ties = FALSE) %>%
        dplyr::ungroup() %>%
        dplyr::select(game_id, pos_group, position_group_score) %>%
        tidyr::pivot_wider(
          id_cols = game_id,
          names_from = pos_group,
          values_from = position_group_score,
          names_prefix = paste0(tolower(side), "_stab_")
        )
      
      picked
    }
    
    home <- attach_side(df, "home")
    away <- attach_side(df, "away")
    
    out <- df %>%
      dplyr::left_join(home, by = "game_id") %>%
      dplyr::left_join(away, by = "game_id")
    
    out <- out %>%
      dplyr::mutate(
        across(dplyr::starts_with("home_stab_"), ~ dplyr::coalesce(.x, 0)),
        across(dplyr::starts_with("away_stab_"), ~ dplyr::coalesce(.x, 0))
      ) %>%
      dplyr::mutate(
        used_stab_prior_home = as.integer(rowSums(dplyr::across(dplyr::starts_with("home_stab_"), ~ .x == 0)) > 0 & .data$week == 1),
        used_stab_prior_away = as.integer(rowSums(dplyr::across(dplyr::starts_with("away_stab_"), ~ .x == 0)) > 0 & .data$week == 1)
      )
    
    for (g in c("OL","WR","TE","RB","DB","DL","LB")) {
      h <- paste0("home_stab_", g)
      a <- paste0("away_stab_", g)
      d <- paste0("diff_stab_", g)
      if (h %in% names(out) && a %in% names(out)) out[[d]] <- out[[h]] - out[[a]]
    }
    
    out
  }
  
  #-----------------------------------------
  # S3e. Team S2D basics (win%, PPG for/against, point diff TOTAL, TO diff TOTAL)
  #-----------------------------------------
  add_team_basics_s2d <- function(df) {
    g0 <- df %>%
      dplyr::select(dplyr::any_of(c(
        "season","week","season_type","game_id","kickoff",
        "home_team","away_team","home_score","away_score"
      )))
    
    tg <- dplyr::bind_rows(
      g0 %>%
        dplyr::transmute(
          season, tg_week = as.integer(week), tg_season_type = season_type, kickoff,
          team = home_team, opp_team = away_team,
          pts_for = as.numeric(home_score), pts_against = as.numeric(away_score)
        ),
      g0 %>%
        dplyr::transmute(
          season, tg_week = as.integer(week), tg_season_type = season_type, kickoff,
          team = away_team, opp_team = home_team,
          pts_for = as.numeric(away_score), pts_against = as.numeric(home_score)
        )
    ) %>%
      dplyr::mutate(win = as.integer(pts_for > pts_against))
    
    attach_side <- function(gdf, side = c("home","away")) {
      side <- match.arg(side)
      key_team <- rlang::sym(paste0(side, "_team"))
      
      gdf %>%
        dplyr::left_join(
          tg,
          by = dplyr::join_by(season == season, !!key_team == team),
          relationship = "many-to-many"
        ) %>%
        dplyr::left_join(reg_last_week_by_season, by = "season") %>%
        dplyr::group_by(game_id, season, season_type, week, reg_last_week) %>%
        dplyr::summarise(
          !!paste0(side, "_games_played_prior") := {
            idx <- if (first(season_type) == "REG") tg_week < first(week) else (tg_week <= first(reg_last_week))
            sum(idx, na.rm = TRUE)
          },
          !!paste0(side, "_win_pct_prior") := {
            idx <- if (first(season_type) == "REG") tg_week < first(week) else (tg_week <= first(reg_last_week))
            n <- sum(idx, na.rm = TRUE); if (n == 0) 0 else sum(win[idx], na.rm = TRUE) / n
          },
          !!paste0(side, "_ppg_for_prior") := {
            idx <- if (first(season_type) == "REG") tg_week < first(week) else (tg_week <= first(reg_last_week))
            n <- sum(idx, na.rm = TRUE); if (n == 0) 0 else sum(pts_for[idx], na.rm = TRUE) / n
          },
          !!paste0(side, "_ppg_against_prior") := {
            idx <- if (first(season_type) == "REG") tg_week < first(week) else (tg_week <= first(reg_last_week))
            n <- sum(idx, na.rm = TRUE); if (n == 0) 0 else sum(pts_against[idx], na.rm = TRUE) / n
          },
          # TOTALS to date (prior to game):
          !!paste0(side, "_pt_diff_pg_prior") := {
            idx <- if (first(season_type) == "REG") tg_week < first(week) else (tg_week <= first(reg_last_week))
            sum(pts_for[idx], na.rm = TRUE) - sum(pts_against[idx], na.rm = TRUE)
          },
          .groups = "drop"
        )
    }
    
    home <- attach_side(df, "home")
    away <- attach_side(df, "away")
    
    df %>%
      dplyr::left_join(home, by = c("game_id","season","season_type","week")) %>%
      dplyr::left_join(away, by = c("game_id","season","season_type","week")) %>%
      dplyr::mutate(
        diff_win_pct_prior     = .data$home_win_pct_prior      - .data$away_win_pct_prior,
        diff_ppg_for_prior     = .data$home_ppg_for_prior      - .data$away_ppg_for_prior,
        diff_ppg_against_prior = .data$home_ppg_against_prior  - .data$away_ppg_against_prior,
        diff_pt_diff_pg_prior  = .data$home_pt_diff_pg_prior   - .data$away_pt_diff_pg_prior
      )
  }
  
  #-----------------------------------------
  # NEW: S3f. Defense & Offense S2D aggregates (prior to game)
  #-----------------------------------------
  add_def_off_stats_s2d <- function(df) {
    off_tbl <- safe_tbl(off_team_stats_week_tbl_name)
    def_tbl <- safe_tbl(def_team_stats_week_tbl_name)
    if (is.null(off_tbl) && is.null(def_tbl)) {
      message("off_team_stats_week_tbl and def_team_stats_week_tbl not found; skipping.")
      return(df)
    }
    
    # collect once
    off_wk <- if (!is.null(off_tbl)) {
      off_tbl %>%
        dplyr::filter(season %in% !!seasons) %>%
        dplyr::select(dplyr::any_of(c(
          "season","week","team",
          "completions","attempts","passing_yards","passing_tds","interceptions",
          "sacks","passing_first_downs","carries","rushing_yards","rushing_tds",
          "rushing_fumbles","receiving_fumbles"
        ))) %>%
        dplyr::mutate(
          week = as.integer(week),
          across(c(completions,attempts,passing_yards,passing_tds,interceptions,
                   sacks,passing_first_downs,carries,rushing_yards,rushing_tds,
                   rushing_fumbles,receiving_fumbles), ~ as.numeric(.x))
        ) %>%
        dplyr::collect()
    } else NULL
    
    def_wk <- if (!is.null(def_tbl)) {
      def_tbl %>%
        dplyr::filter(season %in% !!seasons) %>%
        dplyr::select(dplyr::any_of(c(
          "season","week","team",
          "def_interceptions","def_fumble_recovery_opp",
          "def_penalty","def_penalty_yards",
          "def_sacks","def_qb_hits","def_tackles_for_loss"
        ))) %>%
        dplyr::mutate(
          week = as.integer(week),
          across(c(def_interceptions,def_fumble_recovery_opp,def_penalty,def_penalty_yards,
                   def_sacks,def_qb_hits,def_tackles_for_loss), ~ as.numeric(.x))
        ) %>%
        dplyr::collect()
    } else NULL
    
    attach_side <- function(gdf, side = c("home","away")) {
      side <- match.arg(side)
      key_team <- rlang::sym(paste0(side, "_team"))
      
      base <- gdf %>% dplyr::left_join(reg_last_week_by_season, by = "season")
      
      # OFFENSE aggregates
      off_aggs <- if (!is.null(off_wk)) {
        base %>%
          dplyr::left_join(off_wk %>% dplyr::rename(stat_week = week),
                           by = dplyr::join_by(season == season, !!key_team == team),
                           relationship = "many-to-many") %>%
          dplyr::group_by(game_id, season, season_type, week, reg_last_week) %>%
          dplyr::summarise(
            n_prior = {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              sum(idx, na.rm = TRUE)
            },
            !!paste0(side, "_off_comp_pct_prior") := {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              num <- sum(completions[idx], na.rm = TRUE)
              den <- sum(attempts[idx], na.rm = TRUE)
              ifelse(den > 0, num/den, 0)
            },
            !!paste0(side, "_off_pass_yards_pg_prior") := {
              n <- first(n_prior); if (n == 0) 0 else sum(passing_yards[stat_week < first(week) | (first(season_type)!="REG" & stat_week <= first(reg_last_week))], na.rm = TRUE) / n
            },
            !!paste0(side, "_off_pass_tds_pg_prior") := {
              n <- first(n_prior); if (n == 0) 0 else sum(passing_tds[stat_week < first(week) | (first(season_type)!="REG" & stat_week <= first(reg_last_week))], na.rm = TRUE) / n
            },
            !!paste0(side, "_off_total_int_prior") := {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              sum(interceptions[idx], na.rm = TRUE)
            },
            !!paste0(side, "_off_total_fumbles_prior") := {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              sum(rushing_fumbles[idx], na.rm = TRUE) + sum(receiving_fumbles[idx], na.rm = TRUE)
            },
            !!paste0(side, "_off_total_sacks_allowed_prior") := {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              sum(sacks[idx], na.rm = TRUE)
            },
            !!paste0(side, "_off_pass_fd_pg_prior") := {
              n <- first(n_prior); if (n == 0) 0 else sum(passing_first_downs[stat_week < first(week) | (first(season_type)!="REG" & stat_week <= first(reg_last_week))], na.rm = TRUE) / n
            },
            !!paste0(side, "_off_carries_pg_prior") := {
              n <- first(n_prior); if (n == 0) 0 else sum(carries[stat_week < first(week) | (first(season_type)!="REG" & stat_week <= first(reg_last_week))], na.rm = TRUE) / n
            },
            !!paste0(side, "_off_rush_yards_pg_prior") := {
              n <- first(n_prior); if (n == 0) 0 else sum(rushing_yards[stat_week < first(week) | (first(season_type)!="REG" & stat_week <= first(reg_last_week))], na.rm = TRUE) / n
            },
            !!paste0(side, "_off_rush_tds_pg_prior") := {
              n <- first(n_prior); if (n == 0) 0 else sum(rushing_tds[stat_week < first(week) | (first(season_type)!="REG" & stat_week <= first(reg_last_week))], na.rm = TRUE) / n
            },
            .groups = "drop"
          )
      } else NULL
      
      # DEFENSE aggregates
      def_aggs <- if (!is.null(def_wk)) {
        base %>%
          dplyr::left_join(def_wk %>% dplyr::rename(stat_week = week),
                           by = dplyr::join_by(season == season, !!key_team == team),
                           relationship = "many-to-many") %>%
          dplyr::group_by(game_id, season, season_type, week, reg_last_week) %>%
          dplyr::summarise(
            n_prior = {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              sum(idx, na.rm = TRUE)
            },
            !!paste0(side, "_def_total_turnovers_prior") := {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              sum(def_interceptions[idx], na.rm = TRUE) + sum(def_fumble_recovery_opp[idx], na.rm = TRUE)
            },
            !!paste0(side, "_def_total_int_prior") := {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              sum(def_interceptions[idx], na.rm = TRUE)
            },
            !!paste0(side, "_def_total_fumrec_prior") := {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              sum(def_fumble_recovery_opp[idx], na.rm = TRUE)
            },
            !!paste0(side, "_def_penalties_pg_prior") := {
              n <- first(n_prior); if (n == 0) 0 else sum(def_penalty[stat_week < first(week) | (first(season_type)!="REG" & stat_week <= first(reg_last_week))], na.rm = TRUE) / n
            },
            !!paste0(side, "_def_penalty_yards_pg_prior") := {
              n <- first(n_prior); if (n == 0) 0 else sum(def_penalty_yards[stat_week < first(week) | (first(season_type)!="REG" & stat_week <= first(reg_last_week))], na.rm = TRUE) / n
            },
            !!paste0(side, "_def_total_sacks_prior") := {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              sum(def_sacks[idx], na.rm = TRUE)
            },
            !!paste0(side, "_def_total_qb_hits_prior") := {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              sum(def_qb_hits[idx], na.rm = TRUE)
            },
            !!paste0(side, "_def_total_tfl_prior") := {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              sum(def_tackles_for_loss[idx], na.rm = TRUE)
            },
            .groups = "drop"
          )
      } else NULL
      
      out <- gdf
      if (!is.null(off_aggs)) out <- out %>% dplyr::left_join(off_aggs, by = c("game_id","season","season_type","week"))
      if (!is.null(def_aggs)) out <- out %>% dplyr::left_join(def_aggs, by = c("game_id","season","season_type","week"))
      out
    }
    
    out <- df %>% attach_side("home") %>% attach_side("away")
    
    # simple diffs could be added later if desired (keeping it minimal per request)
    out
  }
  
  games_ts_inj <- games_ts_inj %>%
    add_snapcounts_s2d() %>%
    add_qb_cap_pct() %>%
    add_position_stability() %>%
    add_team_basics_s2d() %>%
    add_def_off_stats_s2d()
  
  #-----------------------------------------
  # S4. Minimal Week-1 QB priors (no is_starter; uses starters table)
  #-----------------------------------------
  add_qb_priors <- function(df) {
    starters <- safe_tbl(starters_tbl_name)
    if (is.null(starters) || !tbl_exists(career_stats_qb_table)) {
      message("Starters or QB career stats table not found; skipping QB priors.")
      return(
        df %>%
          dplyr::mutate(
            used_prior_home = dplyr::if_else(.data$season_type=="REG" & .data$week==1, 1L, 0L),
            used_prior_away = dplyr::if_else(.data$season_type=="REG" & .data$week==1, 1L, 0L)
          )
      )
    }
    
    starters_ids <- starters %>%
      dplyr::filter(season %in% !!seasons) %>%
      dplyr::select(season, week, team, gsis_id) %>%
      dplyr::collect() %>%
      dplyr::mutate(
        season  = as.integer(season),
        week    = as.integer(week),
        gsis_id = as.character(gsis_id)
      ) %>%
      dplyr::distinct(season, week, team, gsis_id)
    
    qb_career <- tbl(con, in_sch(career_stats_qb_table)) %>%
      dplyr::select(player_id, passing_yards, passing_tds, interceptions, sacks, sack_yards, attempts) %>%
      dplyr::collect() %>%
      dplyr::mutate(
        attempts = dplyr::coalesce(attempts, 0),
        sacks    = dplyr::coalesce(sacks, 0),
        denom    = pmax(attempts + sacks, 1),
        qb_prior = dplyr::case_when(
          all(c("passing_yards","passing_tds","interceptions","sack_yards") %in% names(.)) ~
            (passing_yards + 20*passing_tds - 45*interceptions - dplyr::coalesce(sack_yards, 0)) / denom,
          !is.na(passing_yards) ~ passing_yards / denom,
          TRUE ~ NA_real_
        )
      ) %>%
      dplyr::transmute(gsis_id = as.character(player_id), qb_prior)
    
    overall_med_prior <- stats::median(qb_career$qb_prior, na.rm = TRUE)
    
    attach_side <- function(gdf, side = c("home","away")) {
      side <- match.arg(side)
      key_team <- rlang::sym(paste0(side, "_team"))
      
      gdf %>%
        dplyr::left_join(
          starters_ids,
          by = dplyr::join_by(season == season, week == week, !!key_team == team),
          relationship = "many-to-many"
        ) %>%
        dplyr::left_join(qb_career, by = "gsis_id", relationship = "many-to-many") %>%
        dplyr::group_by(season) %>%
        dplyr::mutate(season_median_prior = suppressWarnings(stats::median(qb_prior, na.rm = TRUE))) %>%
        dplyr::ungroup() %>%
        dplyr::group_by(game_id, season, season_type, week) %>%
        dplyr::summarise(
          !!paste0(side, "_qb_prior") := {
            v <- dplyr::coalesce(qb_prior, season_median_prior, overall_med_prior)
            val <- suppressWarnings(max(v, na.rm = TRUE))
            if (!is.finite(val)) overall_med_prior else val
          },
          .groups = "drop"
        ) %>%
        dplyr::mutate(!!paste0("used_prior_", side) := as.integer(season_type == "REG" & week == 1)) %>%
        dplyr::select(game_id, dplyr::all_of(paste0(side, "_qb_prior")), dplyr::all_of(paste0("used_prior_", side)))
    }
    
    home <- attach_side(df, "home")
    away <- attach_side(df, "away")
    
    df %>%
      dplyr::left_join(home, by = "game_id") %>%
      dplyr::left_join(away, by = "game_id") %>%
      dplyr::mutate(diff_qb_prior = .data$home_qb_prior - .data$away_qb_prior)
  }
  
  out <- games_ts_inj %>%
    add_qb_priors() %>%
    mutate(season_type = toupper(.data$season_type)) %>%
    relocate(.data$home_team, .data$away_team, .after = .data$week) %>%
    relocate(starts_with("diff_"), .after = dplyr::last_col()) %>%
    dplyr::select(-is_dome) %>%
    dplyr::mutate(surface = ifelse(surface == '', "a_turf", surface)) %>%
    # Targets we can compute safely now (no spread yet):
    dplyr::mutate(
      margin       = as.numeric(home_score) - as.numeric(away_score),
      total_points = as.numeric(home_score) + as.numeric(away_score),
      home_win = dplyr::case_when(
        home_score > away_score ~ 1L,
        home_score < away_score ~ 0L,
        TRUE ~ NA_integer_
      )
    ) %>%
    dplyr::filter(!is.na(home_win))
  
  out <- out %>%
    # 3a) Rename point differential totals (they are totals, not per-game)
    dplyr::rename(
      home_pt_diff_prior = home_pt_diff_pg_prior,
      away_pt_diff_prior = away_pt_diff_pg_prior,
      diff_pt_diff_prior = diff_pt_diff_pg_prior
    ) %>%
    # 3b) Add turnover differential (TOTALS prior to game)
    dplyr::mutate(
      home_to_diff_prior = home_def_total_turnovers_prior - (home_off_total_int_prior + home_off_total_fumbles_prior),
      away_to_diff_prior = away_def_total_turnovers_prior - (away_off_total_int_prior + away_off_total_fumbles_prior),
      diff_to_diff_prior = home_to_diff_prior - away_to_diff_prior
    )
  
  # Finalize spread sign + spread_covered and move targets to the end
  out <- finalize_targets_and_clean(out, push_as_met = TRUE)
  
  # Heads-up about optional blocks
  present <- names(out)
  msg_bits <- c()
  if (!any(grepl("^diff_inj_", present))) msg_bits <- c(msg_bits, "injury diffs")
  if (!any(grepl("qb_prior", present)))   msg_bits <- c(msg_bits, "QB priors")
  if (length(msg_bits)) message("Note: missing optional blocks -> ", paste(msg_bits, collapse = ", "), ".")
  
  out
}

# Clean join artefacts, standardize spread to "home perspective", and finalize targets.
# - Drops helper columns: reg_last_week.*, n_prior.*, and any suffix-only .x/.y/.x.x etc.
# - Infers spread sign via correlation with home margin:
#     * If cor(spread_line, home_margin) >= 0  -> spread_home = spread_line (positive = home fav)
#     * Else                                   -> spread_home = -spread_line
# - Computes:
#     margin         = home_score - away_score
#     total_points   = home_score + away_score
#     spread_covered = 1 if (margin - spread_home) >= 0 (push = met by default)
# - Keeps the last four columns as targets: home_win, margin, spread_covered, total_points

finalize_targets_and_clean <- function(df, push_as_met = TRUE) {
  stopifnot(all(c("home_score","away_score","spread_line") %in% names(df)))
  suppressPackageStartupMessages({
    library(dplyr)
  })
  
  # 1) Drop join cruft
  df0 <- df %>% 
    select(
      -matches("(^reg_last_week\\.|^n_prior\\.|\\.(x|y)(\\.(x|y))*$)")
    )
  
  # 2) Targets: margin & total points
  df0 <- df0 %>%
    mutate(
      margin       = home_score - away_score,
      total_points = home_score + away_score
    )
  
  # 3) Infer spread sign -> standardize to "home perspective"
  #    Positive spread_home means home was favored by that many points.
  co <- suppressWarnings(stats::cor(df0$spread_line, df0$margin, use = "complete.obs"))
  spread_home <- if (!is.na(co) && is.finite(co) && co >= 0) df0$spread_line else -df0$spread_line
  df0 <- mutate(df0, spread_home = spread_home)
  
  # 4) Home cover flag (push handling configurable)
  df0 <- df0 %>%
    mutate(
      spread_covered = if (isTRUE(push_as_met)) {
        as.integer((margin - spread_home) >= 0)
      } else {
        as.integer((margin - spread_home) > 0)
      }
    )
  
  # 5) Put the four targets at the very end
  df0 %>%
    relocate(home_win, margin, spread_covered, total_points, .after = dplyr::last_col())
}
