#' Add weekly team ranking features with robust team alias mapping
#' (SD→LAC, STL/LAR→LA, OAK→LV, etc.), wide home/away, diffs, and matchup diffs.
#' @export
add_team_rankings_features <- function(
    con,
    df,
    seasons,
    schema = "prod",
    rankings_table = "team_weekly_rankings_tbl",
    ranking_stats = c(
      'passing_yards','passing_tds','interceptions','sacks',
      'passing_first_downs','passing_epa','avg_drive_depth_into_opp',
      'avg_start_yardline_100','drives','early_epa_per_play',
      'carries','rushing_yards','rushing_tds','rushing_fumbles',
      'rushing_first_downs','rushing_epa','points_scored',
      'def_tackles','def_tackles_for_loss','def_fumbles_forced',
      'def_sacks','def_qb_hits','def_interceptions','def_fumbles',
      'def_penalty','points_allowed','fg_pct',
      'def_passing_yards_allowed','def_passing_tds_allowed','def_passing_first_downs_allowed',
      'def_pass_epa_allowed','def_avg_drive_depth_allow','def_drives_allowed','def_carries_allowed',
      'def_rushing_yards_allowed','def_rushing_tds_allowed','def_rushing_first_downs_allowed',
      'def_rushing_epa_allowed'
    ),
    offense_allowed_map = c(
      passing_yards        = "def_passing_yards_allowed",
      passing_tds          = "def_passing_tds_allowed",
      passing_first_downs  = "def_passing_first_downs_allowed",
      passing_epa          = "def_pass_epa_allowed",
      rushing_yards        = "def_rushing_yards_allowed",
      rushing_tds          = "def_rushing_tds_allowed",
      rushing_first_downs  = "def_rushing_first_downs_allowed",
      rushing_epa          = "def_rushing_epa_allowed",
      avg_drive_depth_into_opp = "def_avg_drive_depth_allow",
      drives               = "def_drives_allowed",   # optional pace
      carries              = "def_carries_allowed",  # optional pace
      points_scored        = "points_allowed"
    ),
    include_pace_pairs = FALSE,
    verbose = TRUE
) {
  stopifnot(all(c("game_id","season","week","home_team","away_team") %in% names(df)))
  
  # ---- Canonical team codes (target set = your “exact names” list) ----
  canonicalize_team <- function(x) {
    x <- toupper(trimws(as.character(x)))
    alias <- c(
      # Legacy -> Modern canonical
      "SD"="LAC","S.D."="LAC","SDG"="LAC","SDC"="LAC","SD CHARGERS"="LAC",
      "OAK"="LV","RAI"="LV","OAK RAIDERS"="LV",
      "STL"="LA","ST.L"="LA","ST LOUIS"="LA","LAR"="LA","L.A. RAMS"="LA","LA RAMS"="LA",
      # Other common variants to canonical
      "JAC"="JAX","NOR"="NO","N.O."="NO","GNB"="GB","WSH"="WAS","WDC"="WAS",
      # Already-canonical passthroughs
      "ARI"="ARI","ATL"="ATL","BAL"="BAL","BUF"="BUF","CAR"="CAR","CHI"="CHI",
      "CIN"="CIN","CLE"="CLE","DAL"="DAL","DEN"="DEN","DET"="DET","GB"="GB","HOU"="HOU",
      "IND"="IND","JAX"="JAX","KC"="KC","LA"="LA","LAC"="LAC","LV"="LV","MIA"="MIA",
      "MIN"="MIN","NE"="NE","NO"="NO","NYG"="NYG","NYJ"="NYJ","PHI"="PHI","PIT"="PIT",
      "SD "="LAC","SEA"="SEA","SF"="SF","TB"="TB","TEN"="TEN","WAS"="WAS"
    )
    y <- ifelse(x %in% names(alias), unname(alias[x]), x)
    as.character(y)
  }
  
  # Trim map (drop pace pairs if requested)
  if (!is.null(offense_allowed_map) && !include_pace_pairs) {
    offense_allowed_map <- offense_allowed_map[!names(offense_allowed_map) %in% c("drives","carries")]
  }
  
  # ---- Load and canonicalize rankings ----
  ranks_tbl <- dplyr::tbl(con, dbplyr::in_schema(schema, rankings_table)) %>%
    dplyr::filter(.data$season %in% !!seasons) %>%
    dplyr::select(dplyr::any_of(c("season","week","team","stat_name","rank"))) %>%
    dplyr::collect() %>%
    dplyr::mutate(
      season    = as.integer(.data$season),
      week      = as.integer(.data$week),
      team_raw  = as.character(.data$team),
      team      = canonicalize_team(.data$team),
      stat_name = as.character(.data$stat_name),
      rank      = as.numeric(.data$rank)
    ) %>%
    dplyr::filter(.data$stat_name %in% ranking_stats) %>%
    dplyr::group_by(.data$season, .data$week, .data$team, .data$stat_name) %>%
    dplyr::summarise(rank = dplyr::first(.data$rank), .groups = "drop")
  
  # ---- Canonicalize games team codes ----
  games_can <- df %>%
    dplyr::mutate(
      home_team_can = canonicalize_team(.data$home_team),
      away_team_can = canonicalize_team(.data$away_team)
    )
  
  # ---- HOME wide ----
  home_wide <- games_can %>%
    dplyr::select(.data$game_id, .data$season, .data$week, home_team_can) %>%
    dplyr::left_join(
      ranks_tbl %>% dplyr::rename(home_team_can = .data$team),
      by = c("season","week","home_team_can")
    ) %>%
    tidyr::pivot_wider(
      id_cols = c(.data$game_id),
      names_from  = .data$stat_name,
      values_from = .data$rank,
      names_glue  = "home_{stat_name}_rank"
    )
  
  # ---- AWAY wide ----
  away_wide <- games_can %>%
    dplyr::select(.data$game_id, .data$season, .data$week, away_team_can) %>%
    dplyr::left_join(
      ranks_tbl %>% dplyr::rename(away_team_can = .data$team),
      by = c("season","week","away_team_can")
    ) %>%
    tidyr::pivot_wider(
      id_cols = c(.data$game_id),
      names_from  = .data$stat_name,
      values_from = .data$rank,
      names_glue  = "away_{stat_name}_rank"
    )
  
  # ---- Attach to games ----
  out <- games_can %>%
    dplyr::select(-home_team_can, -away_team_can) %>%
    dplyr::left_join(home_wide, by = "game_id") %>%
    dplyr::left_join(away_wide, by = "game_id")
  
  # ---- Generic diffs ----
  for (s in unique(ranking_stats)) {
    h <- paste0("home_", s, "_rank")
    a <- paste0("away_", s, "_rank")
    d <- paste0("diff_", s, "_rank")
    if (h %in% names(out) && a %in% names(out)) out[[d]] <- out[[h]] - out[[a]]
  }
  
  # ---- Matchup diffs (home offense − away defense-allowed) ----
  if (!is.null(offense_allowed_map) && length(offense_allowed_map) > 0) {
    valid_pairs <- offense_allowed_map[
      names(offense_allowed_map) %in% ranking_stats &
        offense_allowed_map %in% ranking_stats
    ]
    for (off_s in names(valid_pairs)) {
      def_s <- valid_pairs[[off_s]]
      h_off <- paste0("home_", off_s, "_rank")
      a_def <- paste0("away_", def_s, "_rank")
      d_nm  <- paste0("diff_", off_s, "_vs_", def_s, "_rank")
      if (h_off %in% names(out) && a_def %in% names(out)) {
        out[[d_nm]] <- out[[h_off]] - out[[a_def]]
      }
    }
  }
  
  # ---- Diagnostics: any remaining missing joins? ----
  if (isTRUE(verbose)) {
    probe <- paste0("home_", ranking_stats[1], "_rank")
    if (probe %in% names(out)) {
      miss_home <- sum(is.na(out[[probe]]))
    } else miss_home <- NA_integer_
    probe2 <- paste0("away_", ranking_stats[1], "_rank")
    if (probe2 %in% names(out)) {
      miss_away <- sum(is.na(out[[probe2]]))
    } else miss_away <- NA_integer_
    
    if (is.finite(miss_home) && is.finite(miss_away) && (miss_home + miss_away) > 0) {
      message("Rank join NA after aliasing — home: ", miss_home, ", away: ", miss_away,
              ". Check for unseen aliases in schedules or rankings.")
    }
  }
  
  # Put diffs at the end
  diff_cols <- grep("^diff_.*_rank$", names(out), value = TRUE)
  out <- dplyr::relocate(out, dplyr::all_of(diff_cols), .after = dplyr::last_col())
  
  out
}

#' Build pregame, game-level modelling dataset (2016–present) with as-of features
#'
#' @description
#' Creates a **one-row-per-game** matrix for pre-game win modelling. Features include:
#' - team strength (as-of): `rating_net`, `net_epa_smooth` for home/away + `diff_*`
#' - injuries (T−30m): position-weighted indices for OL/WR/TE/RB/DL/LB/DB, home/away + diffs
#' - weather/context: `temp`, `wind`, buckets, `roof`, `surface`, `dome_flag`
#' - minimal Week-1 **QB priors** (last-season avg), with `used_prior_*` flags (Week 1 only)
#'
#' All season-to-date team features are computed **as of kickoff** via week fences
#' (REG: use games through week N−1; POST: uses all REG weeks).
#'
#' @param con DBI connection to Postgres.
#' @param seasons integer vector of seasons to include (e.g., 2016:2025).
#' @param schema character schema name. Default: "prod".
#' @param games_table character table name for schedules/games. Default: "games_tbl".
#' @param team_strength_table character table with weekly team ratings. Default: "team_strength_weekly_tbl".
#' @param injuries_table character table with T−30m injuries. Default: "injuries_tbl".
#' @param injury_weights named numeric of status weights. Default: c(Out=1.0, Doubtful=0.7, Questionable=0.3).
#' @param depth_charts_qb_table optional table for starters. Default: "depth_charts_starters_tbl".
#' @param career_stats_qb_table table for QB stats (seasonal if available). Default: "career_stats_qb_tbl".
#' @return A tibble with one row per game and engineered features suitable for LR / RF / XGB.
#' @export
build_pregame_dataset <- function(
    con,
    seasons = 2016:2025,
    schema  = "prod",
    games_table = "games_tbl",
    team_strength_table = "team_strength_weekly_tbl",
    injuries_table = "injuries_tbl",
    injury_weights = c(Out = 1.0, Doubtful = 0.7, Questionable = 0.3),
    depth_charts_qb_table = "depth_charts_starters_tbl",
    career_stats_qb_table = "career_stats_qb_tbl"
) {
  requireNamespace("dplyr", quietly = TRUE)
  requireNamespace("dbplyr", quietly = TRUE)
  requireNamespace("tidyr", quietly = TRUE)
  requireNamespace("stringr", quietly = TRUE)
  requireNamespace("lubridate", quietly = TRUE)
  library(dplyr); library(dbplyr); library(tidyr); library(stringr); library(lubridate)
  
  in_sch <- function(tbl) dbplyr::in_schema(schema, tbl)
  tbl_exists <- function(tbl) DBI::dbExistsTable(con, DBI::Id(schema = schema, table = tbl))
  
  # ---- TABLE NAMES ----
  pbp_off_tbl_name   <- "participation_offense_pbp_tbl" # (unused, keep for compatibility)
  pbp_def_tbl_name   <- "participation_defense_pbp_tbl" # (unused, keep for compatibility)
  snap_week_tbl_name <- "snapcount_weekly_tbl"
  contracts_qb_tbl_name <- "contracts_qb_tbl"
  starters_tbl_name  <- depth_charts_qb_table
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
  normalize_abbr <- function(x) {
    x <- toupper(trimws(as.character(x)))
    dplyr::recode(
      x,
      "SD"  = "LAC",
      "STL" = "LA",
      "LAR" = "LA",   # unify to your schema's "LA"
      "OAK" = "LV",
      "JAC" = "JAX",
      "NOR" = "NO",
      "WSH" = "WAS",
      "GNB" = "GB",
      .default = x
    )
  }
  
  impute_weather <- function(df) {
    df0 <- df %>%
      mutate(
        stid    = coalesce(as.character(.data$stadium_id), as.character(.data$stadium)),
        mo      = lubridate::month(.data$kickoff),
        is_dome = tolower(coalesce(.data$roof, "")) %in% c("dome","closed"),
        temp_num = suppressWarnings(as.numeric(.data$temp)),
        wind_num = suppressWarnings(as.numeric(.data$wind))
      )
    
    ref <- df0 %>%
      group_by(.data$stid, .data$roof, .data$mo) %>%
      summarise(
        temp_med = stats::median(.data$temp_num, na.rm = TRUE),
        wind_med = stats::median(.data$wind_num, na.rm = TRUE),
        .groups = "drop"
      )
    
    df1 <- df0 %>%
      left_join(ref, by = c("stid","roof","mo")) %>%
      mutate(
        temp_miss = as.integer(is.na(.data$temp_num)),
        wind_miss = as.integer(is.na(.data$wind_num)),
        temp_num  = case_when(
          .data$is_dome ~ 70,
          is.na(.data$temp_num) ~ temp_med,
          TRUE ~ .data$temp_num
        ),
        wind_num  = case_when(
          .data$is_dome ~ 0,
          is.na(.data$wind_num) ~ wind_med,
          TRUE ~ .data$wind_num
        ),
        temp = temp_num,
        wind = wind_num
      ) %>%
      select(-stid,-mo,-temp_num,-wind_num,-temp_med,-wind_med)
    
    df1 %>%
      mutate(
        wind_bucket = cut(as.numeric(.data$wind),
                          breaks = c(-Inf,5,10,15,20,Inf),
                          labels = c("0-5","6-10","11-15","16-20","21+")),
        temp_bucket = cut(as.numeric(.data$temp),
                          breaks = c(-Inf,32,49,79,Inf),
                          labels = c("≤32","33-49","50-79","80+"))
      )
  }
  
  # --- S1. Base games/schedules ---
  games_raw <- tbl(con, in_sch(games_table)) %>%
    filter(.data$season %in% !!seasons) %>%
    select(any_of(c(
      "game_id","season","week","game_type","season_type",
      "home_team","away_team",
      "kickoff",
      "stadium","stadium_id","roof","surface","temp","wind",
      "home_score","away_score",
      "spread_line"
    ))) %>%
    collect()
  
  if (!"season_type" %in% names(games_raw) && "game_type" %in% names(games_raw)) {
    games_raw <- games_raw %>%
      mutate(season_type = ifelse(toupper(.data$game_type) == "REG", "REG", "POST"))
  }
  
  retractable_ids <- c("IND00","ATL97","PHO00","DAL00","HOU00")
  
  games_base <- games_raw %>%
    mutate(
      season_type = if (!"season_type" %in% names(.)) ifelse(toupper(game_type)=="REG","REG","POST") else season_type,
      kickoff = if (inherits(kickoff, "POSIXt")) kickoff else
        suppressWarnings(lubridate::parse_date_time(
          kickoff,
          orders = c("Y-m-d H:M:S","Y-m-d H:M","m/d/Y H:M","Ymd HMS","Ymd HM"),
          tz = "America/New_York"
        )),
      roof_clean = tolower(coalesce(roof, "")),
      roof_state = case_when(
        roof_clean %in% c("dome","closed") ~ "indoors",
        roof_clean == "open" & stadium_id %in% retractable_ids ~ "retractable_open",
        roof_clean %in% c("open","outdoors") ~ "outdoors",
        TRUE ~ "unknown"
      ),
      dome_flag = roof_state == "indoors"
    ) %>%
    mutate(
      temp_num = suppressWarnings(as.numeric(gsub("[^0-9.-]","", as.character(temp)))),
      wind_num = suppressWarnings(as.numeric(gsub("[^0-9.-]","", as.character(wind)))),
      temp_miss = as.integer(is.na(temp_num)),
      wind_miss = as.integer(is.na(wind_num)),
      mo = lubridate::month(kickoff)
    )
  
  games_base <- games_base %>%
    mutate(
      home_team = normalize_abbr(home_team),
      away_team = normalize_abbr(away_team)
    )
  
  ref_stad_mo <- games_base %>%
    group_by(stadium_id, roof_state, mo) %>%
    summarise(
      temp_med = stats::median(temp_num, na.rm = TRUE),
      wind_med = stats::median(wind_num, na.rm = TRUE),
      .groups = "drop"
    )
  ref_roof_mo <- games_base %>%
    group_by(roof_state, mo) %>%
    summarise(
      temp_med = stats::median(temp_num, na.rm = TRUE),
      wind_med = stats::median(wind_num, na.rm = TRUE),
      .groups = "drop"
    )
  ref_mo <- games_base %>%
    group_by(mo) %>%
    summarise(
      temp_med = stats::median(temp_num, na.rm = TRUE),
      wind_med = stats::median(wind_num, na.rm = TRUE),
      .groups = "drop"
    )
  
  games_base <- games_base %>%
    left_join(ref_stad_mo, by = c("stadium_id","roof_state","mo")) %>%
    rename(temp_med_stad = temp_med, wind_med_stad = wind_med) %>%
    left_join(ref_roof_mo, by = c("roof_state","mo")) %>%
    rename(temp_med_roof = temp_med, wind_med_roof = wind_med) %>%
    left_join(ref_mo, by = "mo") %>%
    rename(temp_med_mo = temp_med, wind_med_mo = wind_med) %>%
    mutate(
      temp = case_when(
        roof_state == "indoors" ~ 70,
        !is.na(temp_num) ~ temp_num,
        !is.na(temp_med_stad) ~ temp_med_stad,
        !is.na(temp_med_roof) ~ temp_med_roof,
        !is.na(temp_med_mo) ~ temp_med_mo,
        TRUE ~ 70
      ),
      wind = case_when(
        roof_state == "indoors" ~ 0,
        !is.na(wind_num) ~ wind_num,
        !is.na(wind_med_stad) ~ wind_med_stad,
        !is.na(wind_med_roof) ~ wind_med_roof,
        !is.na(wind_med_mo) ~ wind_med_mo,
        TRUE ~ 0
      )
    ) %>%
    mutate(
      wind_bucket = cut(as.numeric(wind), breaks = c(-Inf,5,10,15,20,Inf),
                        labels = c("0-5","6-10","11-15","16-20","21+")),
      temp_bucket = cut(as.numeric(temp), breaks = c(-Inf,32,49,79,Inf),
                        labels = c("≤32","33-49","50-79","80+"))
    ) %>%
    select(-temp_num, -wind_num, -temp_med_stad, -wind_med_stad,
           -temp_med_roof, -wind_med_roof, -temp_med_mo, -wind_med_mo, -mo, -roof_clean)
  
  games_base <- games_base %>% impute_weather()
  
  # REG-season last week (for POST fences and prior-season picks)
  reg_last_week_by_season <- games_base %>%
    filter(.data$season_type == "REG") %>%
    group_by(.data$season) %>%
    summarise(reg_last_week = max(.data$week, na.rm = TRUE), .groups = "drop")
  
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
      filter(if_else(season_type == "REG", ts_week < week, TRUE)) %>%
      group_by(game_id) %>%
      slice_max(order_by = ts_week, n = 1, with_ties = FALSE) %>%
      ungroup() %>%
      transmute(
        game_id,
        !!paste0(side, "_rating_net")      := rating_net,
        !!paste0(side, "_net_epa_smooth")  := net_epa_smooth
      )
  }
  
  ts_home <- get_ts_asof(games_base, "home")
  ts_away <- get_ts_asof(games_base, "away")
  
  games_ts <- games_base %>%
    left_join(ts_home, by = "game_id") %>%
    left_join(ts_away, by = "game_id")
  
  # Week 1 ONLY: backfill prior-season REG last-week team strength (no shrinkage by HC/QB)
  prior_ts_last <- ts_wk %>%
    left_join(reg_last_week_by_season, by = "season") %>%
    filter(!is.na(reg_last_week), ts_week <= reg_last_week) %>%
    group_by(season, team) %>%
    slice_max(ts_week, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    transmute(season_next = season + 1L, team,
              rating_net_prev = rating_net,
              net_epa_smooth_prev = net_epa_smooth)
  
  games_ts <- games_ts %>%
    # attach prior-season values keyed to next season (for Week 1)
    left_join(prior_ts_last %>% rename(home_team_prev = team,
                                       home_rating_prev = rating_net_prev,
                                       home_epa_prev    = net_epa_smooth_prev),
              by = c("season" = "season_next", "home_team" = "home_team_prev")) %>%
    left_join(prior_ts_last %>% rename(away_team_prev = team,
                                       away_rating_prev = rating_net_prev,
                                       away_epa_prev    = net_epa_smooth_prev),
              by = c("season" = "season_next", "away_team" = "away_team_prev")) %>%
    mutate(
      home_rating_net     = if_else(season_type=="REG" & week==1 & is.na(home_rating_net), home_rating_prev, home_rating_net),
      home_net_epa_smooth = if_else(season_type=="REG" & week==1 & is.na(home_net_epa_smooth), home_epa_prev, home_net_epa_smooth),
      away_rating_net     = if_else(season_type=="REG" & week==1 & is.na(away_rating_net), away_rating_prev, away_rating_net),
      away_net_epa_smooth = if_else(season_type=="REG" & week==1 & is.na(away_net_epa_smooth), away_epa_prev, away_net_epa_smooth)
    ) %>%
    select(-home_rating_prev,-home_epa_prev,-away_rating_prev,-away_epa_prev) %>%
    mutate(
      diff_rating_net     = home_rating_net - away_rating_net,
      diff_net_epa_smooth = home_net_epa_smooth - away_net_epa_smooth
    ) %>%
    # Flags & coalesce (unchanged behaviour elsewhere)
    mutate(
      used_ts_prior_home = as.integer(is.na(.data$home_rating_net) | is.na(.data$home_net_epa_smooth)),
      used_ts_prior_away = as.integer(is.na(.data$away_rating_net) | is.na(.data$away_net_epa_smooth)),
      home_rating_net     = coalesce(.data$home_rating_net, 0),
      away_rating_net     = coalesce(.data$away_rating_net, 0),
      home_net_epa_smooth = coalesce(.data$home_net_epa_smooth, 0),
      away_net_epa_smooth = coalesce(.data$away_net_epa_smooth, 0),
      diff_rating_net     = .data$home_rating_net - .data$away_rating_net,
      diff_net_epa_smooth = .data$home_net_epa_smooth - .data$away_net_epa_smooth
    )
  
  #-----------------------------
  # S3. Injuries (weekly, by position) — within-season only
  #-----------------------------
  add_injury_indices <- function(df) {
    if (!tbl_exists(injuries_table)) {
      message("Injuries table not found: ", injuries_table, " — skipping injury features.")
      return(df)
    }
    
    inj_raw <- tbl(con, in_sch(injuries_table)) %>%
      filter(.data$season %in% !!seasons) %>%
      select(any_of(c(
        "season","week","team",
        "position","position_group","status",
        "position_injuries"
      ))) %>%
      collect() %>%
      mutate(
        season   = as.integer(.data$season),
        week     = as.integer(.data$week),
        team     = as.character(.data$team)
      )
    
    if (!"position_group" %in% names(inj_raw)) {
      inj_raw <- inj_raw %>% mutate(position_group = NA_character_)
    }
    
    inj_norm <- inj_raw %>%
      mutate(
        position = toupper(coalesce(.data$position, "")),
        pos_group = coalesce(
          toupper(.data$position_group),
          case_when(
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
      filter(!is.na(.data$pos_group))
    
    has_status <- "status" %in% names(inj_norm)
    
    if (has_status) {
      status_key <- tibble::tibble(
        status = names(injury_weights),
        w = as.numeric(injury_weights)
      )
      inj_w <- inj_norm %>%
        mutate(status = as.character(.data$status)) %>%
        left_join(status_key, by = "status") %>%
        mutate(w = coalesce(.data$w, 0)) %>%
        group_by(.data$season, .data$week, .data$team, .data$pos_group) %>%
        summarise(inj_val = sum(.data$w, na.rm = TRUE), .groups = "drop")
    } else {
      inj_w <- inj_norm %>%
        mutate(position_injuries = as.numeric(.data$position_injuries)) %>%
        group_by(.data$season, .data$week, .data$team, .data$pos_group) %>%
        summarise(inj_val = sum(.data$position_injuries, na.rm = TRUE), .groups = "drop")
    }
    
    ipw <- inj_w %>%
      tidyr::pivot_wider(
        id_cols    = c(.data$season, .data$week, .data$team),
        names_from = .data$pos_group,
        values_from = .data$inj_val,
        names_glue = "inj_{pos_group}_count",
        values_fill = 0
      )
    
    out <- df %>%
      mutate(week = as.integer(.data$week)) %>%
      left_join(ipw, by = dplyr::join_by(season == season, week == week, home_team == team)) %>%
      rename_with(~ paste0("home_", .x), starts_with("inj_")) %>%
      left_join(ipw, by = dplyr::join_by(season == season, week == week, away_team == team)) %>%
      rename_with(~ paste0("away_", .x), starts_with("inj_")) %>%
      mutate(
        across(starts_with("home_inj_"), ~ coalesce(.x, 0)),
        across(starts_with("away_inj_"), ~ coalesce(.x, 0))
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
      filter(.data$season %in% !!seasons) %>%
      select(any_of(c("season","week","team","offense_snaps","defense_snaps"))) %>%
      mutate(
        week          = as.integer(.data$week),
        offense_snaps = as.numeric(.data$offense_snaps),
        defense_snaps = as.numeric(.data$defense_snaps)
      ) %>%
      group_by(.data$season, .data$week, .data$team) %>%
      summarise(
        team_off_snaps = max(.data$offense_snaps, na.rm = TRUE),
        team_def_snaps = max(.data$defense_snaps, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      collect()
    
    calc_side <- function(gdf, side = c("home","away")) {
      side <- match.arg(side)
      key_team <- rlang::sym(paste0(side, "_team"))
      
      gdf %>%
        left_join(
          snwk %>% rename(snap_week = week),
          by = dplyr::join_by(season == season, !!key_team == team),
          relationship = "many-to-many"
        ) %>%
        group_by(.data$game_id, .data$season, .data$week) %>%
        summarise(
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
      left_join(home, by = c("game_id","season","week")) %>%
      left_join(away, by = c("game_id","season","week")) %>%
      mutate(
        across(all_of(c(
          "home_off_snaps_pg","home_def_snaps_pg",
          "away_off_snaps_pg","away_def_snaps_pg"
        )), ~ coalesce(.x, 0)),
        diff_off_snaps_pg = .data$home_off_snaps_pg - .data$away_off_snaps_pg,
        diff_def_snaps_pg = .data$home_def_snaps_pg - .data$away_def_snaps_pg
      )
  }
  
  #--------------------------------------
  # S3c. QB APY % of Team Cap
  #--------------------------------------
  add_qb_cap_pct <- function(df) {
    starters <- safe_tbl(starters_tbl_name)
    cq       <- safe_tbl(contracts_qb_tbl_name)
    
    if (is.null(starters) || is.null(cq)) {
      message("Starters or contracts_qb table missing; skipping QB cap% features.")
      return(df)
    }
    
    qb_starters <- starters %>%
      filter(season %in% !!seasons) %>%
      select(season, week, team, position, gsis_id) %>%
      collect() %>%
      mutate(
        season   = as.integer(season),
        week     = as.integer(week),
        position = toupper(position),
        gsis_id  = as.character(gsis_id)
      ) %>%
      filter(position == "QB") %>%
      distinct(season, week, team, gsis_id)
    
    qb_contracts_raw <- cq %>%
      select(any_of(c("gsis_id","year_signed","apy_cap_pct"))) %>%
      collect() %>%
      filter(!is.na(gsis_id))
    
    season_meds <- qb_contracts_raw %>%
      filter(!is.na(year_signed), !is.na(apy_cap_pct)) %>%
      group_by(year_signed) %>%
      summarise(season_median_apy = stats::median(apy_cap_pct), .groups = "drop")
    
    overall_med <- stats::median(qb_contracts_raw$apy_cap_pct, na.rm = TRUE)
    
    attach_side <- function(gdf, side = c("home","away")) {
      side <- match.arg(side)
      key_team <- rlang::sym(paste0(side, "_team"))
      
      gdf %>%
        left_join(
          qb_starters,
          by = dplyr::join_by(season == season, week == week, !!key_team == team),
          relationship = "many-to-many"
        ) %>%
        mutate(gsis_id = as.character(gsis_id)) %>%
        left_join(qb_contracts_raw %>% select(gsis_id, apy_cap_pct), by = "gsis_id",
                  relationship = "many-to-many") %>%
        left_join(season_meds, by = c(season = "year_signed")) %>%
        mutate(
          apy_cap_pct_filled = if_else(
            is.na(apy_cap_pct),
            coalesce(season_median_apy, overall_med),
            apy_cap_pct
          )
        ) %>%
        group_by(game_id) %>%
        summarise(
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
      left_join(home, by = "game_id") %>%
      left_join(away, by = "game_id") %>%
      mutate(diff_qb_apy_pct_cap = home_qb_apy_pct_cap - away_qb_apy_pct_cap)
  }
  
  #--------------------------------------
  # S3d. Position-group stability (as-of)
  # Week 1 rule: set all group scores to 1.0 (diffs = 0)
  #--------------------------------------
  add_position_stability <- function(df) {
    ps <- safe_tbl(pos_stability_tbl_name)
    if (is.null(ps)) {
      message("Position stability table not found; skipping stability features.")
      return(df)
    }
    
    # 1) Normalize to your coarse groups
    stab <- ps %>%
      select(season, week, team, position, position_group_score) %>%
      filter(season %in% !!seasons) %>%
      collect() %>%
      mutate(
        week = as.integer(week),
        position = toupper(trimws(as.character(position))),
        pos_group = case_when(
          position %in% c("OL") ~ "OL",
          position %in% c("RB") ~ "RB",
          position %in% c("QB") ~ "QB",
          position %in% c("REC") ~ "REC",   # pass catchers
          position %in% c("DEF") ~ "DEF",   # entire defense
          position %in% c("K", "PK") ~ "K",
          position %in% c("ST", "SPECIAL", "SPECIAL TEAMS", "SPT") ~ "ST",
          TRUE ~ NA_character_
        )
      ) %>%
      filter(!is.na(pos_group)) %>%
      group_by(season, week, team, pos_group) %>%
      summarise(position_group_score = mean(position_group_score, na.rm = TRUE), .groups = "drop")
    
    # Expected set of groups in your DB
    groups <- c("OL","RB","QB","REC","DEF","K","ST")
    
    attach_side <- function(gdf, side = c("home","away")) {
      side <- match.arg(side)
      key_team <- rlang::sym(paste0(side, "_team"))
      
      base <- gdf %>%
        left_join(reg_last_week_by_season, by = "season") %>%
        left_join(
          stab %>% rename(stab_week = week),
          by = dplyr::join_by(season == season, !!key_team == team),
          relationship = "many-to-many"
        ) %>%
        mutate(use_week = if_else(season_type == "REG", week, reg_last_week))
      
      picked <- base %>%
        filter(stab_week < use_week) %>%
        group_by(game_id, pos_group) %>%
        slice_max(order_by = stab_week, n = 1, with_ties = FALSE) %>%
        ungroup() %>%
        select(game_id, pos_group, position_group_score) %>%
        tidyr::pivot_wider(
          id_cols = game_id,
          names_from = pos_group,
          values_from = position_group_score,
          names_prefix = paste0(tolower(side), "_stab_")
        )
      
      # Ensure all expected columns exist
      for (g in groups) {
        nm <- paste0(tolower(side), "_stab_", g)
        if (!nm %in% names(picked)) picked[[nm]] <- NA_real_
      }
      
      picked
    }
    
    home <- attach_side(df, "home")
    away <- attach_side(df, "away")
    
    out <- df %>%
      left_join(home, by = "game_id") %>%
      left_join(away, by = "game_id") %>%
      # Week 1 default = 1.0 for all groups (REG Week 1 only)
      mutate(
        across(matches("^home_stab_|^away_stab_"),
               ~ if_else(season_type == "REG" & week == 1L, 1.0, .x))
      ) %>%
      # Coalesce any still-missing to 0 (means we truly had no history)
      mutate(
        across(matches("^home_stab_|^away_stab_"), ~ coalesce(.x, 0))
      ) %>%
      # Flags (true only if Week 1 and something would have been missing)
      mutate(
        used_stab_prior_home = as.integer(
          (season_type == "REG" & week == 1L) &
            rowSums(across(starts_with("home_stab_"), ~ .x == 0)) > 0
        ),
        used_stab_prior_away = as.integer(
          (season_type == "REG" & week == 1L) &
            rowSums(across(starts_with("away_stab_"), ~ .x == 0)) > 0
        )
      )
    
    # Diffs for all stability groups (including QB/K/ST if you want those signals)
    for (g in groups) {
      h <- paste0("home_stab_", g)
      a <- paste0("away_stab_", g)
      d <- paste0("diff_stab_", g)
      if (h %in% names(out) && a %in% names(out)) out[[d]] <- out[[h]] - out[[a]]
    }
    
    out
  }
  
  #-----------------------------------------
  # S3e. Team S2D basics (win%, PPG, point diff totals, etc.)
  #-----------------------------------------
  add_team_basics_s2d <- function(df) {
    g0 <- df %>%
      select(any_of(c(
        "season","week","season_type","game_id","kickoff",
        "home_team","away_team","home_score","away_score"
      )))
    
    tg <- bind_rows(
      g0 %>%
        transmute(
          season, tg_week = as.integer(week), tg_season_type = season_type, kickoff,
          team = home_team, opp_team = away_team,
          pts_for = as.numeric(home_score), pts_against = as.numeric(away_score)
        ),
      g0 %>%
        transmute(
          season, tg_week = as.integer(week), tg_season_type = season_type, kickoff,
          team = away_team, opp_team = home_team,
          pts_for = as.numeric(away_score), pts_against = as.numeric(home_score)
        )
    ) %>%
      mutate(win = as.integer(pts_for > pts_against))
    
    attach_side <- function(gdf, side = c("home","away")) {
      side <- match.arg(side)
      key_team <- rlang::sym(paste0(side, "_team"))
      
      gdf %>%
        left_join(
          tg,
          by = dplyr::join_by(season == season, !!key_team == team),
          relationship = "many-to-many"
        ) %>%
        left_join(reg_last_week_by_season, by = "season") %>%
        group_by(game_id, season, season_type, week, reg_last_week) %>%
        summarise(
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
          !!paste0(side, "_pt_diff_pg_prior") := {
            idx <- if (first(season_type) == "REG") tg_week < first(week) else (tg_week <= first(reg_last_week))
            n <- sum(idx, na.rm = TRUE)
            if (n == 0) 0 else (sum(pts_for[idx], na.rm = TRUE) - sum(pts_against[idx], na.rm = TRUE)) / n
          },
          .groups = "drop"
        )
    }
    
    home <- attach_side(df, "home")
    away <- attach_side(df, "away")
    
    df %>%
      left_join(home, by = c("game_id","season","season_type","week")) %>%
      left_join(away, by = c("game_id","season","season_type","week")) %>%
      mutate(
        diff_win_pct_prior     = .data$home_win_pct_prior      - .data$away_win_pct_prior,
        diff_ppg_for_prior     = .data$home_ppg_for_prior      - .data$away_ppg_for_prior,
        diff_ppg_against_prior = .data$home_ppg_against_prior  - .data$away_ppg_against_prior,
        diff_pt_diff_pg_prior  = .data$home_pt_diff_pg_prior   - .data$away_pt_diff_pg_prior
      )
  }
  
  #-----------------------------------------
  # S3f. Offense & Defense S2D aggregates (prior to game)
  #-----------------------------------------
  add_def_off_stats_s2d <- function(df) {
    off_tbl <- safe_tbl(off_team_stats_week_tbl_name)
    def_tbl <- safe_tbl(def_team_stats_week_tbl_name)
    if (is.null(off_tbl) && is.null(def_tbl)) {
      message("off_team_stats_week_tbl and def_team_stats_week_tbl not found; skipping.")
      return(df)
    }
    
    off_wk <- if (!is.null(off_tbl)) {
      off_tbl %>%
        filter(season %in% !!seasons) %>%
        select(any_of(c(
          "season","week","team",
          "completions","attempts","passing_yards","passing_tds","interceptions",
          "sacks","passing_first_downs","carries","rushing_yards","rushing_tds",
          "rushing_fumbles","receiving_fumbles"
        ))) %>%
        mutate(
          week = as.integer(week),
          across(c(completions,attempts,passing_yards,passing_tds,interceptions,
                   sacks,passing_first_downs,carries,rushing_yards,rushing_tds,
                   rushing_fumbles,receiving_fumbles), ~ as.numeric(.x))
        ) %>%
        collect()
    } else NULL
    
    def_wk <- if (!is.null(def_tbl)) {
      def_tbl %>%
        filter(season %in% !!seasons) %>%
        select(any_of(c(
          "season","week","team",
          "def_interceptions","def_fumble_recovery_opp",
          "def_penalty","def_penalty_yards",
          "def_sacks","def_qb_hits","def_tackles_for_loss"
        ))) %>%
        mutate(
          week = as.integer(week),
          across(c(def_interceptions,def_fumble_recovery_opp,def_penalty,def_penalty_yards,
                   def_sacks,def_qb_hits,def_tackles_for_loss), ~ as.numeric(.x))
        ) %>%
        collect()
    } else NULL
    
    attach_side <- function(gdf, side = c("home","away")) {
      side <- match.arg(side)
      key_team <- rlang::sym(paste0(side, "_team"))
      
      base <- gdf %>% left_join(reg_last_week_by_season, by = "season")
      
      off_aggs <- if (!is.null(off_wk)) {
        base %>%
          left_join(off_wk %>% rename(stat_week = week),
                    by = dplyr::join_by(season == season, !!key_team == team),
                    relationship = "many-to-many") %>%
          group_by(game_id, season, season_type, week, reg_last_week) %>%
          summarise(
            n_prior = {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              sum(idx, na.rm = TRUE)
            },
            !!paste0(side, "_off_comp_pct_prior") := {
              idx <- if (first(season_type) == "REG") stat_week < first(week) else (stat_week <= first(reg_last_week))
              num <- sum(completions[idx], na.rm = TRUE)
              den <- sum(attempts[idx],   na.rm = TRUE)
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
      
      def_aggs <- if (!is.null(def_wk)) {
        base %>%
          left_join(def_wk %>% rename(stat_week = week),
                    by = dplyr::join_by(season == season, !!key_team == team),
                    relationship = "many-to-many") %>%
          group_by(game_id, season, season_type, week, reg_last_week) %>%
          summarise(
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
      if (!is.null(off_aggs)) out <- out %>% left_join(off_aggs, by = c("game_id","season","season_type","week"))
      if (!is.null(def_aggs)) out <- out %>% left_join(def_aggs, by = c("game_id","season","season_type","week"))
      out
    }
    
    out <- df %>% attach_side("home") %>% attach_side("away")
    out
  }
  
  games_ts_inj <- games_ts_inj %>%
    add_snapcounts_s2d() %>%
    add_qb_cap_pct() %>%
    add_position_stability() %>%
    add_team_basics_s2d() %>%
    add_def_off_stats_s2d() %>% 
    add_team_rankings_features(
      con      = con,
      seasons  = seasons,
      schema   = schema,
      rankings_table = "team_weekly_rankings_tbl",
    )
  
  #-----------------------------------------
  # S4. Minimal Week-1 QB priors (use LAST SEASON average; Week 1 only)
  #-----------------------------------------
  # --- drop-in replacement ---
  add_qb_priors <- function(df) {
    starters <- safe_tbl(starters_tbl_name)
    if (is.null(starters) || !tbl_exists(career_stats_qb_table)) {
      message("Starters or QB stats table not found; skipping QB priors.")
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
    
    # Load QB stats; season may or may not exist on your table
    qb_stats_raw <- tbl(con, in_sch(career_stats_qb_table)) %>%
      dplyr::select(dplyr::any_of(c(
        "player_id","gsis_id","season",
        "attempts","passing_yards","passing_tds","interceptions","sacks","sack_yards"
      ))) %>%
      dplyr::collect() %>%
      dplyr::mutate(
        attempts   = dplyr::coalesce(attempts, 0),
        sacks      = dplyr::coalesce(sacks, 0),
        sack_yards = dplyr::coalesce(sack_yards, 0)
      )
    
    # Normalize ID column name if your table uses gsis_id instead of player_id
    if (!"player_id" %in% names(qb_stats_raw) && "gsis_id" %in% names(qb_stats_raw)) {
      qb_stats_raw <- qb_stats_raw %>% dplyr::rename(player_id = gsis_id)
    }
    
    # Only coerce season if present
    if ("season" %in% names(qb_stats_raw)) {
      qb_stats_raw <- qb_stats_raw %>%
        dplyr::mutate(season = suppressWarnings(as.integer(season)))
    }
    
    has_season <- ("season" %in% names(qb_stats_raw)) && any(!is.na(qb_stats_raw$season))
    
    if (has_season) {
      qb_szn <- qb_stats_raw %>%
        dplyr::group_by(season, player_id) %>%
        dplyr::summarise(
          attempts       = sum(attempts, na.rm = TRUE),
          passing_yards  = sum(passing_yards, na.rm = TRUE),
          passing_tds    = sum(passing_tds, na.rm = TRUE),
          interceptions  = sum(interceptions, na.rm = TRUE),
          sacks          = sum(sacks, na.rm = TRUE),
          sack_yards     = sum(sack_yards, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        dplyr::mutate(
          denom    = pmax(attempts + sacks, 1),
          qb_prior = (passing_yards + 20*passing_tds - 45*interceptions - sack_yards) / denom
        ) %>%
        dplyr::transmute(season, gsis_id = as.character(player_id), qb_prior)
      
      szn_meds <- qb_szn %>%
        dplyr::group_by(season) %>%
        dplyr::summarise(season_median_prior = stats::median(qb_prior, na.rm = TRUE), .groups = "drop")
      
      overall_med_prior <- stats::median(qb_szn$qb_prior, na.rm = TRUE)
    } else {
      # No season column -> compute a single overall prior per QB and a global median
      qb_szn <- qb_stats_raw %>%
        dplyr::group_by(player_id) %>%
        dplyr::summarise(
          attempts       = sum(attempts, na.rm = TRUE),
          passing_yards  = sum(passing_yards, na.rm = TRUE),
          passing_tds    = sum(passing_tds, na.rm = TRUE),
          interceptions  = sum(interceptions, na.rm = TRUE),
          sacks          = sum(sacks, na.rm = TRUE),
          sack_yards     = sum(sack_yards, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        dplyr::mutate(
          denom    = pmax(attempts + sacks, 1),
          qb_prior = (passing_yards + 20*passing_tds - 45*interceptions - sack_yards) / denom
        ) %>%
        dplyr::transmute(gsis_id = as.character(player_id), qb_prior)
      
      overall_med_prior <- stats::median(qb_szn$qb_prior, na.rm = TRUE)
    }
    
    attach_side <- function(gdf, side = c("home","away")) {
      side <- match.arg(side)
      key_team <- rlang::sym(paste0(side, "_team"))
      
      base <- gdf %>%
        dplyr::left_join(
          starters_ids,
          by = dplyr::join_by(season == season, week == week, !!key_team == team),
          relationship = "many-to-many"
        ) %>%
        dplyr::mutate(gsis_id = as.character(gsis_id),
                      prev_season = season - 1L)
      
      if (has_season) {
        base <- base %>%
          dplyr::left_join(qb_szn %>% dplyr::rename(prev_season = season),
                           by = c("prev_season","gsis_id"),
                           relationship = "many-to-many") %>%
          dplyr::left_join(szn_meds %>% dplyr::rename(prev_season = season),
                           by = "prev_season")
      } else {
        base <- base %>%
          dplyr::left_join(qb_szn, by = "gsis_id", relationship = "many-to-many") %>%
          dplyr::mutate(season_median_prior = overall_med_prior)
      }
      
      # inside attach_side()
      base %>%
        dplyr::group_by(game_id, season, season_type, week) %>%
        dplyr::summarise(
          # raw previous-season prior if available for the named starter
          qb_prior_raw = {
            v <- suppressWarnings(max(qb_prior, na.rm = TRUE))
            if (!is.finite(v)) NA_real_ else v
          },
          qb_prior_fill = dplyr::coalesce(first(season_median_prior), overall_med_prior),
          .groups = "drop"
        ) %>%
        dplyr::mutate(
          is_prior_game = (season_type != "REG") | (season_type == "REG" & week == 1L),
          !!paste0(side, "_qb_prior") := dplyr::if_else(
            is_prior_game,
            dplyr::coalesce(qb_prior_raw, qb_prior_fill),
            NA_real_
          ),
          !!paste0(side, "_qb_prior_imputed") := dplyr::if_else(
            is_prior_game,
            as.integer(is.na(qb_prior_raw)),
            0L
          ),
          !!paste0("used_prior_", side) := as.integer(is_prior_game)
        ) %>%
        dplyr::select(
          game_id,
          dplyr::all_of(paste0(side, "_qb_prior")),
          dplyr::all_of(paste0(side, "_qb_prior_imputed")),
          dplyr::all_of(paste0("used_prior_", side))
        )
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
    select(-is_dome) %>%
    mutate(surface = ifelse(surface == '', "a_turf", surface)) %>%
    mutate(
      margin       = as.numeric(home_score) - as.numeric(away_score),
      total_points = as.numeric(home_score) + as.numeric(away_score),
      home_win = dplyr::case_when(
        home_score > away_score ~ 1L,
        home_score < away_score ~ 0L,
        TRUE ~ NA_integer_
      )
    )
  
  out <- out %>%
    rename(
      home_pt_diff_prior = home_pt_diff_pg_prior,
      away_pt_diff_prior = away_pt_diff_pg_prior,
      diff_pt_diff_prior = diff_pt_diff_pg_prior
    ) %>%
    mutate(
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
finalize_targets_and_clean <- function(df, push_as_met = TRUE) {
  stopifnot(all(c("home_score","away_score","spread_line") %in% names(df)))
  suppressPackageStartupMessages({ library(dplyr) })
  
  df0 <- df %>%
    select(-matches("(^reg_last_week\\.|^n_prior\\.|\\.(x|y)(\\.(x|y))*$)"))
  
  df0 <- df0 %>%
    mutate(
      margin       = home_score - away_score,
      total_points = home_score + away_score
    )
  
  co <- suppressWarnings(stats::cor(df0$spread_line, df0$margin, use = "complete.obs"))
  spread_home <- if (!is.na(co) && is.finite(co) && co >= 0) df0$spread_line else -df0$spread_line
  df0 <- mutate(df0, spread_home = spread_home)
  
  df0 <- df0 %>%
    mutate(
      spread_covered = if (isTRUE(push_as_met)) {
        as.integer((margin - spread_home) >= 0)
      } else {
        as.integer((margin - spread_home) > 0)
      }
    )
  
  df0 %>%
    relocate(home_win, margin, spread_covered, total_points, .after = dplyr::last_col())
}

