#' Plot Top N Player Weekly Stat Trajectories
#'
#' Create a ggplot showing weekly or cumulative stat trajectories
#' for the top N players in a given season, position, and stat.
#'
#' @param data A data frame like `player_weekly` with columns:
#'   `player_id`, `name`, `team`, `season`, `season_type`,
#'   `week`, `position`, `stat_type`, `stat_name`, `value`,
#'   `team_color`, and `team_color2`.
#' @param season_choice Season (e.g., 2023).
#' @param season_type_choice Season type ("REG" or "POST").
#' @param stat_choice Stat name (e.g., `"passing_yards"`).
#' @param position_choice Position filter (e.g., `"QB"`).
#' @param top_n Number of top players to display.
#' @param week_range Weeks to include (default 1:18).
#' @param base_cumulative Stat type (default `"base"`).
#' @param cumulative_mode If TRUE, plot cumulative totals; if FALSE, weekly values.
#'
#' @return A ggplot object.
#' @export
plot_player_weekly_trajectory <- function(data,
                                          season_choice,
                                          season_type_choice = "REG",
                                          stat_choice,
                                          position_choice = NULL,
                                          top_n = 5,
                                          week_range = 1:18,
                                          base_cumulative = "base",
                                          cumulative_mode = TRUE) {
  # --- Identify top N players ---
  top_players <- data %>%
    dplyr::filter(season == !!season_choice,
                  season_type == !!season_type_choice,
                  stat_name == !!stat_choice,
                  stat_type == !!base_cumulative,
                  is.null(position_choice) | position == !!position_choice) %>%
    dplyr::group_by(player_id, name, team) %>%
    dplyr::summarise(total_value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(total_value)) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::pull(player_id)
  
  # --- Filter for those players and weeks ---
  plot_data <- data %>%
    dplyr::filter(season == !!season_choice,
                  season_type == !!season_type_choice,
                  stat_name == !!stat_choice,
                  stat_type == !!base_cumulative,
                  (!is.null(position_choice) & position == !!position_choice) | is.null(position_choice),
                  player_id %in% top_players,
                  week %in% week_range) %>%
    dplyr::group_by(player_id) %>%
    dplyr::arrange(week, .by_group = TRUE) %>%
    dplyr::mutate(value = if (cumulative_mode) cumsum(value) else value)
  
  # --- Clean y-axis label ---
  stat_label <- tools::toTitleCase(gsub("_", " ", stat_choice))
  
  # --- Build plot ---
  p <- ggplot2::ggplot(plot_data,
                       ggplot2::aes(x = week, y = value,
                                    group = player_id,
                                    color = I(team_color))) +
    ggplot2::geom_line(size = 1.3, alpha = 0.9) +
    ggplot2::geom_point(aes(fill = I(team_color2)),
                        size = 2.8, stroke = 0.2, shape = 21, color = "black") +
    ggrepel::geom_text_repel(
      data = plot_data %>% dplyr::filter(week == max(week)),
      ggplot2::aes(x = week, y = value, label = name),
      nudge_x = 0.5,
      hjust = 0,
      size = 3.5,
      segment.color = "grey70",
      color = "black",
      inherit.aes = FALSE
    ) +
    ggplot2::scale_x_continuous(breaks = scales::pretty_breaks()) +
    ggplot2::scale_y_continuous(labels = scales::comma) +
    ggplot2::labs(
      title = paste0("Top ", top_n, " ", stat_label, " — ", season_choice, " ", season_type_choice),
      subtitle = if (cumulative_mode) "Cumulative totals by week" else "Weekly values",
      x = "Week of Season",
      y = if (cumulative_mode) paste("Cumulative", stat_label) else stat_label,
      caption = "Data: player_weekly | Colors: Team colors"
    ) +
    ggplot2::theme_light(base_size = 14) +
    ggplot2::theme(
      axis.title.y = ggplot2::element_text(margin = ggplot2::margin(r = 10)),
      axis.title.x = ggplot2::element_text(margin = ggplot2::margin(t = 10)),
      plot.title = ggplot2::element_text(face = "bold", size = 18, hjust = 0),
      plot.subtitle = ggplot2::element_text(size = 12, margin = ggplot2::margin(b = 10)),
      plot.caption = ggplot2::element_text(size = 9, color = "grey50", hjust = 1),
      legend.position = "none"
    ) +
    ggplot2::coord_cartesian(clip = "off")
  
  return(p)
}

#' Plot Player Consistency vs. Volatility (Violin Distributions)
#'
#' Violin plots of weekly stat distributions per player for a selected
#' *multi-season* window, season type, stat, and week window. Limits to
#' Top N players by total `value` (of `stat_choice`) over the pooled window.
#'
#' @param data A data frame like `player_weekly` with columns:
#'   `player_id`, `name`, `team`, `season`, `season_type`,
#'   `week`, `position`, `stat_type`, `stat_name`, `value`,
#'   `team_color`, and `team_color2`.
#' @param season_choice Integer or integer vector of seasons (e.g., 2023 or 2023:2024).
#' @param season_type_choice "REG", "POST", or "ALL" (ALL = both; no preseason).
#' @param stat_choice Stat name (e.g., "passing_yards").
#' @param position_choice Optional position filter (e.g., "QB"). If NULL, use all positions.
#' @param top_n Number of top players to display (by total `value` over the window).
#' @param week_range Integer vector of weeks to include (e.g., 1:18). Applied within each season.
#' @param base_cumulative Stat type (default "base").
#' @param order_by Ordering of players along x-axis; one of "rCV", "IQR", "median".
#'   - "rCV" = robust coefficient of variation = MAD / median (ascending; lower = steadier)
#'   - "IQR" = interquartile range (ascending; tighter = steadier)
#'   - "median" = median value (descending; larger = left)
#' @param show_points If TRUE, overlay jittered weekly points (filled with `team_color2`).
#' @param min_games_for_badges Minimum games required to be considered in top/bottom consistency badges.
#'
#' @return A ggplot object.
#' @export
plot_player_consistency_violin <- function(data,
                                           season_choice,
                                           season_type_choice = "REG",
                                           stat_choice,
                                           position_choice = NULL,
                                           top_n = 5,
                                           week_range = 1:18,
                                           base_cumulative = "base",
                                           order_by = c("rCV", "IQR", "median"),
                                           show_points = FALSE,
                                           min_games_for_badges = 6) {
  order_by <- match.arg(order_by)
  
  # pretty season label (e.g., "2023–2024" if consecutive; else comma-separated)
  .season_label <- function(s) {
    u <- sort(unique(s))
    if (length(u) <= 1) return(as.character(u))
    if (max(u) - min(u) + 1 == length(u)) paste0(min(u), "–", max(u)) else paste(u, collapse = ", ")
  }
  
  # ---- Early filter (multi-season, season type, stat, stat_type, position, weeks) ----
  df_filt <- data %>%
    dplyr::filter(
      season %in% !!season_choice,
      (season_type_choice == "ALL") | (season_type == !!season_type_choice),
      stat_name == !!stat_choice,
      stat_type == !!base_cumulative,
      week %in% !!week_range,
      is.null(position_choice) | position == !!position_choice
    )
  
  # ---- Identify Top N players by total over selected seasons+weeks ----
  top_players <- df_filt %>%
    dplyr::group_by(player_id, name) %>%
    dplyr::summarise(total_value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(total_value)) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::pull(player_id)
  
  # ---- Data to plot: only Top N; exclude NA weekly values (keep zeros) ----
  plot_data <- df_filt %>%
    dplyr::filter(player_id %in% !!top_players, !is.na(value))
  
  # If nothing to plot, return empty plot with message
  if (nrow(plot_data) == 0) {
    return(
      ggplot2::ggplot() +
        ggplot2::labs(
          title = "No data to plot",
          subtitle = "Check filters: seasons, season_type, stat, position, week_range"
        ) +
        ggplot2::theme_void(base_size = 14)
    )
  }
  
  # ---- Dominant team color per player (mode across pooled window) ----
  dominant_team <- plot_data %>%
    dplyr::count(player_id, team, team_color, sort = TRUE) %>%
    dplyr::group_by(player_id) %>%
    dplyr::slice_head(n = 1) %>%
    dplyr::ungroup() %>%
    dplyr::transmute(
      player_id,
      team_mode = team,
      team_color_major = team_color
    )
  
  # ---- Per-player distribution summaries (median, IQR, MAD, rCV, n) ----
  player_summ <- plot_data %>%
    dplyr::group_by(player_id, name) %>%
    dplyr::summarise(
      n_games = sum(!is.na(value)),
      q50 = stats::median(value, na.rm = TRUE),
      q25 = stats::quantile(value, 0.25, na.rm = TRUE, names = FALSE, type = 7),
      q75 = stats::quantile(value, 0.75, na.rm = TRUE, names = FALSE, type = 7),
      IQR = q75 - q25,
      MAD = stats::mad(value, constant = 1, na.rm = TRUE), # unscaled MAD
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      rCV = dplyr::if_else(q50 == 0, NA_real_, MAD / abs(q50))
    ) %>%
    dplyr::left_join(dominant_team, by = "player_id")
  
  # ---- Determine ordering ----
  ord_df <- player_summ %>%
    dplyr::mutate(
      order_metric = dplyr::case_when(
        order_by == "median" ~ -q50,                               # desc
        order_by == "IQR" ~ IQR,                                   # asc
        TRUE ~ dplyr::if_else(is.na(rCV) | !is.finite(rCV), Inf, rCV) # rCV asc
      )
    ) %>%
    dplyr::arrange(order_metric)
  
  player_levels <- ord_df$player_id
  
  # Axis labels with n
  axis_labels <- setNames(
    paste0(player_summ$name[match(player_levels, player_summ$player_id)],
           "\n(n=", player_summ$n_games[match(player_levels, player_summ$player_id)], ")"),
    player_levels
  )
  
  # ---- Mark small-n players ----
  player_summ <- player_summ %>%
    dplyr::mutate(
      small_n = n_games < min_games_for_badges
    )
  
  # ---- Join summaries into plot_data for aesthetics, build factor ----
  plot_data <- plot_data %>%
    dplyr::left_join(
      player_summ %>% dplyr::select(player_id, team_color_major, small_n),
      by = "player_id"
    ) %>%
    dplyr::mutate(
      player_factor = factor(player_id, levels = player_levels)
    )
  
  iqr_df <- player_summ %>%
    dplyr::mutate(
      player_factor = factor(player_id, levels = player_levels)
    )
  
  # ---- Build badges (top/bottom 3 by rCV among adequate samples) ----
  badge_pool <- player_summ %>%
    dplyr::filter(!small_n, !is.na(rCV), is.finite(rCV))
  
  most_consistent <- if (nrow(badge_pool) > 0) {
    badge_pool %>%
      dplyr::arrange(rCV) %>%
      dplyr::slice_head(n = 3) %>%
      dplyr::pull(name) %>%
      paste(collapse = ", ")
  } else "—"
  
  most_volatile <- if (nrow(badge_pool) > 0) {
    badge_pool %>%
      dplyr::arrange(dplyr::desc(rCV)) %>%
      dplyr::slice_head(n = 3) %>%
      dplyr::pull(name) %>%
      paste(collapse = ", ")
  } else "—"
  
  stat_label  <- tools::toTitleCase(gsub("_", " ", stat_choice))
  week_text   <- paste0("Weeks ", paste0(range(week_range), collapse = "–"))
  season_text <- .season_label(season_choice)
  type_text   <- if (season_type_choice == "ALL") "REG+POST" else season_type_choice
  
  # ---- Plot ----
  p <- ggplot2::ggplot(plot_data, ggplot2::aes(x = player_factor, y = value)) +
    # Violins: outline in dominant team color; dim if small-n
    ggplot2::geom_violin(
      ggplot2::aes(color = I(team_color_major), alpha = small_n, group = player_factor),
      fill = NA, linewidth = 1.1, trim = FALSE
    ) +
    ggplot2::scale_alpha_manual(values = c(`TRUE` = 0.45, `FALSE` = 1), guide = "none") +
    
    # IQR per player (thick line) in dominant team color
    ggplot2::geom_linerange(
      data = iqr_df,
      ggplot2::aes(x = player_factor, ymin = q25, ymax = q75, color = I(team_color_major)),
      linewidth = 2, inherit.aes = FALSE
    ) +
    # Median tick (point) in dominant team color
    ggplot2::geom_point(
      data = iqr_df,
      ggplot2::aes(x = player_factor, y = q50, color = I(team_color_major)),
      size = 2.4, inherit.aes = FALSE
    ) +
    
    # Optional jittered weekly points: fill = team_color2, black stroke
    { if (isTRUE(show_points)) ggplot2::geom_jitter(
      ggplot2::aes(fill = I(team_color2)),
      width = 0.18, height = 0, shape = 21, size = 1.9, stroke = 0.25, color = "black", alpha = 0.65
    ) } +
    
    ggplot2::scale_y_continuous(labels = scales::comma) +
    ggplot2::scale_x_discrete(labels = axis_labels) +
    ggplot2::labs(
      title = paste0("Top ", top_n, " ", stat_label, " — ", season_text, " (", type_text, ")"),
      subtitle = paste0(
        week_text, "  •  Order by ", order_by,
        "  •  Most consistent: ", most_consistent,
        "  •  Most volatile: ", most_volatile
      ),
      x = NULL, y = stat_label,
      caption = "Distributions across selected seasons & weeks (pooled).  NA weeks excluded.  Lines in team_color; points (if shown) in team_color2."
    ) +
    ggplot2::theme_light(base_size = 14) +
    ggplot2::theme(
      axis.text.x = ggplot2::element_text(angle = 28, hjust = 1, vjust = 1),
      axis.title.y = ggplot2::element_text(margin = ggplot2::margin(r = 10)),
      plot.title = ggplot2::element_text(face = "bold", size = 18, hjust = 0),
      plot.subtitle = ggplot2::element_text(size = 11, margin = ggplot2::margin(b = 8)),
      plot.caption = ggplot2::element_text(size = 9, color = "grey50", hjust = 1),
      legend.position = "none"
    ) +
    ggplot2::coord_cartesian(clip = "off")
  
  return(p)
}

#' Plot Workload vs. Efficiency — Aggregated (one point per player)
#' Labels every point by default (label_all_points = TRUE).
plot_workload_efficiency_summary <- function(
    data,
    season_choice,
    season_type_choice = "REG",
    week_range = 1:18,
    position_choice,
    metric_choice,
    top_n = 10,
    players = NULL,
    base_cumulative = "base",
    workload_floor = NULL,
    label_all_points = TRUE,   # <-- NEW: label all players
    label_outliers = TRUE,     # kept for flexibility when label_all_points = FALSE
    k_outliers = 5,
    log_x = FALSE
) {
  `%||%` <- function(a, b) if (is.null(a)) b else a
  .metric_config <- function(metric, position) {
    switch(metric,
           "passing_epa_per_dropback" = list(
             required_stats = c("attempts","sacks","passing_epa","passing_yards"),
             denom_fun = function(df) (df$attempts %||% 0) + (df$sacks %||% 0),
             num_fun   = function(df) (df$passing_epa %||% 0),
             gate_fun  = function(df) (df$passing_epa %||% 0),
             x_lab = "Dropbacks (attempts + sacks)",
             y_lab = "EPA per dropback",
             title_lab = "Passing EPA per Dropback",
             default_floor = 10
           ),
           "passing_anya" = list(
             required_stats = c("attempts","sacks","sack_yards","passing_yards","passing_tds","interceptions"),
             denom_fun = function(df) (df$attempts %||% 0) + (df$sacks %||% 0),
             num_fun   = function(df) (df$passing_yards %||% 0) + 20*(df$passing_tds %||% 0) -
               45*(df$interceptions %||% 0) - (df$sack_yards %||% 0),
             gate_fun  = function(df) (df$passing_yards %||% 0),
             x_lab = "Attempts + Sacks",
             y_lab = "ANY/A",
             title_lab = "Adjusted Net Yards per Attempt (ANY/A)",
             default_floor = 10
           ),
           "passing_ypa" = list(
             required_stats = c("attempts","passing_yards"),
             denom_fun = function(df) (df$attempts %||% 0),
             num_fun   = function(df) (df$passing_yards %||% 0),
             gate_fun  = function(df) (df$passing_yards %||% 0),
             x_lab = "Attempts",
             y_lab = "Yards per Attempt",
             title_lab = "Passing Yards per Attempt",
             default_floor = 3
           ),
           "rushing_epa_per_carry" = list(
             required_stats = c("carries","rushing_epa","rushing_yards"),
             denom_fun = function(df) (df$carries %||% 0),
             num_fun   = function(df) (df$rushing_epa %||% 0),
             gate_fun  = function(df) (df$rushing_epa %||% 0),
             x_lab = "Carries",
             y_lab = "EPA per Rush",
             title_lab = "Rushing EPA per Carry",
             default_floor = 5
           ),
           "rushing_ypc" = list(
             required_stats = c("carries","rushing_yards"),
             denom_fun = function(df) (df$carries %||% 0),
             num_fun   = function(df) (df$rushing_yards %||% 0),
             gate_fun  = function(df) (df$rushing_yards %||% 0),
             x_lab = "Carries",
             y_lab = "Yards per Carry",
             title_lab = "Rushing Yards per Carry",
             default_floor = 5
           ),
           "receiving_epa_per_target" = list(
             required_stats = c("targets","receiving_epa","receiving_yards"),
             denom_fun = function(df) (df$targets %||% 0),
             num_fun   = function(df) (df$receiving_epa %||% 0),
             gate_fun  = function(df) (df$receiving_epa %||% 0),
             x_lab = "Targets",
             y_lab = "EPA per Target",
             title_lab = "Receiving EPA per Target",
             default_floor = 3
           ),
           "receiving_ypt" = list(
             required_stats = c("targets","receiving_yards"),
             denom_fun = function(df) (df$targets %||% 0),
             num_fun   = function(df) (df$receiving_yards %||% 0),
             gate_fun  = function(df) (df$receiving_yards %||% 0),
             x_lab = "Targets",
             y_lab = "Yards per Target",
             title_lab = "Receiving Yards per Target",
             default_floor = 3
           ),
           "receiving_ypr" = list(
             required_stats = c("receptions","receiving_yards"),
             denom_fun = function(df) (df$receptions %||% 0),
             num_fun   = function(df) (df$receiving_yards %||% 0),
             gate_fun  = function(df) (df$receiving_yards %||% 0),
             x_lab = "Receptions",
             y_lab = "Yards per Reception",
             title_lab = "Receiving Yards per Reception",
             default_floor = 3
           ),
           "total_epa_per_opportunity" = list(
             required_stats = c("attempts","sacks","carries","targets",
                                "passing_epa","rushing_epa","receiving_epa",
                                "passing_yards","rushing_yards","receiving_yards",
                                "passing_tds","rushing_tds","receiving_tds"),
             denom_fun = function(df) if (position_choice == "QB")
               ((df$attempts %||% 0) + (df$sacks %||% 0)) + (df$carries %||% 0)
             else
               (df$targets %||% 0) + (df$carries %||% 0),
             num_fun = function(df) if (position_choice == "QB")
               (df$passing_epa %||% 0) + (df$rushing_epa %||% 0)
             else
               (df$receiving_epa %||% 0) + (df$rushing_epa %||% 0),
             gate_fun = function(df) if (position_choice == "QB")
               (df$passing_epa %||% 0) + (df$rushing_epa %||% 0)
             else
               (df$receiving_epa %||% 0) + (df$rushing_epa %||% 0),
             x_lab = "Opportunities",
             y_lab = "Total EPA per Opportunity",
             title_lab = "Total EPA per Opportunity",
             default_floor = 5
           ),
           "yards_per_opportunity" = list(
             required_stats = c("attempts","sacks","carries","targets",
                                "passing_yards","rushing_yards","receiving_yards"),
             denom_fun = function(df) if (position_choice == "QB")
               ((df$attempts %||% 0) + (df$sacks %||% 0)) + (df$carries %||% 0)
             else
               (df$targets %||% 0) + (df$carries %||% 0),
             num_fun = function(df) if (position_choice == "QB")
               (df$passing_yards %||% 0) + (df$rushing_yards %||% 0)
             else
               (df$receiving_yards %||% 0) + (df$rushing_yards %||% 0),
             gate_fun = function(df) if (position_choice == "QB")
               (df$passing_yards %||% 0) + (df$rushing_yards %||% 0)
             else
               (df$receiving_yards %||% 0) + (df$rushing_yards %||% 0),
             x_lab = "Opportunities",
             y_lab = "Yards per Opportunity",
             title_lab = "Yards per Opportunity",
             default_floor = 5
           ),
           "td_rate_per_opportunity" = list(
             required_stats = c("attempts","sacks","carries","targets",
                                "passing_tds","rushing_tds","receiving_tds"),
             denom_fun = function(df) if (position_choice == "QB")
               ((df$attempts %||% 0) + (df$sacks %||% 0)) + (df$carries %||% 0)
             else
               (df$targets %||% 0) + (df$carries %||% 0),
             num_fun = function(df) if (position_choice == "QB")
               (df$passing_tds %||% 0) + (df$rushing_tds %||% 0)
             else
               (df$receiving_tds %||% 0) + (df$rushing_tds %||% 0),
             gate_fun = function(df) if (position_choice == "QB")
               (df$passing_tds %||% 0) + (df$rushing_tds %||% 0)
             else
               (df$receiving_tds %||% 0) + (df$rushing_tds %||% 0),
             x_lab = "Opportunities",
             y_lab = "TD Rate per Opportunity",
             title_lab = "TD Rate per Opportunity",
             default_floor = 5
           ),
           "receiving_epa_per_opportunity" = list(
             required_stats = c("targets","carries","receiving_epa"),
             denom_fun = function(df) (df$targets %||% 0) + (df$carries %||% 0),
             num_fun   = function(df) (df$receiving_epa %||% 0),
             gate_fun  = function(df) (df$receiving_epa %||% 0),
             x_lab = "Opportunities (targets + carries)",
             y_lab = "Receiving EPA per Opportunity",
             title_lab = "Receiving EPA per Opportunity",
             default_floor = 5
           ),
           { stop(sprintf("Unknown metric_choice: '%s'", metric)) }
    )
  }
  
  # Filter
  df <- data %>%
    dplyr::filter(
      season == !!season_choice,
      season_type == !!season_type_choice,
      stat_type == !!base_cumulative,
      week %in% !!week_range,
      position == !!position_choice
    )
  if (!is.null(players)) df <- df %>% dplyr::filter(player_id %in% !!players)
  
  cfg <- .metric_config(metric_choice, position_choice)
  
  df_needed <- df %>%
    dplyr::filter(stat_name %in% cfg$required_stats) %>%
    dplyr::select(player_id, name, team, position, season, season_type, week,
                  team_color, team_color2, stat_name, value)
  
  if (nrow(df_needed) == 0) {
    return(ggplot2::ggplot() +
             ggplot2::labs(
               title = paste0("Workload vs. Efficiency — ", cfg$title_lab),
               subtitle = paste0(position_choice, " • ", season_choice, " ", season_type_choice,
                                 " • Weeks ", paste(range(week_range), collapse = "–"),
                                 " • No rows for this metric/filters."),
               x = cfg$x_lab, y = cfg$y_lab) +
             ggplot2::theme_void(base_size = 14))
  }
  
  wide <- df_needed %>%
    tidyr::pivot_wider(
      id_cols = c(player_id, name, team, position, season, season_type, week, team_color, team_color2),
      names_from = stat_name,
      values_from = value,
      values_fill = 0
    )
  
  # Row-wise pieces
  wide$._den  <- cfg$denom_fun(wide)
  wide$._num  <- cfg$num_fun(wide)
  wide$._gate <- cfg$gate_fun(wide)
  
  # Aggregate to one point per player
  agg <- wide %>%
    dplyr::group_by(player_id, name, team, team_color, team_color2) %>%
    dplyr::summarise(
      workload  = sum(._den,  na.rm = TRUE),
      numerator = sum(._num,  na.rm = TRUE),
      gate_total= sum(._gate, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::mutate(efficiency = dplyr::if_else(workload > 0, numerator / workload, NA_real_)) %>%
    dplyr::filter(!is.na(efficiency), is.finite(efficiency))
  
  if (is.null(workload_floor)) workload_floor <- cfg$default_floor
  agg <- agg %>% dplyr::filter(workload >= workload_floor)
  
  if (nrow(agg) == 0) {
    return(ggplot2::ggplot() +
             ggplot2::labs(
               title = paste0("Workload vs. Efficiency — ", cfg$title_lab),
               subtitle = paste0(position_choice, " • ", season_choice, " ", season_type_choice,
                                 " • Weeks ", paste(range(week_range), collapse = "–"),
                                 " • No valid players (denominator zero/under floor)."),
               x = cfg$x_lab, y = cfg$y_lab) +
             ggplot2::theme_void(base_size = 14))
  }
  
  # Top-N gating
  top_ids <- agg %>%
    dplyr::arrange(dplyr::desc(gate_total)) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::pull(player_id)
  agg <- agg %>% dplyr::filter(player_id %in% !!top_ids)
  
  if (nrow(agg) == 0) {
    return(ggplot2::ggplot() +
             ggplot2::labs(
               title = paste0("Workload vs. Efficiency — ", cfg$title_lab),
               subtitle = paste0(position_choice, " • ", season_choice, " ", season_type_choice,
                                 " • Weeks ", paste(range(week_range), collapse = "–"),
                                 " • No points after Top-", top_n, " selection."),
               x = cfg$x_lab, y = cfg$y_lab) +
             ggplot2::theme_void(base_size = 14))
  }
  
  # Quadrant medians
  med_x <- stats::median(agg$workload,   na.rm = TRUE)
  med_y <- stats::median(agg$efficiency, na.rm = TRUE)
  
  # Label data
  if (isTRUE(label_all_points)) {
    label_df <- agg %>% dplyr::transmute(x = workload, y = efficiency, label = name)
  } else if (isTRUE(label_outliers) && k_outliers > 0) {
    label_df <- agg %>%
      dplyr::filter(workload >= med_x) %>%
      dplyr::mutate(dev = abs(efficiency - med_y)) %>%
      dplyr::arrange(dplyr::desc(dev)) %>%
      dplyr::slice_head(n = k_outliers) %>%
      dplyr::transmute(x = workload, y = efficiency, label = name)
  } else {
    label_df <- dplyr::tibble()
  }
  
  week_text <- paste0("Weeks ", paste(range(week_range), collapse = "–"))
  
  p <- ggplot2::ggplot(agg, ggplot2::aes(x = workload, y = efficiency)) +
    ggplot2::geom_vline(xintercept = med_x, linetype = "dashed", linewidth = 0.4, color = "grey55") +
    ggplot2::geom_hline(yintercept = med_y, linetype = "dashed", linewidth = 0.4, color = "grey55") +
    ggplot2::geom_point(
      ggplot2::aes(color = I(team_color), fill = I(team_color2)),
      size = 3.2, shape = 21, stroke = 0.5, alpha = 0.9
    ) +
    { if (nrow(label_df) > 0) ggrepel::geom_text_repel(
      data = label_df,
      ggplot2::aes(x = x, y = y, label = label),
      size = 3.2, max.overlaps = 1000, box.padding = 0.25, point.padding = 0.15,
      min.segment.length = 0.05
    ) } +
    { if (isTRUE(log_x)) ggplot2::scale_x_continuous(trans = "log10") } +
    ggplot2::labs(
      title = paste0("Workload vs. Efficiency — ", cfg$title_lab),
      subtitle = paste0(position_choice, " • ", season_choice, " ", season_type_choice,
                        " • ", week_text,
                        " • Top ", top_n, " by total value • Medians shown"),
      x = cfg$x_lab, y = cfg$y_lab,
      caption = "One point per player.  Y = ratio of sums over the window.  Points filled with team_color2; outline = team_color."
    ) +
    ggplot2::theme_light(base_size = 14) +
    ggplot2::theme(
      plot.title    = ggplot2::element_text(face = "bold", size = 18, hjust = 0),
      plot.subtitle = ggplot2::element_text(size = 11, margin = ggplot2::margin(b = 8)),
      plot.caption  = ggplot2::element_text(size = 9, color = "grey50", hjust = 1),
      legend.position = "none"
    )
  
  return(p)
}

#' Player Scatter with Quadrants (Generalized)
#'
#' One point per player over a selected window, plotting any two metrics.
#' Uses median x/y guides, equal aspect (square), and team colors.
#'
#' @param data long table like `player_weekly` with columns:
#'   player_id, name, team, season, season_type, week, position,
#'   stat_type, stat_name, value, team_color, team_color2
#' @param season_choice integer or integer vector (e.g., 1999:2024)
#' @param season_type_choice "REG","POST","ALL" (ALL includes both)
#' @param week_range integer vector of weeks (e.g., 1:18)
#' @param position_choice "QB","RB","WR","TE"
#' @param top_n number of players to display (ranked by combined gate totals)
#' @param metric_x string metric id (e.g., "targets", "receiving_ypt", "passing_epa_per_dropback")
#' @param metric_y string metric id (same style as metric_x)
#' @param label_all_points logical; label every point with player name
#' @param log_x logical; log10 x-scale (drops non-positive)
#' @param log_y logical; log10 y-scale (drops non-positive)
#'
#' @return ggplot object
plot_player_scatter_quadrants <- function(
    data,
    season_choice,
    season_type_choice = "REG",
    week_range = 1:18,
    position_choice,
    top_n = 20,
    metric_x,
    metric_y,
    label_all_points = TRUE,
    log_x = FALSE,
    log_y = FALSE,
    top_by = c("combined","x_gate","y_gate","x_value","y_value")  # NEW
) {
  `%||%` <- function(a, b) if (is.null(a)) b else a
  top_by <- match.arg(top_by)
  
  # --- Metric config (derived rates and raw sum fallback) ---
  .metric_config <- function(metric, position_choice) {
    derived <- list(
      passing_epa_per_dropback = list(
        kind="rate",
        required_stats=c("attempts","sacks","passing_epa"),
        denom_fun=function(df) (df$attempts %||% 0) + (df$sacks %||% 0),
        num_fun=function(df) (df$passing_epa %||% 0),
        gate_fun=function(df) (df$passing_epa %||% 0),
        label="EPA per Dropback"
      ),
      passing_anya = list(
        kind="rate",
        required_stats=c("attempts","sacks","sack_yards","passing_yards","passing_tds","interceptions"),
        denom_fun=function(df) (df$attempts %||% 0) + (df$sacks %||% 0),
        num_fun=function(df) (df$passing_yards %||% 0) + 20*(df$passing_tds %||% 0) -
          45*(df$interceptions %||% 0) - (df$sack_yards %||% 0),
        gate_fun=function(df) (df$passing_yards %||% 0),
        label="ANY/A"
      ),
      rushing_epa_per_carry = list(
        kind="rate",
        required_stats=c("carries","rushing_epa"),
        denom_fun=function(df) (df$carries %||% 0),
        num_fun=function(df) (df$rushing_epa %||% 0),
        gate_fun=function(df) (df$rushing_epa %||% 0),
        label="EPA per Rush"
      ),
      receiving_epa_per_target = list(
        kind="rate",
        required_stats=c("targets","receiving_epa"),
        denom_fun=function(df) (df$targets %||% 0),
        num_fun=function(df) (df$receiving_epa %||% 0),
        gate_fun=function(df) (df$receiving_epa %||% 0),
        label="EPA per Target"
      ),
      total_epa_per_opportunity = list(
        kind="rate",
        required_stats=c("attempts","sacks","carries","targets","passing_epa","rushing_epa","receiving_epa"),
        denom_fun=function(df) if (position_choice=="QB")
          ((df$attempts %||% 0)+(df$sacks %||% 0))+(df$carries %||% 0) else
            (df$targets %||% 0)+(df$carries %||% 0),
        num_fun=function(df) if (position_choice=="QB")
          (df$passing_epa %||% 0)+(df$rushing_epa %||% 0) else
            (df$receiving_epa %||% 0)+(df$rushing_epa %||% 0),
        gate_fun=function(df) if (position_choice=="QB")
          (df$passing_epa %||% 0)+(df$rushing_epa %||% 0) else
            (df$receiving_epa %||% 0)+(df$rushing_epa %||% 0),
        label="Total EPA per Opportunity"
      ),
      yards_per_opportunity = list(
        kind="rate",
        required_stats=c("attempts","sacks","carries","targets","passing_yards","rushing_yards","receiving_yards"),
        denom_fun=function(df) if (position_choice=="QB")
          ((df$attempts %||% 0)+(df$sacks %||% 0))+(df$carries %||% 0) else
            (df$targets %||% 0)+(df$carries %||% 0),
        num_fun=function(df) if (position_choice=="QB")
          (df$passing_yards %||% 0)+(df$rushing_yards %||% 0) else
            (df$receiving_yards %||% 0)+(df$rushing_yards %||% 0),
        gate_fun=function(df) if (position_choice=="QB")
          (df$passing_yards %||% 0)+(df$rushing_yards %||% 0) else
            (df$receiving_yards %||% 0)+(df$rushing_yards %||% 0),
        label="Yards per Opportunity"
      )
    )
    if (metric %in% names(derived)) return(derived[[metric]])
    list(kind="raw_sum",
         required_stats=c(metric),
         stat_col=metric,
         label=tools::toTitleCase(gsub("_"," ",metric)))
  }
  
  # --- Filter window ---
  df <- data %>%
    dplyr::filter(
      season %in% !!season_choice,
      (season_type_choice == "ALL") | (season_type == !!season_type_choice),
      week %in% !!week_range,
      position == !!position_choice,
      stat_type == "base"
    )
  if (nrow(df) == 0) {
    return(ggplot2::ggplot() +
             ggplot2::labs(
               title="Player Scatter with Quadrants",
               subtitle=paste0(position_choice," • ",
                               paste(range(week_range), collapse="–"),
                               " • ", paste(season_choice, collapse=", "),
                               " (", season_type_choice, ") — no rows"),
               x=metric_x, y=metric_y) +
             ggplot2::theme_void(base_size=14))
  }
  
  # --- Resolve metric configs ---
  cfg_x <- .metric_config(metric_x, position_choice)
  cfg_y <- .metric_config(metric_y, position_choice)
  req_stats <- unique(c(cfg_x$required_stats, cfg_y$required_stats))
  
  df_needed <- df %>%
    dplyr::filter(stat_name %in% req_stats) %>%
    dplyr::select(player_id, name, team, team_color, team_color2, stat_name, value)
  
  if (nrow(df_needed) == 0) {
    return(ggplot2::ggplot() +
             ggplot2::labs(
               title="Player Scatter with Quadrants",
               subtitle="Required stats not found for chosen metrics.",
               x=cfg_x$label, y=cfg_y$label) +
             ggplot2::theme_void(base_size=14))
  }
  
  # --- Aggregate to player-level sums per stat, then pivot wide ---
  wide <- df_needed %>%
    dplyr::group_by(player_id, name, team, team_color, team_color2, stat_name) %>%
    dplyr::summarise(total = sum(value, na.rm=TRUE), .groups="drop_last") %>%
    tidyr::pivot_wider(names_from=stat_name, values_from=total, values_fill=0) %>%
    dplyr::ungroup()
  
  # --- Compute metric values & gates ---
  compute_metric <- function(w, cfg) {
    if (identical(cfg$kind, "rate")) {
      den <- cfg$denom_fun(w); num <- cfg$num_fun(w); gate <- cfg$gate_fun(w)
      val <- ifelse(den > 0, num / den, NA_real_)
      list(value = val, gate = gate %||% 0)
    } else {
      v <- w[[cfg$stat_col]] %||% 0
      list(value = v, gate = abs(v))
    }
  }
  mx <- compute_metric(wide, cfg_x)
  my <- compute_metric(wide, cfg_y)
  
  agg <- wide %>%
    dplyr::mutate(
      x_value = mx$value, y_value = my$value,
      gate_x  = mx$gate,  gate_y  = my$gate
    ) %>%
    dplyr::filter(is.finite(x_value), is.finite(y_value))
  
  # Log-domain constraints
  if (isTRUE(log_x)) agg <- agg %>% dplyr::filter(x_value > 0)
  if (isTRUE(log_y)) agg <- agg %>% dplyr::filter(y_value > 0)
  if (nrow(agg) == 0) {
    return(ggplot2::ggplot() +
             ggplot2::labs(
               title="Player Scatter with Quadrants",
               subtitle="No valid points after metric/log filters.",
               x=cfg_x$label, y=cfg_y$label) +
             ggplot2::theme_void(base_size=14))
  }
  
  # --- Top-N selection by 'top_by' ---
  agg <- agg %>%
    dplyr::mutate(
      gate_total = (gate_x %||% 0) + (gate_y %||% 0),
      rank_key = dplyr::case_when(
        top_by == "combined" ~ gate_total,
        top_by == "x_gate"   ~ gate_x,
        top_by == "y_gate"   ~ gate_y,
        top_by == "x_value"  ~ x_value,
        top_by == "y_value"  ~ y_value
      )
    ) %>%
    dplyr::arrange(
      dplyr::desc(rank_key),
      dplyr::desc(gate_total),
      dplyr::desc(x_value + y_value),
      name
    ) %>%
    dplyr::slice_head(n = top_n)
  
  # Medians for quadrant guides
  med_x <- stats::median(agg$x_value, na.rm=TRUE)
  med_y <- stats::median(agg$y_value, na.rm=TRUE)
  
  # Labels
  label_df <- if (isTRUE(label_all_points)) {
    agg %>% dplyr::transmute(x = x_value, y = y_value, label = name)
  } else dplyr::tibble()
  
  week_text   <- paste0("Weeks ", paste(range(week_range), collapse="–"))
  season_text <- paste(season_choice, collapse=", ")
  
  # --- Plot ---
  p <- ggplot2::ggplot(agg, ggplot2::aes(x = x_value, y = y_value)) +
    ggplot2::geom_vline(xintercept = med_x, linetype="dashed", linewidth=0.4, color="grey55") +
    ggplot2::geom_hline(yintercept = med_y, linetype="dashed", linewidth=0.4, color="grey55") +
    ggplot2::geom_point(
      ggplot2::aes(color = I(team_color), fill = I(team_color2)),
      size = 3.2, shape = 21, stroke = 0.5, alpha = 0.9
    ) +
    { if (nrow(label_df) > 0) ggrepel::geom_text_repel(
      data = label_df,
      ggplot2::aes(x = x, y = y, label = label),
      size = 3.2, max.overlaps = 1000, box.padding = 0.25, point.padding = 0.15,
      min.segment.length = 0.05
    ) } +
    { if (isTRUE(log_x)) ggplot2::scale_x_continuous(trans="log10") } +
    { if (isTRUE(log_y)) ggplot2::scale_y_continuous(trans="log10") } +
    ggplot2::labs(
      title = paste0(cfg_x$label, " vs ", cfg_y$label),
      subtitle = paste0(position_choice, " • ", season_text, " (", season_type_choice, ") • ",
                        week_text, " • Top ", top_n, " by ", top_by,
                        " • Medians shown"),
      x = cfg_x$label, y = cfg_y$label,
      caption = "Each point = one player over the window.  Derived metrics are ratio-of-sums.  Points: fill=team_color2, outline=team_color."
    ) +
    ggplot2::theme_light(base_size = 14) +
    ggplot2::theme(
      plot.title    = ggplot2::element_text(face="bold", size=18, hjust=0),
      plot.subtitle = ggplot2::element_text(size=11, margin=ggplot2::margin(b=8)),
      plot.caption  = ggplot2::element_text(size=9, color="grey50", hjust=1),
      legend.position = "none",
      aspect.ratio = 1  # square panel, independent axes
    )
  
  return(p)
}

#' Plot Top N Team Weekly Stat Trajectories (alpha hotfix)
plot_team_time_series <- function(data,
                                  season_choice,
                                  season_type_choice = "REG",
                                  stat_choice,
                                  top_n = 10,
                                  week_range = 1:18,
                                  base_cumulative = "base",
                                  cumulative_mode = TRUE,
                                  highlight = NULL,           # c("DET","KC"), "ALL", or NULL
                                  facet_by_season = TRUE,
                                  label_last = TRUE,
                                  muted_alpha = 0.35) {
  
  season_types <- if (identical(season_type_choice, "ALL")) c("REG","POST") else season_type_choice
  
  base_df <- data %>%
    dplyr::filter(
      season %in% !!season_choice,
      season_type %in% season_types,
      stat_type == !!base_cumulative,
      stat_name == !!stat_choice,
      week %in% !!week_range
    )
  
  if (nrow(base_df) == 0) {
    stop("No rows after filtering. Check season_choice, season_type_choice, stat_choice, and week_range.")
  }
  
  # Top-N by total over the window (per season)
  top_tbl <- base_df %>%
    dplyr::group_by(season, team) %>%
    dplyr::summarise(total_value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(season, dplyr::desc(total_value), team) %>%
    dplyr::group_by(season) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::ungroup() %>%
    dplyr::select(season, team)
  
  plot_data <- base_df %>%
    dplyr::semi_join(top_tbl, by = c("season", "team")) %>%
    dplyr::group_by(season, team) %>%
    dplyr::arrange(week, .by_group = TRUE) %>%
    dplyr::mutate(value_plot = if (cumulative_mode) cumsum(value) else value) %>%
    dplyr::ungroup()
  
  # Ensure colors exist
  if (!"team_color" %in% names(plot_data))  plot_data$team_color  <- "#555555"
  if (!"team_color2" %in% names(plot_data)) plot_data$team_color2 <- "#CCCCCC"
  
  # Highlight flags
  highlight_all <- identical(highlight, "ALL")
  has_specific_highlights <- !is.null(highlight) && !highlight_all
  
  plot_data <- plot_data %>%
    dplyr::mutate(
      is_highlight = dplyr::case_when(
        highlight_all ~ TRUE,
        has_specific_highlights ~ team %in% highlight,
        TRUE ~ TRUE
      )
    )
  
  # Alpha values (branch outside mutate to avoid length mismatch)
  if (has_specific_highlights) {
    plot_data <- plot_data %>%
      dplyr::mutate(alpha_val = dplyr::if_else(is_highlight, 1, muted_alpha))
  } else {
    plot_data <- plot_data %>%
      dplyr::mutate(alpha_val = 1)
  }
  
  # Labels: last observed week per (season, team)
  label_data <- plot_data %>%
    dplyr::group_by(season, team) %>%
    dplyr::filter(week == max(week, na.rm = TRUE)) %>%
    dplyr::ungroup()
  
  if (!highlight_all && has_specific_highlights) {
    label_data <- dplyr::filter(label_data, team %in% highlight)
  }
  if (is.null(highlight)) {
    label_data <- label_data[0, ]
  }
  
  stat_label <- tools::toTitleCase(gsub("_", " ", stat_choice))
  
  p <- ggplot2::ggplot(
    plot_data,
    ggplot2::aes(x = week, y = value_plot, group = interaction(season, team))
  ) +
    ggplot2::geom_line(
      ggplot2::aes(color = I(team_color), alpha = alpha_val),
      size = 1.1
    ) +
    ggplot2::geom_point(
      ggplot2::aes(fill = I(team_color2), alpha = alpha_val),
      shape = 21, size = 2.6, stroke = 0.25, color = "black"
    ) +
    ggplot2::scale_alpha_identity() +
    ggplot2::scale_x_continuous(breaks = scales::pretty_breaks()) +
    ggplot2::scale_y_continuous(labels = scales::comma) +
    ggplot2::labs(
      title = paste0("Top ", top_n, " Teams — ", stat_label),
      subtitle = paste0(
        if (cumulative_mode) "Cumulative totals by week" else "Weekly values",
        " | Season", if (length(unique(plot_data$season)) > 1) "s: " else ": ",
        paste(sort(unique(plot_data$season)), collapse = ", "),
        " | ", paste0(season_types, collapse = "+")
      ),
      x = "Week of Season",
      y = if (cumulative_mode) paste("Cumulative", stat_label) else stat_label,
      caption = "Data: team_weekly | Colors: Team colors"
    ) +
    ggplot2::theme_light(base_size = 14) +
    ggplot2::theme(
      axis.title.y = ggplot2::element_text(margin = ggplot2::margin(r = 10)),
      axis.title.x = ggplot2::element_text(margin = ggplot2::margin(t = 10)),
      plot.title   = ggplot2::element_text(face = "bold", size = 18, hjust = 0),
      plot.subtitle= ggplot2::element_text(size = 12, margin = ggplot2::margin(b = 10)),
      plot.caption = ggplot2::element_text(size = 9, color = "grey50", hjust = 1),
      legend.position = "none"
    ) +
    ggplot2::coord_cartesian(clip = "off")
  
  if (label_last && nrow(label_data) > 0) {
    p <- p + ggrepel::geom_text_repel(
      data = label_data,
      ggplot2::aes(x = week, y = value_plot, label = team),
      nudge_x = 0.5,
      hjust = 0,
      size = 3.4,
      segment.color = "grey70",
      color = "black",
      inherit.aes = FALSE,
      max.overlaps = Inf,
      box.padding = 0.3,
      point.padding = 0.2
    )
  }
  
  if (facet_by_season && length(unique(plot_data$season)) > 1) {
    p <- p + ggplot2::facet_wrap(~ season, ncol = 1)
  }
  
  return(p)
}


#' Plot Team Consistency vs. Volatility (Violin Distributions)
#'
#' Violin plots of weekly stat distributions per team for a selected
#' *multi-season* window, season type, stat, and week window. Limits to
#' Top N teams by total `value` (of `stat_choice`) over the pooled window.
#'
#' @param data A data frame like `team_weekly` with columns:
#'   `team`, `season`, `season_type`, `week`,
#'   `opponent`, `stat_type`, `stat_name`, `value`,
#'   optional `game_id`, and recommended `team_color`, `team_color2`.
#' @param season_choice Integer or integer vector of seasons (e.g., 2024 or 2023:2024).
#' @param season_type_choice "REG", "POST", or "ALL" (ALL = both; no preseason). Default "REG".
#' @param stat_choice Stat name string (e.g., "passing_yards").
#' @param top_n Number of top teams to display (by total `value` over the pooled window). Default 10.
#' @param week_range Integer vector of weeks to include (applied within each season). Default 1:18.
#' @param base_cumulative Stat type filter (default "base").
#' @param order_by Ordering of teams along x-axis; one of "rCV", "IQR", "median".
#'   - "rCV" = robust coefficient of variation = MAD / median (ascending; lower = steadier)
#'   - "IQR" = interquartile range (ascending; tighter = steadier)
#'   - "median" = median value (descending; larger = left)
#' @param show_points If TRUE, overlay jittered weekly points (filled with `team_color2`). Default FALSE.
#' @param min_games_for_badges Minimum games required to be considered in consistency badges. Default 6.
#'
#' @return A ggplot object.
#' @export
plot_team_consistency_violin <- function(data,
                                         season_choice,
                                         season_type_choice = "REG",
                                         stat_choice,
                                         top_n = 10,
                                         week_range = 1:18,
                                         base_cumulative = "base",
                                         order_by = c("rCV", "IQR", "median"),
                                         show_points = FALSE,
                                         min_games_for_badges = 6) {
  order_by <- match.arg(order_by)
  
  # pretty season label (e.g., "2023–2024" if consecutive; else comma-separated)
  .season_label <- function(s) {
    u <- sort(unique(s))
    if (length(u) <= 1) return(as.character(u))
    if (max(u) - min(u) + 1 == length(u)) paste0(min(u), "–", max(u)) else paste(u, collapse = ", ")
  }
  
  # ---- Early filter (multi-season, season type, stat, stat_type, weeks) ----
  df_filt <- data %>%
    dplyr::filter(
      season %in% !!season_choice,
      (season_type_choice == "ALL") | (season_type == !!season_type_choice),
      stat_name == !!stat_choice,
      stat_type == !!base_cumulative,
      week %in% !!week_range
    )
  
  # ---- Identify Top N teams by total over selected seasons+weeks (pooled) ----
  top_teams <- df_filt %>%
    dplyr::group_by(team) %>%
    dplyr::summarise(total_value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(total_value), team) %>%   # deterministic tie-break by abbr
    dplyr::slice_head(n = top_n) %>%
    dplyr::pull(team)
  
  # ---- Data to plot: only Top N; exclude NA weekly values (keep zeros) ----
  plot_data <- df_filt %>%
    dplyr::filter(team %in% !!top_teams, !is.na(value))
  
  # If nothing to plot, return empty plot with message
  if (nrow(plot_data) == 0) {
    return(
      ggplot2::ggplot() +
        ggplot2::labs(
          title = "No data to plot",
          subtitle = "Check filters: seasons, season_type, stat, week_range"
        ) +
        ggplot2::theme_void(base_size = 14)
    )
  }
  
  # ---- Dominant team color per team (mode across pooled window) ----
  dominant_team <- plot_data %>%
    dplyr::count(team, team_color, sort = TRUE) %>%
    dplyr::group_by(team) %>%
    dplyr::slice_head(n = 1) %>%
    dplyr::ungroup() %>%
    dplyr::transmute(
      team,
      team_color_major = team_color
    )
  
  # ---- Per-team distribution summaries (median, IQR, MAD, rCV, n) ----
  team_summ <- plot_data %>%
    dplyr::group_by(team) %>%
    dplyr::summarise(
      n_games = sum(!is.na(value)),
      q50 = stats::median(value, na.rm = TRUE),
      q25 = stats::quantile(value, 0.25, na.rm = TRUE, names = FALSE, type = 7),
      q75 = stats::quantile(value, 0.75, na.rm = TRUE, names = FALSE, type = 7),
      IQR = q75 - q25,
      MAD = stats::mad(value, constant = 1, na.rm = TRUE), # unscaled MAD
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      rCV = dplyr::if_else(q50 == 0, NA_real_, MAD / abs(q50))
    ) %>%
    dplyr::left_join(dominant_team, by = "team")
  
  # ---- Determine ordering ----
  ord_df <- team_summ %>%
    dplyr::mutate(
      order_metric = dplyr::case_when(
        order_by == "median" ~ -q50,                               # desc
        order_by == "IQR" ~ IQR,                                   # asc
        TRUE ~ dplyr::if_else(is.na(rCV) | !is.finite(rCV), Inf, rCV) # rCV asc
      )
    ) %>%
    dplyr::arrange(order_metric)
  
  team_levels <- ord_df$team
  
  # Axis labels with n
  axis_labels <- setNames(
    paste0(team_summ$team[match(team_levels, team_summ$team)],
           "\n(n=", team_summ$n_games[match(team_levels, team_summ$team)], ")"),
    team_levels
  )
  
  # ---- Mark small-n teams ----
  team_summ <- team_summ %>%
    dplyr::mutate(
      small_n = n_games < min_games_for_badges
    )
  
  # ---- Join summaries into plot_data for aesthetics, build factor ----
  plot_data <- plot_data %>%
    dplyr::left_join(
      team_summ %>% dplyr::select(team, team_color_major, small_n),
      by = "team"
    ) %>%
    dplyr::mutate(
      team_factor = factor(team, levels = team_levels)
    )
  
  iqr_df <- team_summ %>%
    dplyr::mutate(
      team_factor = factor(team, levels = team_levels)
    )
  
  # ---- Badges (top/bottom 3 by rCV among adequate samples) ----
  badge_pool <- team_summ %>%
    dplyr::filter(!small_n, !is.na(rCV), is.finite(rCV))
  
  most_consistent <- if (nrow(badge_pool) > 0) {
    badge_pool %>%
      dplyr::arrange(rCV) %>%
      dplyr::slice_head(n = 3) %>%
      dplyr::pull(team) %>%
      paste(collapse = ", ")
  } else "—"
  
  most_volatile <- if (nrow(badge_pool) > 0) {
    badge_pool %>%
      dplyr::arrange(dplyr::desc(rCV)) %>%
      dplyr::slice_head(n = 3) %>%
      dplyr::pull(team) %>%
      paste(collapse = ", ")
  } else "—"
  
  stat_label  <- tools::toTitleCase(gsub("_", " ", stat_choice))
  week_text   <- paste0("Weeks ", paste0(range(week_range), collapse = "–"))
  season_text <- .season_label(season_choice)
  type_text   <- if (season_type_choice == "ALL") "REG+POST" else season_type_choice
  
  # ---- Plot ----
  p <- ggplot2::ggplot(plot_data, ggplot2::aes(x = team_factor, y = value)) +
    # Violins: outline in dominant team color; dim if small-n
    ggplot2::geom_violin(
      ggplot2::aes(color = I(team_color_major), alpha = small_n, group = team_factor),
      fill = NA, linewidth = 1.1, trim = FALSE
    ) +
    ggplot2::scale_alpha_manual(values = c(`TRUE` = 0.45, `FALSE` = 1), guide = "none") +
    
    # IQR per team (thick line) in dominant team color
    ggplot2::geom_linerange(
      data = iqr_df,
      ggplot2::aes(x = team_factor, ymin = q25, ymax = q75, color = I(team_color_major)),
      linewidth = 2, inherit.aes = FALSE
    ) +
    # Median tick (point) in dominant team color
    ggplot2::geom_point(
      data = iqr_df,
      ggplot2::aes(x = team_factor, y = q50, color = I(team_color_major)),
      size = 2.4, inherit.aes = FALSE
    ) +
    
    # Optional jittered weekly points: fill = team_color2, black stroke
    { if (isTRUE(show_points)) ggplot2::geom_jitter(
      ggplot2::aes(fill = I(team_color2)),
      width = 0.18, height = 0, shape = 21, size = 1.9, stroke = 0.25, color = "black", alpha = 0.65
    ) } +
    
    ggplot2::scale_y_continuous(labels = scales::comma) +
    ggplot2::scale_x_discrete(labels = axis_labels) +
    ggplot2::labs(
      title = paste0("Top ", top_n, " ", stat_label, " — ", season_text, " (", type_text, ")"),
      subtitle = paste0(
        week_text, "  •  Order by ", order_by,
        "  •  Most consistent: ", most_consistent,
        "  •  Most volatile: ", most_volatile
      ),
      x = NULL, y = stat_label,
      caption = "Distributions across selected seasons & weeks (pooled).  NA weeks excluded.  Lines in team_color; points (if shown) in team_color2."
    ) +
    ggplot2::theme_light(base_size = 14) +
    ggplot2::theme(
      axis.text.x = ggplot2::element_text(angle = 28, hjust = 1, vjust = 1),
      axis.title.y = ggplot2::element_text(margin = ggplot2::margin(r = 10)),
      plot.title = ggplot2::element_text(face = "bold", size = 18, hjust = 0),
      plot.subtitle = ggplot2::element_text(size = 11, margin = ggplot2::margin(b = 8)),
      plot.caption = ggplot2::element_text(size = 9, color = "grey50", hjust = 1),
      legend.position = "none"
    ) +
    ggplot2::coord_cartesian(clip = "off")
  
  return(p)
}

#' Team Scatter with Quadrants (Generalized + Highlight)
#'
#' One point per team over a selected window, plotting any two metrics.
#' Uses median x/y guides, square aspect, team colors, and optional highlights.
#'
#' @param data long table like `team_weekly` with:
#'   team, season, season_type, week, stat_type, stat_name, value,
#'   and ideally team_color, team_color2
#' @param season_choice integer or integer vector (e.g., 2023:2024)
#' @param season_type_choice "REG","POST","ALL" (ALL includes both)
#' @param week_range integer vector of weeks (e.g., 1:18)
#' @param top_n number of teams to display (ranked by gate logic)
#' @param metric_x string metric id (e.g., "passing_yards","completion_pct")
#' @param metric_y string metric id (same style)
#' @param top_by one of "combined","x_gate","y_gate","x_value","y_value"
#' @param log_x logical; log10 x-scale (drops non-positive)
#' @param log_y logical; log10 y-scale (drops non-positive)
#' @param highlight NULL, character vector of teams to emphasize, or "ALL"
#' @param label_mode "highlighted","all","none" (default "highlighted")
#' @param muted_alpha alpha for non-highlighted points when `highlight` is a vector (default 0.35)
#'
#' @return ggplot object
#' @export
plot_team_scatter_quadrants <- function(
    data,
    season_choice,
    season_type_choice = "REG",
    week_range = 1:18,
    top_n = 20,
    metric_x,
    metric_y,
    top_by = c("combined","x_gate","y_gate","x_value","y_value"),
    log_x = FALSE,
    log_y = FALSE,
    highlight = NULL,                # c("DET","KC"), "ALL", or NULL
    label_mode = c("highlighted","all","none"),
    muted_alpha = 0.35
) {
  `%||%` <- function(a, b) if (is.null(a)) b else a
  top_by <- match.arg(top_by)
  label_mode <- match.arg(label_mode)
  
  # --- Metric config (team-level derived + raw fallback) ---
  .metric_config <- function(metric) {
    derived <- list(
      # Passing—volume-aware, ratio-of-sums
      completion_pct = list(
        kind="rate",
        required_stats=c("completions","attempts"),
        denom_fun=function(df) (df$attempts %||% 0),
        num_fun=function(df) (df$completions %||% 0),
        gate_fun=function(df) (df$attempts %||% 0),
        label="Completion %"
      ),
      yards_per_attempt = list(
        kind="rate",
        required_stats=c("passing_yards","attempts"),
        denom_fun=function(df) (df$attempts %||% 0),
        num_fun=function(df) (df$passing_yards %||% 0),
        gate_fun=function(df) (df$attempts %||% 0),
        label="Yards per Attempt"
      ),
      passing_epa_per_dropback = list(
        kind="rate",
        required_stats=c("attempts","sacks","passing_epa"),
        denom_fun=function(df) (df$attempts %||% 0) + (df$sacks %||% 0),
        num_fun=function(df) (df$passing_epa %||% 0),
        gate_fun=function(df) (df$attempts %||% 0) + (df$sacks %||% 0),
        label="EPA per Dropback"
      ),
      passing_anya = list(
        kind="rate",
        required_stats=c("attempts","sacks","sack_yards","passing_yards","passing_tds","interceptions"),
        denom_fun=function(df) (df$attempts %||% 0) + (df$sacks %||% 0),
        num_fun=function(df) (df$passing_yards %||% 0) +
          20*(df$passing_tds %||% 0) -
          45*(df$interceptions %||% 0) -
          (df$sack_yards %||% 0),
        gate_fun=function(df) (df$attempts %||% 0) + (df$sacks %||% 0),
        label="ANY/A"
      ),
      sack_rate = list(
        kind="rate",
        required_stats=c("attempts","sacks"),
        denom_fun=function(df) (df$attempts %||% 0) + (df$sacks %||% 0),
        num_fun=function(df) (df$sacks %||% 0),
        gate_fun=function(df) (df$attempts %||% 0) + (df$sacks %||% 0),
        label="Sack Rate"
      ),
      interception_rate = list(
        kind="rate",
        required_stats=c("interceptions","attempts"),
        denom_fun=function(df) (df$attempts %||% 0),
        num_fun=function(df) (df$interceptions %||% 0),
        gate_fun=function(df) (df$attempts %||% 0),
        label="INT Rate"
      ),
      
      # Rushing / Receiving
      yards_per_carry = list(
        kind="rate",
        required_stats=c("rushing_yards","carries"),
        denom_fun=function(df) (df$carries %||% 0),
        num_fun=function(df) (df$rushing_yards %||% 0),
        gate_fun=function(df) (df$carries %||% 0),
        label="Yards per Carry"
      ),
      receiving_epa_per_target = list(
        kind="rate",
        required_stats=c("targets","receiving_epa"),
        denom_fun=function(df) (df$targets %||% 0),
        num_fun=function(df) (df$receiving_epa %||% 0),
        gate_fun=function(df) (df$targets %||% 0),
        label="EPA per Target"
      ),
      
      # Blended usage/efficiency
      total_epa_per_opportunity = list(
        kind="rate",
        required_stats=c("attempts","sacks","carries","targets",
                         "passing_epa","rushing_epa","receiving_epa"),
        denom_fun=function(df) (df$attempts %||% 0) + (df$sacks %||% 0) +
          (df$carries %||% 0) + (df$targets %||% 0),
        num_fun=function(df) (df$passing_epa %||% 0) +
          (df$rushing_epa %||% 0) +
          (df$receiving_epa %||% 0),
        gate_fun=function(df) (df$attempts %||% 0) + (df$sacks %||% 0) +
          (df$carries %||% 0) + (df$targets %||% 0),
        label="Total EPA per Opportunity"
      ),
      yards_per_opportunity = list(
        kind="rate",
        required_stats=c("attempts","sacks","carries","targets",
                         "passing_yards","rushing_yards","receiving_yards"),
        denom_fun=function(df) (df$attempts %||% 0) + (df$sacks %||% 0) +
          (df$carries %||% 0) + (df$targets %||% 0),
        num_fun=function(df) (df$passing_yards %||% 0) +
          (df$rushing_yards %||% 0) +
          (df$receiving_yards %||% 0),
        gate_fun=function(df) (df$attempts %||% 0) + (df$sacks %||% 0) +
          (df$carries %||% 0) + (df$targets %||% 0),
        label="Yards per Opportunity"
      )
    )
    if (metric %in% names(derived)) return(derived[[metric]])
    list(kind="raw_sum",
         required_stats=c(metric),
         stat_col=metric,
         label=tools::toTitleCase(gsub("_"," ",metric)))
  }
  
  # --- Filter window ---
  df <- data %>%
    dplyr::filter(
      season %in% !!season_choice,
      (season_type_choice == "ALL") | (season_type == !!season_type_choice),
      week %in% !!week_range,
      stat_type == "base"
    )
  
  if (nrow(df) == 0) {
    return(ggplot2::ggplot() +
             ggplot2::labs(
               title="Team Scatter with Quadrants",
               subtitle=paste0(
                 paste(range(week_range), collapse="–"), " • ",
                 paste(season_choice, collapse=", "), " (", season_type_choice, ") — no rows"
               ),
               x=metric_x, y=metric_y) +
             ggplot2::theme_void(base_size=14))
  }
  
  # --- Resolve metric configs ---
  cfg_x <- .metric_config(metric_x)
  cfg_y <- .metric_config(metric_y)
  req_stats <- unique(c(cfg_x$required_stats, cfg_y$required_stats))
  
  df_needed <- df %>%
    dplyr::filter(stat_name %in% req_stats) %>%
    dplyr::select(team, team_color, team_color2, stat_name, value)
  
  if (nrow(df_needed) == 0) {
    return(ggplot2::ggplot() +
             ggplot2::labs(
               title="Team Scatter with Quadrants",
               subtitle="Required stats not found for chosen metrics.",
               x=cfg_x$label, y=cfg_y$label) +
             ggplot2::theme_void(base_size=14))
  }
  
  # --- Aggregate to team-level sums per stat, then wide ---
  wide <- df_needed %>%
    dplyr::group_by(team, team_color, team_color2, stat_name) %>%
    dplyr::summarise(total = sum(value, na.rm=TRUE), .groups="drop_last") %>%
    tidyr::pivot_wider(names_from=stat_name, values_from=total, values_fill=0) %>%
    dplyr::ungroup()
  
  # --- Compute metric values & gates (ratio-of-sums for rates) ---
  compute_metric <- function(w, cfg) {
    if (identical(cfg$kind, "rate")) {
      den <- cfg$denom_fun(w); num <- cfg$num_fun(w); gate <- cfg$gate_fun(w)
      val <- ifelse(den > 0, num / den, NA_real_)
      list(value = val, gate = gate %||% 0)
    } else {
      v <- w[[cfg$stat_col]] %||% 0
      list(value = v, gate = abs(v))
    }
  }
  mx <- compute_metric(wide, cfg_x)
  my <- compute_metric(wide, cfg_y)
  
  agg <- wide %>%
    dplyr::mutate(
      x_value = mx$value, y_value = my$value,
      gate_x  = mx$gate,  gate_y  = my$gate
    ) %>%
    dplyr::filter(is.finite(x_value), is.finite(y_value))
  
  # Log-domain constraints
  if (isTRUE(log_x)) agg <- agg %>% dplyr::filter(x_value > 0)
  if (isTRUE(log_y)) agg <- agg %>% dplyr::filter(y_value > 0)
  
  if (nrow(agg) == 0) {
    return(ggplot2::ggplot() +
             ggplot2::labs(
               title="Team Scatter with Quadrants",
               subtitle="No valid points after metric/log filters.",
               x=cfg_x$label, y=cfg_y$label) +
             ggplot2::theme_void(base_size=14))
  }
  
  # --- Top-N selection by 'top_by' ---
  agg <- agg %>%
    dplyr::mutate(
      gate_total = (gate_x %||% 0) + (gate_y %||% 0),
      rank_key = dplyr::case_when(
        top_by == "combined" ~ gate_total,
        top_by == "x_gate"   ~ gate_x,
        top_by == "y_gate"   ~ gate_y,
        top_by == "x_value"  ~ x_value,
        top_by == "y_value"  ~ y_value
      )
    ) %>%
    dplyr::arrange(
      dplyr::desc(rank_key),
      dplyr::desc(gate_total),
      dplyr::desc(x_value + y_value),
      team
    ) %>%
    dplyr::slice_head(n = top_n)
  
  # --- Highlight logic & labels ---
  highlight_all <- identical(highlight, "ALL")
  has_specific_highlights <- !is.null(highlight) && !highlight_all
  
  agg <- agg %>%
    dplyr::mutate(
      is_highlight = dplyr::case_when(
        highlight_all ~ TRUE,
        has_specific_highlights ~ team %in% highlight,
        TRUE ~ TRUE
      ),
      alpha_val = if (has_specific_highlights) ifelse(is_highlight, 1, muted_alpha) else 1
    )
  
  label_df <-
    dplyr::case_when(
      label_mode == "all" ~ TRUE,
      label_mode == "highlighted" ~ agg$is_highlight,
      TRUE ~ FALSE
    )
  label_df <- agg[which(label_df), , drop = FALSE]
  
  # --- Quadrant medians
  med_x <- stats::median(agg$x_value, na.rm=TRUE)
  med_y <- stats::median(agg$y_value, na.rm=TRUE)
  
  # --- Plot ---
  p <- ggplot2::ggplot(agg, ggplot2::aes(x = x_value, y = y_value)) +
    ggplot2::geom_vline(xintercept = med_x, linetype="dashed", linewidth=0.4, color="grey55") +
    ggplot2::geom_hline(yintercept = med_y, linetype="dashed", linewidth=0.4, color="grey55") +
    ggplot2::geom_point(
      ggplot2::aes(color = I(team_color), fill = I(team_color2), alpha = alpha_val),
      size = 3.2, shape = 21, stroke = 0.5
    ) +
    ggplot2::scale_alpha_identity() +
    { if (nrow(label_df) > 0) ggrepel::geom_text_repel(
      data = label_df,
      ggplot2::aes(x = x_value, y = y_value, label = team),
      size = 3.2, max.overlaps = 1000, box.padding = 0.25, point.padding = 0.15,
      min.segment.length = 0.05
    ) } +
    { if (isTRUE(log_x)) ggplot2::scale_x_continuous(trans="log10") } +
    { if (isTRUE(log_y)) ggplot2::scale_y_continuous(trans="log10") } +
    ggplot2::labs(
      title = paste0(cfg_x$label, " vs ", cfg_y$label),
      subtitle = paste0(
        "Seasons: ", paste(season_choice, collapse=", "),
        " (", season_type_choice, ") • Weeks ", paste(range(week_range), collapse="–"),
        " • Top ", top_n, " by ", top_by, " • Medians shown",
        if (has_specific_highlights) paste0(" • Highlight: ", paste(highlight, collapse=", ")) else ""
      ),
      x = cfg_x$label, y = cfg_y$label,
      caption = "Each point = one team over the window.  Derived metrics are ratio-of-sums.  Points: fill=team_color2, outline=team_color."
    ) +
    ggplot2::theme_light(base_size = 14) +
    ggplot2::theme(
      plot.title    = ggplot2::element_text(face="bold", size=18, hjust=0),
      plot.subtitle = ggplot2::element_text(size=11, margin=ggplot2::margin(b=8)),
      plot.caption  = ggplot2::element_text(size=9, color="grey50", hjust=1),
      legend.position = "none",
      aspect.ratio = 1  # square panel, independent axes
    )
  
  return(p)
}


#' Rolling Form Percentiles — Sparkline Grid
#'
#' For each (season, season_type, week), compute each player's percentile
#' within their POSITION for a chosen weekly metric, then (optionally)
#' smooth with a rolling window. One small-multiple panel per player.
#'
#' Top-N players are selected by total `value` of `metric_choice` across
#' the pooled window (seasons × weeks × season types).
#'
#' @param data long table like `player_weekly`
#' @param season_choice integer or integer vector (e.g., 2023:2024)
#' @param season_type_choice "REG","POST","ALL" (ALL includes both; no preseason)
#' @param week_range integer vector (e.g., 1:18)
#' @param position_choice "QB","RB","WR","TE"
#' @param metric_choice stat_name to percentile (e.g., "rushing_epa","receiving_yards")
#' @param top_n number of players to show (ranked by total `value` of metric)
#' @param base_cumulative stat_type filter (default "base")
#' @param rolling_window integer >=1; k=1 disables smoothing (default 4)
#' @param show_points logical; show weekly dots (default TRUE)
#' @param label_last logical; label last point value (0–100) per panel (default TRUE)
#' @param ncol panels per row (default 4)
#'
#' @return ggplot object (small multiples)
plot_player_rolling_percentiles <- function(
    data,
    season_choice,
    season_type_choice = "REG",
    week_range = 1:18,
    position_choice,
    metric_choice,
    top_n = 16,
    base_cumulative = "base",
    rolling_window = 4,
    show_points = TRUE,
    label_last = TRUE,
    ncol = 4
) {
  # helper: nice season label
  .season_label <- function(s) {
    u <- sort(unique(s))
    if (length(u) <= 1) return(as.character(u))
    if (max(u) - min(u) + 1 == length(u)) paste0(min(u), "–", max(u)) else paste(u, collapse = ", ")
  }
  # helper: simple rolling mean with partial windows (includes current + previous k-1)
  .roll_mean_partial <- function(x, k) {
    if (k <= 1) return(x)
    n <- length(x); out <- numeric(n)
    for (i in seq_len(n)) {
      lo <- max(1, i - k + 1)
      out[i] <- mean(x[lo:i], na.rm = TRUE)
    }
    out
  }
  
  # ---- Filter window ----
  df <- data %>%
    dplyr::filter(
      season %in% !!season_choice,
      (season_type_choice == "ALL") | (season_type == !!season_type_choice),
      week %in% !!week_range,
      position == !!position_choice,
      stat_type == !!base_cumulative,
      stat_name == !!metric_choice
    ) %>%
    dplyr::filter(!is.na(value))
  
  if (nrow(df) == 0) {
    return(ggplot2::ggplot() +
             ggplot2::labs(
               title = "Rolling Form Percentiles",
               subtitle = "No rows after filters.",
               x = NULL, y = NULL
             ) +
             ggplot2::theme_void(base_size = 14))
  }
  
  # ---- Top-N players by total value over the window ----
  top_players <- df %>%
    dplyr::group_by(player_id, name) %>%
    dplyr::summarise(total_value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(total_value)) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::pull(player_id)
  
  df <- df %>% dplyr::filter(player_id %in% !!top_players)
  
  # ---- Dominant team colors per player (mode) ----
  dom_cols <- df %>%
    dplyr::count(player_id, team, team_color, team_color2, sort = TRUE) %>%
    dplyr::group_by(player_id) %>%
    dplyr::slice_head(n = 1) %>%
    dplyr::ungroup() %>%
    dplyr::transmute(
      player_id,
      team_mode = team,
      team_color_major = team_color,
      team_color2_major = team_color2
    )
  
  # ---- Build a global time index (season, type, week) -> t_idx ----
  phase_levels <- c("REG", "POST")
  time_map <- df %>%
    dplyr::distinct(season, season_type, week) %>%
    dplyr::mutate(season_type = factor(season_type, levels = phase_levels)) %>%
    dplyr::arrange(season, season_type, week) %>%
    dplyr::mutate(t_idx = dplyr::row_number())
  
  # ---- Percentile per (season, season_type, week) within POSITION ----
  df_pct <- df %>%
    dplyr::left_join(time_map, by = c("season","season_type","week")) %>%
    dplyr::group_by(season, season_type, week) %>%
    dplyr::mutate(
      n_obs = dplyr::n(),
      pct = ifelse(n_obs > 1, dplyr::percent_rank(value) * 100, 50)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(dom_cols, by = "player_id")
  
  # ---- Rolling mean (per player, per season to avoid crossing boundaries) ----
  df_pct <- df_pct %>%
    dplyr::arrange(player_id, season, season_type, t_idx) %>%
    dplyr::group_by(player_id, season, season_type) %>%
    dplyr::mutate(pct_roll = .roll_mean_partial(pct, rolling_window)) %>%
    dplyr::ungroup()
  
  # last point per player (for labeling & facet order)
  last_pts <- df_pct %>%
    dplyr::group_by(player_id, name) %>%
    dplyr::filter(t_idx == max(t_idx)) %>%
    dplyr::summarise(last_pct = tail(pct_roll, 1), .groups = "drop")
  
  # facet order by last rolling percentile (desc)
  player_levels <- last_pts %>% dplyr::arrange(dplyr::desc(last_pct)) %>% dplyr::pull(player_id)
  df_pct <- df_pct %>% dplyr::mutate(player_factor = factor(player_id, levels = player_levels))
  
  # label positions (last value per player)
  lab_df <- df_pct %>%
    dplyr::group_by(player_factor, name) %>%
    dplyr::filter(t_idx == max(t_idx)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(label_txt = sprintf("%.0f", pct_roll))
  
  metric_label  <- tools::toTitleCase(gsub("_", " ", metric_choice))
  week_text     <- paste0("Weeks ", paste(range(week_range), collapse = "–"))
  season_text   <- .season_label(season_choice)
  type_text     <- if (season_type_choice == "ALL") "REG+POST" else season_type_choice
  roll_text     <- if (rolling_window > 1) paste0("Rolling ", rolling_window, "-wk mean") else "No smoothing"
  
  # ---- Plot ----
  p <- ggplot2::ggplot(df_pct, ggplot2::aes(x = t_idx, y = pct_roll, group = player_id)) +
    ggplot2::geom_line(ggplot2::aes(color = I(team_color_major)), linewidth = 0.9, alpha = 0.9) +
    { if (isTRUE(show_points)) ggplot2::geom_point(
      ggplot2::aes(fill = I(team_color2_major)),
      shape = 21, size = 1.8, stroke = 0.25, color = "black", alpha = 0.9
    ) } +
    { if (isTRUE(label_last)) ggrepel::geom_text_repel(
      data = lab_df,
      ggplot2::aes(x = t_idx, y = pct_roll, label = label_txt),
      size = 3.0, box.padding = 0.15, point.padding = 0.1, min.segment.length = 0.05,
      seed = 42
    ) } +
    ggplot2::scale_y_continuous(limits = c(0, 100), breaks = c(0,25,50,75,100)) +
    ggplot2::labs(
      title = paste0("Rolling Form Percentiles — ", metric_label, " (", position_choice, ")"),
      subtitle = paste0(season_text, " (", type_text, ") • ", week_text, " • ", roll_text,
                        " • Top ", top_n, " by total ", metric_label),
      x = NULL, y = "Percentile (within position, weekly)"
    ) +
    ggplot2::facet_wrap(~ player_factor, ncol = ncol, labeller = ggplot2::labeller(
      player_factor = function(ids) {
        nm <- dplyr::left_join(
          data.frame(player_id = levels(df_pct$player_factor)[levels(df_pct$player_factor) %in% ids]),
          df_pct %>% dplyr::distinct(player_id, name),
          by = "player_id"
        )$name
        nm
      })) +
    ggplot2::theme_light(base_size = 13) +
    ggplot2::theme(
      strip.text = ggplot2::element_text(face = "bold"),
      panel.grid.minor = ggplot2::element_blank(),
      axis.text.x = ggplot2::element_blank(),
      axis.ticks.x = ggplot2::element_blank(),
      legend.position = "none",
      plot.title = ggplot2::element_text(face = "bold", size = 18, hjust = 0),
      plot.subtitle = ggplot2::element_text(size = 11)
    )
  
  return(p)
}

#' Rolling Form Percentiles — Teams (Sparkline Grid)
#'
#' For each (season, season_type, week), compute each team's percentile
#' within the LEAGUE for a chosen weekly metric, then (optionally) smooth
#' with a rolling window. One small-multiple panel per team.
#'
#' Top-N teams are selected by total `value` of `metric_choice` across
#' the pooled window (seasons × weeks × season types).
#'
#' @param data long table like `team_weekly`
#' @param season_choice integer or integer vector (e.g., 2023:2024)
#' @param season_type_choice "REG","POST","ALL" (ALL includes both; no preseason)
#' @param week_range integer vector (e.g., 1:18)
#' @param metric_choice stat_name to percentile (e.g., "rushing_epa","passing_yards")
#' @param top_n number of teams to show (ranked by total `value` of metric)
#' @param base_cumulative stat_type filter (default "base")
#' @param rolling_window integer >=1; k=1 disables smoothing (default 4)
#' @param show_points logical; show weekly dots (default TRUE)
#' @param label_last logical; label last point value (0–100) per panel (default TRUE)
#' @param ncol panels per row (default 4)
#'
#' @return ggplot object (small multiples)
#' @export
plot_team_rolling_percentiles <- function(
    data,
    season_choice,
    season_type_choice = "REG",
    week_range = 1:18,
    metric_choice,
    top_n = 16,
    base_cumulative = "base",
    rolling_window = 4,
    show_points = TRUE,
    label_last = TRUE,
    ncol = 4
) {
  # helper: nice season label
  .season_label <- function(s) {
    u <- sort(unique(s))
    if (length(u) <= 1) return(as.character(u))
    if (max(u) - min(u) + 1 == length(u)) paste0(min(u), "–", max(u)) else paste(u, collapse = ", ")
  }
  # helper: simple rolling mean with partial windows (includes current + previous k-1)
  .roll_mean_partial <- function(x, k) {
    if (k <= 1) return(x)
    n <- length(x); out <- numeric(n)
    for (i in seq_len(n)) {
      lo <- max(1, i - k + 1)
      out[i] <- mean(x[lo:i], na.rm = TRUE)
    }
    out
  }
  
  # ---- Filter window ----
  df <- data %>%
    dplyr::filter(
      season %in% !!season_choice,
      (season_type_choice == "ALL") | (season_type == !!season_type_choice),
      week %in% !!week_range,
      stat_type == !!base_cumulative,
      stat_name == !!metric_choice
    ) %>%
    dplyr::filter(!is.na(value))
  
  if (nrow(df) == 0) {
    return(ggplot2::ggplot() +
             ggplot2::labs(
               title = "Rolling Form Percentiles — Teams",
               subtitle = "No rows after filters.",
               x = NULL, y = NULL
             ) +
             ggplot2::theme_void(base_size = 14))
  }
  
  # ---- Top-N teams by total value over the window ----
  top_teams <- df %>%
    dplyr::group_by(team) %>%
    dplyr::summarise(total_value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    dplyr::arrange(dplyr::desc(total_value), team) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::pull(team)
  
  df <- df %>% dplyr::filter(team %in% !!top_teams)
  
  # ---- Dominant team colors per team (mode across pooled window) ----
  dom_cols <- df %>%
    dplyr::count(team, team_color, team_color2, sort = TRUE) %>%
    dplyr::group_by(team) %>%
    dplyr::slice_head(n = 1) %>%
    dplyr::ungroup() %>%
    dplyr::transmute(
      team,
      team_color_major  = team_color,
      team_color2_major = team_color2
    )
  
  # ---- Build a global time index (season, type, week) -> t_idx ----
  phase_levels <- c("REG", "POST")
  time_map <- df %>%
    dplyr::distinct(season, season_type, week) %>%
    dplyr::mutate(season_type = factor(season_type, levels = phase_levels)) %>%
    dplyr::arrange(season, season_type, week) %>%
    dplyr::mutate(t_idx = dplyr::row_number())
  
  # ---- Percentile per (season, season_type, week) within LEAGUE ----
  df_pct <- df %>%
    dplyr::left_join(time_map, by = c("season","season_type","week")) %>%
    dplyr::group_by(season, season_type, week) %>%
    dplyr::mutate(
      n_obs = dplyr::n(),
      pct = ifelse(n_obs > 1, dplyr::percent_rank(value) * 100, 50)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(dom_cols, by = "team")
  
  # ---- Rolling mean (per team, per season to avoid crossing boundaries) ----
  df_pct <- df_pct %>%
    dplyr::arrange(team, season, season_type, t_idx) %>%
    dplyr::group_by(team, season, season_type) %>%
    dplyr::mutate(pct_roll = .roll_mean_partial(pct, rolling_window)) %>%
    dplyr::ungroup()
  
  # last point per team (for facet ordering)
  last_pts <- df_pct %>%
    dplyr::group_by(team) %>%
    dplyr::filter(t_idx == max(t_idx)) %>%
    dplyr::summarise(last_pct = tail(pct_roll, 1), .groups = "drop")
  
  # facet order by last rolling percentile (desc)
  team_levels <- last_pts %>% dplyr::arrange(dplyr::desc(last_pct)) %>% dplyr::pull(team)
  df_pct <- df_pct %>% dplyr::mutate(team_factor = factor(team, levels = team_levels))
  
  # labels (last value per team)
  lab_df <- df_pct %>%
    dplyr::group_by(team_factor) %>%
    dplyr::filter(t_idx == max(t_idx)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(label_txt = sprintf("%.0f", pct_roll))
  
  metric_label  <- tools::toTitleCase(gsub("_", " ", metric_choice))
  week_text     <- paste0("Weeks ", paste(range(week_range), collapse = "–"))
  season_text   <- .season_label(season_choice)
  type_text     <- if (season_type_choice == "ALL") "REG+POST" else season_type_choice
  roll_text     <- if (rolling_window > 1) paste0("Rolling ", rolling_window, "-wk mean") else "No smoothing"
  
  # ---- Plot ----
  p <- ggplot2::ggplot(df_pct, ggplot2::aes(x = t_idx, y = pct_roll, group = team)) +
    ggplot2::geom_line(ggplot2::aes(color = I(team_color_major)), linewidth = 0.9, alpha = 0.9) +
    { if (isTRUE(show_points)) ggplot2::geom_point(
      ggplot2::aes(fill = I(team_color2_major)),
      shape = 21, size = 1.8, stroke = 0.25, color = "black", alpha = 0.9
    ) } +
    { if (isTRUE(label_last)) ggrepel::geom_text_repel(
      data = lab_df,
      ggplot2::aes(x = t_idx, y = pct_roll, label = label_txt),
      size = 3.0, box.padding = 0.15, point.padding = 0.1, min.segment.length = 0.05,
      seed = 42
    ) } +
    ggplot2::scale_y_continuous(limits = c(0, 100), breaks = c(0,25,50,75,100)) +
    ggplot2::labs(
      title = paste0("Rolling Form Percentiles — ", metric_label, " (Teams)"),
      subtitle = paste0(season_text, " (", type_text, ") • ", week_text, " • ", roll_text,
                        " • Top ", top_n, " by total ", metric_label),
      x = NULL, y = "Percentile (within league, weekly)"
    ) +
    ggplot2::facet_wrap(~ team_factor, ncol = ncol) +
    ggplot2::theme_light(base_size = 13) +
    ggplot2::theme(
      strip.text = ggplot2::element_text(face = "bold"),
      panel.grid.minor = ggplot2::element_blank(),
      axis.text.x = ggplot2::element_blank(),
      axis.ticks.x = ggplot2::element_blank(),
      legend.position = "none",
      plot.title = ggplot2::element_text(face = "bold", size = 18, hjust = 0),
      plot.subtitle = ggplot2::element_text(size = 11)
    )
  
  return(p)
}


