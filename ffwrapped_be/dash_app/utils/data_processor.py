def process_weekly_data(api_data):
    """Processes API data into points per week & formatted hover text"""
    api_data = {int(k): v for k, v in api_data.items()}
    weeks = sorted(api_data.keys())
    points_per_week = []
    hover_data = []

    for week in weeks:
        week_data = api_data[week]
        best_lineup = week_data.get("starters", {})

        # Calculate total points
        total_points = sum(
            player["points"] for position in best_lineup.values() for player in position
        )
        points_per_week.append(total_points)

        # Create formatted hover tooltip
        hover_text = "<b>Position | Player | Points</b><br>"
        hover_text += "<br>".join(
            f"{position}: {player['name']} ({player['points']} pts)"
            for position, players in best_lineup.items()
            for player in players
        )
        hover_data.append(hover_text)

    return weeks, points_per_week, hover_data
