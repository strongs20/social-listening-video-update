from psycopg2.extras import execute_batch, execute_values

def update_videos(cur, video_data_list):
    if not video_data_list:
        print("No processed videos to update.")
        return

    videos_to_update = [
        (
            video['video_id'],
            video['views'],
            video['likes'],
            video['comments'],
            video['scrape_frequency']
        ) for video in video_data_list
    ]

    update_sql = """
        UPDATE videos AS v SET
            views = data.views,
            likes = data.likes,
            comments = data.comments,
            scrape_frequency = data.scrape_frequency,
            last_updated = now()
        FROM (VALUES %s) AS data(video_id, views, likes, comments, scrape_frequency)
        WHERE v.video_id = data.video_id;
    """
    
    if videos_to_update:
        execute_values(cur, update_sql, videos_to_update)
