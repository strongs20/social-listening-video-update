from psycopg2.extras import execute_batch, execute_values

def mark_error_videos(cur, video_ids):
    if not video_ids:
        return
    query = """
        UPDATE videos
        SET has_error = TRUE
        WHERE video_id IN %s
    """
    cur.execute(query, (tuple(video_ids),))