from psycopg2.extras import execute_batch, execute_values
import os

def mark_error_videos(cur, video_ids):
    if not video_ids:
        return
    region_table = "videos"
    if os.getenv("REGION") != "us":
        region_table = f"videos_{os.getenv('REGION')}"
    query = f"""
        UPDATE {region_table}
        SET has_error = TRUE
        WHERE video_id IN %s
    """
    cur.execute(query, (tuple(video_ids),))