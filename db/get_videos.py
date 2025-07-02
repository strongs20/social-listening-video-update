from db.connect import get_db_connection
import logging
import os

def get_videos(scrape_frequency=4, limit=50):
    region_table = "videos"
    if os.getenv("REGION") == "uk":
        region_table = "videos_uk"
    sql = f"""
        SELECT
            video_id,
            author_id,
            product_id,
            description,
            time_posted,
            is_ad,
            last_updated,
            scrape_frequency,
            comments,
            views,
            likes,
            handle
        FROM
            {region_table}
        WHERE
            scrape_frequency = %s
        ORDER BY
            last_updated ASC NULLS FIRST
        LIMIT %s;
    """
    try:
        # Using a new connection for each call in a simple script is fine
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (scrape_frequency, limit))                
                rows = cur.fetchall()    
                colnames = [desc[0] for desc in cur.description]
                videos = [dict(zip(colnames, row)) for row in rows]
                
                return videos
    except Exception as e:
        logging.error(f"Error fetching videos for timeseries: {e}")
        return []
