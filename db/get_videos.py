from db.connect import get_db_connection
import logging

def get_videos(scrape_frequency=4, limit=50):
    sql = """
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
            videos
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
