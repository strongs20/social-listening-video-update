def claim_videos(cur, scrape_frequency=4, limit=50):
    sql = """
        SELECT
            video_id, product_id, views, last_updated
        FROM videos
        WHERE
            scrape_frequency = %s
            AND (last_updated IS NULL OR last_updated < NOW() - INTERVAL '23 hours')
        ORDER BY
            last_updated ASC NULLS FIRST
        LIMIT %s
        FOR UPDATE SKIP LOCKED;
    """
    cur.execute(sql, (scrape_frequency, limit))
    colnames = [desc[0] for desc in cur.description]
    videos = [dict(zip(colnames, row)) for row in cur.fetchall()]
    return videos
