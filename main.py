import asyncio
import json
import os
from time import time
from datetime import datetime
from dotenv import load_dotenv
from scraper.video_stats_scraper import get_video_stats

from db.connect import get_db_connection
from db.claim_videos import claim_videos
from db.update_videos import update_videos
from db.failed_videos import mark_error_videos

load_dotenv()
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50))
SCRAPE_FREQUENCY = int(os.getenv("SCRAPE_FREQUENCY", 4))
CONCURRENT_REQUESTS = int(os.getenv("CONCURRENT_REQUESTS", 10))
FAILURE_THRESHOLD_PERCENT = 0.20

def calculate_new_scrape_frequency(view_gain, days_since_last_scrape):
    if days_since_last_scrape <= 0:
        days_since_last_scrape = 1 
    
    avpd = int(view_gain / days_since_last_scrape)
    
    if avpd < 1000: return 0
    if avpd < 10000: return 1
    if avpd < 100000: return 2
    if avpd < 1000000: return 3
    return 4

async def worker(semaphore, video_id: str):
    async with semaphore:
        stats = await get_video_stats(video_id) 
        if stats:
            return {"video_id": video_id, "stats": stats, "status": "SUCCESS"}
        return {"video_id": video_id, "stats": None, "status": "FAILED"}
    
async def main():
    scraping_completed = False
    
    while not scraping_completed:
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    start_time = time()
                    videos = claim_videos(cur, scrape_frequency=SCRAPE_FREQUENCY, limit=BATCH_SIZE)
                    if not videos:
                        print("No more videos to process. Exiting.")
                        scraping_completed = True
                        break
                    
                    print(f"Claimed {len(videos)} videos to process.")

                    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
                    tasks = [worker(semaphore, video['video_id']) for video in videos]

                    scrape_results = await asyncio.gather(*tasks)

                    successful_results = [r for r in scrape_results if r['status'] == 'SUCCESS']
                    failed_results = [r for r in scrape_results if r['status'] == 'FAILED']
                    
                    failure_rate = len(failed_results) / len(videos)

                    if failure_rate > FAILURE_THRESHOLD_PERCENT:
                        print(f"High failure rate ({failure_rate:.2%}) detected.")

                    elif failed_results:
                        print(f"Retrying {len(failed_results)} failed videos.")
                        retry_tasks = [worker(semaphore, r['video_id']) for r in failed_results]
                        retry_results = await asyncio.gather(*retry_tasks)
                        
                        failed_after_retry = [r for r in retry_results if r['status'] == 'FAILED']
                        successful_results.extend([r for r in retry_results if r['status'] == 'SUCCESS'])

                        if failed_after_retry:
                            error_ids = [r['video_id'] for r in failed_after_retry]
                            mark_error_videos(cur, error_ids)
                            print(f"Marked {len(error_ids)} videos as permanently failed.")

                    processed_data_for_db = []
                    old_data_map = {v['video_id']: v for v in videos}
                    current_time = datetime.now()

                    for result in successful_results:
                        video_id = result['video_id']
                        old_data = old_data_map[video_id]
                        new_stats = result['stats']

                        view_gain = new_stats['views'] - (old_data.get('views') or 0)
                        
                        last_updated_time = old_data.get('last_updated') or datetime.fromtimestamp(0)
                        days_since_last_scrape = (current_time - last_updated_time).total_seconds() / (24 * 3600)
                        days_since_last_scrape = max(days_since_last_scrape, 1.0)

                        new_scrape_frequency = calculate_new_scrape_frequency(view_gain, days_since_last_scrape)
                        
                        processed_data_for_db.append({
                            "video_id": video_id,
                            "views": new_stats['views'],
                            "likes": new_stats['likes'],
                            "comments": new_stats['comments'],
                            "scrape_frequency": new_scrape_frequency
                        })

                    if processed_data_for_db:
                        print(f"Updating database with {len(processed_data_for_db)} successful scrapes")
                        update_videos(cur, processed_data_for_db)
                    
                    conn.commit()
                    duration = round(time() - start_time, 2)

        except Exception as e:
            print(f"Error during processing: {e}")

        await asyncio.sleep(10)
        
        
if __name__ == "__main__":
    asyncio.run(main())
