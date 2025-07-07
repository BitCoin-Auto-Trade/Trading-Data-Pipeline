from apscheduler.schedulers.background import BackgroundScheduler
import time
from utils.logger import get_logger

logger = get_logger("scheduler")

def sample_job():
    logger.info("스케줄러 테스트 작업 실행!")

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(sample_job, 'interval', seconds=60)
    scheduler.start()
    logger.info("스케줄러 시작!")

    try:
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("스케줄러 종료!")

if __name__ == "__main__":
    start_scheduler()