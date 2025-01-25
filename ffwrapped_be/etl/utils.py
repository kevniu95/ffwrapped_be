from ratelimit import RateLimitException, sleep_and_retry
import logging
import time

logging.basicConfig(level=logging.INFO)

def custom_sleep_and_retry(func):
    @sleep_and_retry
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except RateLimitException as e:
            logging.info(f"Rate limit reached. Sleeping for {e.period_remaining} seconds.")
            time.sleep(e.period_remaining)
            return func(*args, **kwargs)
    return wrapper
