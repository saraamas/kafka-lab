import threading
from GoogleNews import GoogleNews
from kafka import KafkaProducer
from newsapi import NewsApiClient
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("NewsProducer")

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    retries=5,            # Retry sending messages up to 5 times
    linger_ms=100,        # Batch messages within 100ms before sending
)

# Initialize NewsAPI client
API_KEY = 'da12b135f6dd4a2d977ef63c4c218042'
newsapi = NewsApiClient(api_key=API_KEY)

def fetch_and_send_news_NEWSAPI():
    """
    Fetch articles from NewsAPI and send them to Kafka.
    """
    while True:
        try:
            logger.info("Fetching news articles from NewsAPI...")
            top_headlines = newsapi.get_top_headlines(country='us')

            if 'articles' in top_headlines:
                articles = top_headlines['articles']
                logger.info(f"Fetched {len(articles)} articles from NewsAPI.")

                for article in articles:
                    news_data = {
                        'source': 'NewsAPI',
                        'title': article['title'],
                        'content': article.get('content', ''),
                        'publishedAt': article['publishedAt'],
                    }

                    producer.send('raw-news', value=news_data)
                    logger.info(f"Sent article to Kafka: {news_data['title']}")
            else:
                logger.warning("No articles found from NewsAPI.")

        except Exception as e:
            logger.error(f"Error in fetch_and_send_news_NEWSAPI: {e}")

        time.sleep(60)  # Fetch news every minute

def fetch_and_send_google_news():
    """
    Fetch articles from Google News and send them to Kafka.
    """
    googlenews = GoogleNews(lang='en', region='US')

    while True:
        try:
            logger.info("Fetching news articles from Google News...")
            googlenews.get_news('')
            news = googlenews.results()

            if news:
                logger.info(f"Fetched {len(news)} articles from Google News.")

                for article in news:
                    news_data = {
                        'source': 'Google News',
                        'title': article['title'],
                        'content': article.get('desc', ''),
                        'publishedAt': article['date'],
                    }

                    producer.send('raw-news', value=news_data)
                    logger.info(f"Sent article to Kafka: {news_data['title']}")
            else:
                logger.warning("No articles found from Google News.")

        except Exception as e:
            logger.error(f"Error in fetch_and_send_google_news: {e}")

        time.sleep(60)  # Fetch news every minute

def main():
    """
    Main function to start producer threads for NewsAPI and Google News.
    """
    try:
        logger.info("Starting producer threads...")

        # Thread for NewsAPI
        newsapi_thread = threading.Thread(target=fetch_and_send_news_NEWSAPI, daemon=True)

        # Thread for Google News
        google_news_thread = threading.Thread(target=fetch_and_send_google_news, daemon=True)

        # Start threads
        newsapi_thread.start()
        google_news_thread.start()

        # Keep main thread alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

if __name__ == "__main__":
    main()



