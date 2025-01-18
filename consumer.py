import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
import logging
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import nltk
from nltk.corpus import stopwords
import threading
from threading import Lock
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("NewsConsumer")

# Download NLTK resources
nltk.download("stopwords")
stop_words = set(stopwords.words("english"))

# Shared data structure
articles = []
lock = Lock()


def detect_topics(content):
    """
    Detect topics from the news content using LDA.
    """
    try:
        logger.info("Detecting topics...")
        words = [word for word in content.split() if word.lower() not in stop_words]
        if len(words) < 5:
            return ["Insufficient content for topic detection"]

        vectorizer = CountVectorizer(max_features=1000, stop_words="english")
        doc_term_matrix = vectorizer.fit_transform([" ".join(words)])
        lda = LatentDirichletAllocation(n_components=3, random_state=42)
        lda.fit(doc_term_matrix)

        topic_words = vectorizer.get_feature_names_out()
        topics = [topic_words[i] for i in lda.components_.argsort(axis=1)[:, -3:][:, -1]]

        logger.info(f"Detected topics: {topics}")
        return topics

    except Exception as e:
        logger.error(f"Error in topic detection: {e}")
        return ["Error detecting topics"]


def process_news(news_data):
    """
    Process each news article and add to the visualization list.
    """
    try:
        logger.info(f"Processing news article: {news_data['title']}")
        content = news_data.get("content", "")
        if not content:
            logger.warning("No content found for topic detection.")
            return

        detected_topics = detect_topics(content)

        article = {
            "title": news_data["title"],
            "source": news_data["source"],
            "content": news_data["content"],
            "publishedAt": news_data.get("publishedAt", "N/A"),
            "topics": ", ".join(detected_topics),
        }

        with lock:
            articles.append(article)

    except Exception as e:
        logger.error(f"Error processing article: {e}")


def consume_and_display():
    """
    Kafka Consumer to process news articles in real-time.
    """
    try:
        consumer = KafkaConsumer(
            "raw-news",
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="news-consumer-group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        logger.info("Started Kafka consumer, waiting for messages...")

        for message in consumer:
            news_data = message.value
            logger.info(f"Received article: {news_data['title']}")
            process_news(news_data)

    except Exception as e:
        logger.error(f"Error in Kafka consumer: {e}")
    finally:
        logger.info("Shutting down Kafka consumer.")
        
from collections import Counter
import matplotlib.pyplot as plt

def compute_topic_stats(articles):
    """
    Compute topic counts and percentages for visualization.
    """
    all_topics = []
    for article in articles:
        topics = article.get("topics", "").split(", ")
        all_topics.extend(topics)
    
    # Count topics
    topic_counts = Counter(all_topics)
    
    # Convert counts to percentages
    total_count = sum(topic_counts.values())
    topic_percentages = {topic: (count / total_count) * 100 for topic, count in topic_counts.items()}
    
    return topic_counts, topic_percentages



def main():
    """
    Start the Streamlit app and Kafka consumer in parallel.
    """
    st.title("Real-Time News and Topic Detection")
    st.subheader("News Articles")

    # Create a placeholder for dynamic content
    placeholder = st.empty()

    # Start the Kafka consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_and_display, daemon=True)
    consumer_thread.start()

    while True:
        with lock:
            # Use the placeholder to prevent duplication
            with placeholder.container():
                if articles:
                    # Create a DataFrame for the articles
                    df = pd.DataFrame(articles)
                    st.dataframe(df)

                    # Compute topic stats
                    topic_counts, topic_percentages = compute_topic_stats(articles)

                    # Display topic stats as a table
                    st.subheader("Topic Summary")
                    st.write("Below is a summary of topics across all articles.")
                    topic_stats_df = pd.DataFrame({
                        "Topic": list(topic_counts.keys()),
                        "Count": list(topic_counts.values()),
                        "Percentage (%)": [round(v, 2) for v in topic_percentages.values()],
                    })
                    st.dataframe(topic_stats_df)

                    # Bar chart of topic counts
                    st.subheader("Topic Distribution")
                    st.bar_chart(pd.DataFrame.from_dict(topic_counts, orient='index', columns=['Count']))

                    # Pie chart of topic percentages
                    st.subheader("Topic Percentages")
                    fig, ax = plt.subplots()
                    ax.pie(topic_percentages.values(), labels=topic_percentages.keys(), autopct='%1.1f%%', startangle=140)
                    ax.axis('equal')  # Equal aspect ratio ensures the pie chart is circular
                    st.pyplot(fig)

                else:
                    st.write("Waiting for news articles...")

        time.sleep(5)  # Add a delay to control update frequency



if __name__ == "__main__":
    main()
