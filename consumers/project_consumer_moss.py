"""
project_producer_case.py

Stream JSON data to a file and - if available - a Kafka topic.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

"""


"""
json_consumer_case.py

Consume json messages from a Kafka topic and visualize author counts in real-time.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve"}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting author occurrences

# Import external packages
from dotenv import load_dotenv

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("PROJECT_TOPIC", "unknown_topic") #changed
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up data structures
#####################################

# Initialize a dictionary to store author counts
author_counts = defaultdict(int)

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


# Initialize lists to store timestamps and sentiment scores
timestamps = []
sentiments = []

def update_chart():
    """Update the live line chart with sentiment trends."""
    ax.clear()

    # Plot sentiment over time
    ax.plot(timestamps, sentiments, marker='o', linestyle='-', color="blue")

    ax.set_xlabel("Time")
    ax.set_ylabel("Sentiment Score")
    ax.set_title("Real-Time Sentiment Analysis")

    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)


#####################################
# Function to process timestamp and sentiment data
# #####################################
def process_message(message: str) -> None:
    try:
        message_dict = json.loads(message)
        timestamp = message_dict.get("timestamp", "unknown")
        sentiment = message_dict.get("sentiment", 0.0)

        if timestamp != "unknown":
            timestamps.append(timestamp)
            sentiments.append(sentiment)

            # Keep only the last 100 data points for visualization
            if len(timestamps) > 100:
                timestamps.pop(0)
                sentiments.pop(0)

            update_chart()

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls messages and updates a live chart.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            # message is a complex object with metadata and value
            # Use the value attribute to extract the message as a string
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":

    # Call the main function to start the consumer
    main()

    # Turn off interactive mode after completion
    plt.ioff()  

    # Display the final chart
    plt.show()
