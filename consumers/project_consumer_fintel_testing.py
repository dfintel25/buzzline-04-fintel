"""
project_consumer_fintel.py

Read a JSON-formatted file as it is being written. 

Example JSON message:
{"message": "I just saw a movie! It was amazing.", "author": "Eve"}
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import json
import os # for file operations
import sys # to exit early
import time
import pathlib
from collections import defaultdict  # data structure for counting author occurrences

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_logger import logger


#####################################
# Set up Paths - read from the file the producer writes
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Data folder: {DATA_FOLDER}")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Set up data structures
#####################################

author_counts = defaultdict(int)

#####################################
# Set up live visuals
#####################################

fig, ax = plt.subplots()
plt.ion()  # Turn on interactive mode for live updates

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_chart():
    """Update the live chart with the latest author counts."""
    # Clear the previous chart
    ax.clear()

    # Get the authors and counts from the dictionary
    authors_list = list(author_counts.keys())
    counts_list = list(author_counts.values())

    # Create a bar chart using the bar() method.
    # Pass in the x list, the y list, and the color
    ax.bar(authors_list, counts_list, color="green")

    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Authors")
    ax.set_ylabel("Message Counts")
    ax.set_title("Basic Real-Time Author Message Counts")

    # Use the set_xticklabels() method to rotate the x-axis labels
    # Pass in the x list, specify the rotation angle is 45 degrees,
    # and align them to the right
    # ha stands for horizontal alignment
    ax.set_xticklabels(authors_list, rotation=45, ha="right")

    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.01)


#####################################
# Process Message Function
#####################################

'''
def process_message(message: str) -> None:
    """
    Process a single JSON message and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)
       
        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Ensure it's a dictionary before accessing fields
        if isinstance(message_dict, dict):
            # Extract the 'author' field from the Python dictionary
            author = message_dict.get("author", "unknown")
            logger.info(f"Message received from author: {author}")

            # Increment the count for the author
            author_counts[author] += 1

            # Log the updated counts
            logger.info(f"Updated author counts: {dict(author_counts)}")

            # Update the chart
            update_chart()

            # Log the updated chart
            logger.info(f"Chart updated successfully for message: {message}")

        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Process Message
#####################################

last_timestamp = None  # track latest message time

def process_message(message: str | bytes):
    global last_timestamp
    try:
        if isinstance(message, bytes):
            message = message.decode("utf-8")

        logger.debug(f"Raw message: {message}")
        message_dict = json.loads(message)

        if isinstance(message_dict, dict):
            author = message_dict.get("author", "unknown")
            timestamp = message_dict.get("timestamp", "unknown")

            # update counts + last seen timestamp
            author_counts[author] += 1
            last_timestamp = timestamp  

            logger.info(f"Updated counts: {dict(author_counts)}")
            update_chart()
        else:
            logger.error(f"Expected dict, got {type(message_dict)}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


'''
message_counter = 0  # track message order

def process_message(message: str | bytes):
    global last_timestamp, message_counter
    try:
        if isinstance(message, bytes):
            message = message.decode("utf-8")

        message_dict = json.loads(message)

        if isinstance(message_dict, dict):
            author = message_dict.get("author", "unknown")
            timestamp = message_dict.get("timestamp", "unknown")

            # Update counts
            author_counts[author] += 1
            last_timestamp = timestamp  

            # Update timeline data
            message_counter += 1
            timestamps.append(timestamp)
            message_indices.append(message_counter)

            update_chart()
        else:
            logger.error(f"Expected dict, got {type(message_dict)}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Update Chart
#####################################
'''
def update_chart():
    ax.clear()
    authors_list = list(author_counts.keys())
    counts_list = list(author_counts.values())

    ax.bar(authors_list, counts_list, color="green")
    ax.set_xlabel("Authors")
    ax.set_ylabel("Message Counts")
    ax.set_title("Real-Time Author Message Counts")

    ax.set_xticklabels(authors_list, rotation=45, ha="right")

    # Add latest timestamp as a text callout at the top
    if last_timestamp:
        ax.text(
            0.95, 0.95,
            f"Last msg: {last_timestamp}",
            ha="right", va="top",
            transform=ax.transAxes,
            fontsize=9,
            bbox=dict(facecolor="yellow", alpha=0.3, boxstyle="round,pad=0.3")
        )

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)
'''

def update_chart():
    # --- Top subplot: Author counts ---
    ax_counts.clear()
    authors_list = list(author_counts.keys())
    counts_list = list(author_counts.values())

    ax_counts.bar(authors_list, counts_list, color="green")
    ax_counts.set_xlabel("Authors")
    ax_counts.set_ylabel("Message Counts")
    ax_counts.set_title("Real-Time Author Message Counts")
    ax_counts.set_xticklabels(authors_list, rotation=45, ha="right")

    # Add latest timestamp callout
    if last_timestamp:
        ax_counts.text(
            0.95, 0.95,
            f"Last msg: {last_timestamp}",
            ha="right", va="top",
            transform=ax_counts.transAxes,
            fontsize=9,
            bbox=dict(facecolor="yellow", alpha=0.3, boxstyle="round,pad=0.3")
        )

    # --- Bottom subplot: Timeline ---
    ax_timeline.clear()
    if timestamps:
        ax_timeline.plot(message_indices, timestamps, marker="o", color="blue", linestyle="-")
        ax_timeline.set_xlabel("Message #")
        ax_timeline.set_ylabel("Timestamp")
        ax_timeline.set_title("Message Timeline")

        # Rotate timestamp labels for readability
        ax_timeline.set_xticks(message_indices)
        ax_timeline.set_xticklabels(message_indices, rotation=45, ha="right")

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)
#####################################
# Main Function
#####################################


def main() -> None:
    """
    Main entry point for the consumer.
    - Monitors a file for new messages and updates a live chart.
    """

    logger.info("START consumer.")

    # Verify the file we're monitoring exists if not, exit early
    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist. Exiting.")
        sys.exit(1)

    try:
        # Try to open the file and read from it
        with open(DATA_FILE, "r") as file:

            # Move the cursor to the end of the file
            file.seek(0, os.SEEK_END)
            print("Consumer is ready and waiting for new JSON messages...")

            while True:
                # Read the next line from the file
                line = file.readline()

                # If we strip whitespace from the line and it's not empty
                if line.strip():  
                    # Process this new message
                    process_message(line)
                else:
                    # otherwise, wait a half second before checking again
                    logger.debug("No new messages. Waiting...")
                    delay_secs = 0.5 
                    time.sleep(delay_secs) 
                    continue 

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        plt.ioff()
        plt.show()
        logger.info("Consumer closed.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
