import json
import logging
import psycopg2
import tiktoken
from sqlalchemy import make_url
from app.core.config import DATABASE_URL


def num_tokens_from_string(string: str, model_name: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.encoding_for_model(model_name=model_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens


# Function to modify nodes with given dataFeedIds to add or remove a specific chatbotId if it exists
def modify_chatbot_id_in_nodes(
    org_id, chatbot_id, remove_datafeeds, additional_datafeeds
):
    # For database connection parameters
    url = make_url(DATABASE_URL)

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=url.database,
            user=url.username,
            password=url.password,
            host=url.host,
            port=url.port,
        )
    except psycopg2.Error as e:
        logging.error(f"Error connecting to the database: {e}")
        return False

    try:
        # Create a cursor object
        cur = conn.cursor()

        # Formulate the SQL query to fetch rows with given dataFeedIds and orgID
        query_fetch = """
            SELECT id, metadata_
            FROM data_pg_vector_store
            WHERE metadata_->>'dataFeedId' IN %s
            AND metadata_->>'orgID' = %s
            """

        data_feed_ids = remove_datafeeds.union(additional_datafeeds)

        try:
            # Execute the query to fetch the rows
            cur.execute(query_fetch, (tuple(data_feed_ids), org_id))
            nodes = cur.fetchall()
        except psycopg2.Error as e:
            logging.error(f"Error executing fetch query: {e}")

            # Close the cursor and connection
            try:
                cur.close()
            except Exception as e:
                logging.error(f"Error closing cursor: {e}")

            try:
                conn.close()
            except Exception as e:
                logging.error(f"Error closing connection: {e}")

            return False

        # Iterate through the fetched rows
        for node in nodes:
            node_id = node[0]
            json_data = node[1]

            updated = False

            try:
                if json_data["dataFeedId"] in remove_datafeeds:
                    json_data["chatbotIds"].remove(chatbot_id)
                    node_content = json.loads(json_data.get("_node_content", "{}"))

                    if chatbot_id in node_content.get("metadata", {}).get(
                        "chatbotIds", []
                    ):
                        node_content["metadata"]["chatbotIds"].remove(chatbot_id)

                    relationships = node_content.get("relationships", {})

                    for key in relationships.keys():
                        if relationships[key]["metadata"].get(
                            "dataFeedId", ""
                        ) == json_data["dataFeedId"] and chatbot_id in relationships[
                            key
                        ][
                            "metadata"
                        ].get(
                            "chatbotIds", []
                        ):
                            relationships[key]["metadata"]["chatbotIds"].remove(
                                chatbot_id
                            )

                    json_data["_node_content"] = json.dumps(node_content)
                    updated = True

                if json_data["dataFeedId"] in additional_datafeeds:
                    json_data["chatbotIds"].append(chatbot_id)
                    node_content = json.loads(json_data.get("_node_content", "{}"))
                    node_content["metadata"]["chatbotIds"].append(chatbot_id)

                    relationships = node_content.get("relationships", {})

                    for key in relationships.keys():
                        if relationships[key]["metadata"].get(
                            "dataFeedId", ""
                        ) == json_data[
                            "dataFeedId"
                        ] and chatbot_id not in relationships[
                            key
                        ][
                            "metadata"
                        ].get(
                            "chatbotIds", []
                        ):
                            relationships[key]["metadata"]["chatbotIds"].append(
                                chatbot_id
                            )

                    json_data["_node_content"] = json.dumps(node_content)
                    updated = True

            except KeyError as e:
                logging.error(f"KeyError processing node {node_id}: {e}")

                # Close the cursor and connection
                try:
                    cur.close()
                except Exception as e:
                    logging.error(f"Error closing cursor: {e}")

                try:
                    conn.close()
                except Exception as e:
                    logging.error(f"Error closing connection: {e}")

                return False
            except (ValueError, TypeError) as e:
                logging.error(f"Error processing JSON for node {node_id}: {e}")

                # Close the cursor and connection
                try:
                    cur.close()
                except Exception as e:
                    logging.error(f"Error closing cursor: {e}")

                try:
                    conn.close()
                except Exception as e:
                    logging.error(f"Error closing connection: {e}")

                continue

            # Update the row if any modifications were made
            if updated:
                try:
                    # Formulate the SQL query to update the row
                    query_update = (
                        "UPDATE data_pg_vector_store SET metadata_ = %s WHERE id = %s"
                    )
                    cur.execute(query_update, (json.dumps(json_data), node_id))
                except psycopg2.Error as e:
                    logging.error(
                        f"Error executing update query for node {node_id}: {e}"
                    )

                    # Close the cursor and connection
                    try:
                        cur.close()
                    except Exception as e:
                        logging.error(f"Error closing cursor: {e}")

                    try:
                        conn.close()
                    except Exception as e:
                        logging.error(f"Error closing connection: {e}")

                    continue

        # Commit the changes
        conn.commit()
        return True

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection
        try:
            cur.close()
        except Exception as e:
            logging.error(f"Error closing cursor: {e}")

        try:
            conn.close()
        except Exception as e:
            logging.error(f"Error closing connection: {e}")

    return False


def delete_nodes(org_id, remove_datafeeds):
    # For database connection parameters
    url = make_url(DATABASE_URL)

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=url.database,
            user=url.username,
            password=url.password,
            host=url.host,
            port=url.port,
        )
    except psycopg2.Error as e:
        logging.error(f"Error connecting to the database: {e}")
        return False

    try:
        # Create a cursor object
        cur = conn.cursor()

        # Formulate the SQL query to fetch rows with given dataFeedIds and orgID
        query_fetch = (
            "DELETE FROM data_pg_vector_store "
            "WHERE metadata_->>'dataFeedId' IN %s AND metadata_->>'orgID' = %s"
        )
        try:
            # Execute the query to fetch the rows
            cur.execute(query_fetch, (tuple(map(str, remove_datafeeds)), org_id))
        except psycopg2.Error as e:
            logging.error(f"Error executing fetch query: {e}")

            # Close the cursor and connection
            try:
                cur.close()
            except Exception as e:
                logging.error(f"Error closing cursor: {e}")

            try:
                conn.close()
            except Exception as e:
                logging.error(f"Error closing connection: {e}")

            return False

        # Commit the changes
        conn.commit()
        return True

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection
        try:
            cur.close()
        except Exception as e:
            logging.error(f"Error closing cursor: {e}")

        try:
            conn.close()
        except Exception as e:
            logging.error(f"Error closing connection: {e}")

    return False
