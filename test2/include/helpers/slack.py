from slack_sdk.webhook import WebhookClient


def send_notification(url: str, dag_id: str, dag_url: str, title: str, message: str) -> None :
    """
    Send a Slack notification with the specified details.

    :param url: Webhook URL for Slack.
    :param dag_id: Identifier for the Directed Acyclic Graph (DAG).
    :param dag_url: URL for accessing the DAG details.
    :param title: Title of the notification.
    :param message: Message content of the notification.
    :return:
    """
    client = WebhookClient(url)
    text = f"{title}: {message}"
    message_blocks = get_message_block(dag_id, dag_url, title, message)
    client.send(text=text, blocks=message_blocks)


def get_message_block(dag_id: str, dag_url: str, title: str, message: str) -> list[dict]:
    """
    Generate a Slack message block with details for the notification.

    :param dag_id: Identifier for the Directed Acyclic Graph (DAG).
    :param dag_url: URL for accessing the DAG details.
    :param title: Title of the notification.
    :param message: Message content of the notification.
    :return: A list of dictionaries representing Slack message blocks.
    """
    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":red_circle: " f"{title}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Dag: *`{dag_id}`*",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{message}*",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Dag status: *`Failed`*",
            },
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Check Dag (login first)",
            },
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": "Open"},
                "value": "open_resource",
                "url": f"{dag_url}",
                "action_id": "button-action",
            },
        }
    ]
