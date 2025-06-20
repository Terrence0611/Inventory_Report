import sys
import os
from slack_sdk import WebClient

if len(sys.argv) != 2:
    print("Usage: python send_to_slack.py <file-to-upload>")
    sys.exit(1)

filename = sys.argv[1]
slack_token = os.environ.get("SLACK_TOKEN")
slack_channel = os.environ.get("SLACK_CHANNEL")

if not slack_token or not slack_channel:
    print("SLACK_TOKEN and SLACK_CHANNEL environment variables must be set.")
    sys.exit(1)

client = WebClient(token=slack_token)

try:
    response = client.files_upload_v2(
        channel=slack_channel,
        file=filename,
        title=os.path.basename(filename),
        initial_comment=f"Here is Weekly Inventory Report for your reference.: {os.path.basename(filename)}"
    )
    if not response.get("ok", True):
        print("Failed to send file to Slack:", response.get("error"))
        sys.exit(1)
    else:
        print(f"File {filename} sent to Slack channel {slack_channel} successfully!")
except Exception as e:
    print(f"Error uploading to Slack: {e}")
    sys.exit(1)
