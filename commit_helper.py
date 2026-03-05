import subprocess

commit_message = """feat: Adjust graph width and update DB connection

- Dynamically adjust the width of the simulation result graph.
- Change the database connection information for the AI server.
- Explicitly track the currently running YOLO model for accurate class filtering.
"""

subprocess.call(["git", "add", "."])
subprocess.call(["git", "commit", "-m", commit_message])
subprocess.call(["git", "push"])