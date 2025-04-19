from pathlib import Path
import sys

from dify_plugin.bootstrap import launch_plugin_remote

if __name__ == "__main__":
    launch_plugin_remote(Path(__file__).parent)