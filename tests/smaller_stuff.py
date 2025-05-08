import re
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

def parse_log(filepath):
    """
    Parse the log file to extract timestamps and memory usage (MB).

    Parameters:
        filepath (str): Path to the log file.

    Returns:
        pd.DataFrame: DataFrame with 'time' and 'memory_mb' columns.
    """
    pattern = re.compile(
        r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?Memory usage:\s*(?P<mem>\d+\.\d+)\s*MB'
    )
    data = []
    with open(filepath, 'r') as f:
        for line in f:
            match = pattern.search(line)
            if match:
                ts = datetime.strptime(match.group('timestamp'), '%Y-%m-%d %H:%M:%S,%f')
                mem = float(match.group('mem'))
                data.append({'time': ts, 'memory_mb': mem})
    df = pd.DataFrame(data)
    if not df.empty:
        df.sort_values('time', inplace=True)
    return df


def plot_memory(df):
    # Plot memory vs. time
    if not df.empty:
        plt.figure()
        plt.scatter(df['time'], df['memory_mb'])
        plt.xlabel("Time")
        plt.ylabel("Memory (MB)")
        plt.title("Memory Usage Over Time")
        plt.tight_layout()
        plt.show()
    else:
        print("No memory usage entries found in the log.")


if __name__ == '__main__':

    log_file_path = r'C:\Daten\Kiki\ProgrammingStuff\Log\2025-05-08_12_Application_Log.log'
    df = parse_log(log_file_path)
    plot_memory(df)
