#!/usr/bin/env python
# coding: utf-8

# In[12]:


import pandas as pd
from datetime import datetime, timedelta
import time
from collections import Counter
import argparse

def most(file_name, start, end):
    time_frames = [1, 3, 6]
    results = []

    start_time_dt = datetime.strptime(start, '%Y-%m-%d %H')
    end_time_dt = datetime.strptime(end, '%Y-%m-%d %H')

    if end_time_dt <= start_time_dt:
        raise ValueError("End time must be after start time")

    for time_frame in time_frames:
        tf_end_time_dt = start_time_dt + timedelta(hours=time_frame)
        
        tf_end_time_str = tf_end_time_dt.strftime('%Y-%m-%d %H')
        start_time_str = start_time_dt.strftime('%Y-%m-%d %H')


        color_counts = Counter()
        pixel_counts = Counter()

        start_timer = time.perf_counter_ns()

        filtered_df = pd.read_parquet(file_name, engine="pyarrow", filters=[
            ("timestamp", ">=", start_time_str),
            ("timestamp", "<", tf_end_time_str)
        ])

        color_counts.update(filtered_df["pixel_color"])
        pixel_counts.update(filtered_df["coordinate"])

        most_placed_color = color_counts.most_common(1)[0][0] if color_counts else "None"
        most_placed_pixel = pixel_counts.most_common(1)[0][0] if pixel_counts else "None"

        end_timer = time.perf_counter_ns()
        execution_time = (end_timer - start_timer) / 1_000_000

        results.append({
            "timeframe": f"{start_time_str} to {tf_end_time_str}",
            "execution_time": f"{execution_time:.0f} ms",
            "most_placed_color": most_placed_color,
            "most_placed_pixel": most_placed_pixel
        })

    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file_name")
    parser.add_argument("start")
    parser.add_argument("end")

    args = parser.parse_args()

    results = most(args.file_name, args.start, args.end)

    print("# Week 2 Results")
    for index, res in enumerate(results):
        timeframe_label = f"{[1, 3, 6][index]}- Hour Timeframe"
        print(f"## {timeframe_label}")
        print(f"- **Timeframe:** {res['timeframe']}")
        print(f"- **Execution Time:** {res['execution_time']}")
        print(f"- **Most Placed Color:** {res['most_placed_color']}")
        print(f"- **Most Placed Pixel Location:** {res['most_placed_pixel']}")


# In[13]:





# In[ ]:





# In[ ]:




