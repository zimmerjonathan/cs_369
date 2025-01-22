#!/usr/bin/env python
# coding: utf-8

# In[3]:


import duckdb
from datetime import datetime, timedelta
import time
import argparse


def most(file_name, start, end):
    time_frames = [1, 3, 6]
    results = []

    start_time = datetime.strptime(start, "%Y-%m-%d %H")
    end_time = datetime.strptime(end, "%Y-%m-%d %H")
    
    if end_time <= start_time:
        raise ValueError("End time is before start time")

    con = duckdb.connect()
    
    con.execute(f"""
        CREATE VIEW data AS
        SELECT * 
        FROM read_parquet('{file_name}')
    """)
    
    for time_frame in time_frames:
        tf_end_time = start_time + timedelta(hours=time_frame)

        start_timer = time.perf_counter_ns()
        
        color_query = f"""
            SELECT pixel_color, COUNT(*) as count
            FROM data
            WHERE CAST(timestamp AS TIMESTAMP) >= '{start_time}' 
              AND CAST(timestamp AS TIMESTAMP) < '{tf_end_time}'
            GROUP BY pixel_color
            ORDER BY count DESC
            LIMIT 1
        """
        
        pixel_query = f"""
            SELECT coordinate, COUNT(*) as count
            FROM data
            WHERE CAST(timestamp AS TIMESTAMP) >= '{start_time}' 
              AND CAST(timestamp AS TIMESTAMP) < '{tf_end_time}'
            GROUP BY coordinate
            ORDER BY count DESC
            LIMIT 1
        """
        
        most_placed_color = con.execute(color_query).fetchone()
        most_placed_pixel = con.execute(pixel_query).fetchone()
        
        end_timer = time.perf_counter_ns()
        
        execution_time = (end_timer - start_timer) / 1000000

        results.append({
            "timeframe": f"{start_time} to {tf_end_time}",
            "execution_time": f"{execution_time:.0f} ms",
            "most_placed_color": most_placed_color[0],
            "most_placed_pixel": most_placed_pixel[0]
        })
    
    con.close()
    
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


# In[4]:





# In[ ]:





# In[ ]:




