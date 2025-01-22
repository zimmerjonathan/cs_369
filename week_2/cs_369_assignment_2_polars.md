```python
import polars as pl
from datetime import datetime, timedelta
import time
from collections import Counter
import argparse

def most(file_name, start, end):
    time_frames = [1, 3, 6]
    results = []

    if end <= start:
        raise ValueError("End time must be after start time")

    start_time = datetime.strptime(start, '%Y-%m-%d %H')

    df = pl.scan_parquet(file_name)

    for time_frame in time_frames:
        tf_end_time = start_time + timedelta(hours=time_frame)
        tf_end_time_str = tf_end_time.strftime('%Y-%m-%d %H')

        start_timer = time.perf_counter_ns()

        filtered_df = df.filter(
            (pl.col("timestamp") >= start) & (pl.col("timestamp") < tf_end_time_str)
        ).collect()

        
        color_counts = Counter(filtered_df["pixel_color"].to_list())
        most_placed_color = max(color_counts, key=color_counts.get)

        pixel_counts = Counter(filtered_df["coordinate"].to_list())
        most_placed_pixel = max(pixel_counts, key=pixel_counts.get)

        end_timer = time.perf_counter_ns()
        
        execution_time = (end_timer - start_timer) / 1_000_000

        results.append({
            "timeframe": f"{start_time} to {tf_end_time_str}",
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
```

    usage: ipykernel_launcher.py [-h] file_name start end
    ipykernel_launcher.py: error: the following arguments are required: start, end
    


    An exception has occurred, use %tb to see the full traceback.
    

    SystemExit: 2
    



```python
parquet_file = "C:\\Users\\jonat\\Downloads\\2022_place_canvas_history.parquet"

start = '2022-04-01 12'
end = '2022-04-01 20'

results = most(parquet_file, start, end)


print("# Week 2 Results")
for index, res in enumerate(results):
    timeframe_label = f"{[1, 3, 6][index]}- Hour Timeframe"
    print(f"## {timeframe_label}")
    print(f"- **Timeframe:** {res['timeframe']}")
    print(f"- **Execution Time:** {res['execution_time']}")
    print(f"- **Most Placed Color:** {res['most_placed_color']}")
    print(f"- **Most Placed Pixel Location:** {res['most_placed_pixel']}")
```

    # Week 2 Results
    ## 1- Hour Timeframe
    - **Timeframe:** 2022-04-01 12:00:00 to 2022-04-01 13
    - **Execution Time:** 33 ms
    - **Most Placed Color:** #FFFFFF
    - **Most Placed Pixel Location:** 5,29
    ## 3- Hour Timeframe
    - **Timeframe:** 2022-04-01 12:00:00 to 2022-04-01 15
    - **Execution Time:** 631 ms
    - **Most Placed Color:** #000000
    - **Most Placed Pixel Location:** 0,0
    ## 6- Hour Timeframe
    - **Timeframe:** 2022-04-01 12:00:00 to 2022-04-01 18
    - **Execution Time:** 2210 ms
    - **Most Placed Color:** #000000
    - **Most Placed Pixel Location:** 859,766
    


```python

```
