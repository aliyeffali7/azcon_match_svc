"""Command‑line entry point replicating original script behaviour."""
import time
from . import data_loader as dl
from . import matcher

def main():
    master_df = dl.load_master()
    queries   = dl.load_queries()

    t_start = time.time()
    for idx, (q_raw, q_flag, q_unit) in enumerate(queries, 1):
        t_q = time.time()
        res = matcher.find_matches(q_raw, q_flag, q_unit, master_df)
        print(f"{idx}.", matcher.summarise(res))
        print(f"   [time {time.time() - t_q:.2f}s]\n")

    print(f"Total run: {time.time() - t_start:.2f}s")

if __name__ == "__main__":
    main()
