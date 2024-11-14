import ndcctools.taskvine as vine
import time
import matplotlib.pyplot as plt

def add(x, y):
    return x + y
executor_opts = {"min-workers": 10, "max-workers": 10}
executor = vine.FuturesExecutor(manager_name="future_test_manager", batch_type="condor", opts=executor_opts)
executor.manager.enable_peer_transfers()


chunk_sizes = [1*10**7, 1.6*10**7, 3.2*10**7, 6.4*10**7, 10**8]
chunk_sizes = [int(a) for a in chunk_sizes]
print([10**8 / chunk_size for chunk_size in chunk_sizes])
results = []
for chunk_size in chunk_sizes:
    time_elapsed = 0
    for _ in range(4):
        future = executor.reduce(add, list(range(10**8)), chunk_size=chunk_size)
        original_time = time.time()
        print(future.result())
        time_elapsed += time.time() - original_time
    results.append(time_elapsed/4)
    print(f"Time Elapsed - {chunk_size}", time_elapsed/4)

plt.plot(chunk_sizes, results, marker='o')  # 'marker' adds a point at each (x, y)

# Add title and labels
plt.title("Reduce: summation over 1...100")
plt.xlabel("Chunk Size")
plt.ylabel("Time Elapsed (s)")

# Display the plot
plt.show()
plt.savefig("reduce-100.png")
 