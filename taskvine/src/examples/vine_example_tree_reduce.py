import taskvine as vine
import sys

def find_max(nums):
    if len(nums) < 2:
        return nums[0]
    if nums[0] >= nums[1]:
        return nums[0]
    return nums[1]

lists = [[1,10,10000,100,1000],
        [57,90,68,72,45],
        [4268,643,985,6543],
        [7854,2365,98765,123],
        [12,34,56,78,90]]

if __name__ == "__main__":
    try:
        m = vine.Manager()
    except IOError as e:
        print("couldn't create manager:", e.errno)
        sys.exit(1)
    print("listening on port", m.port)

    m.enable_debug_log("manager.log")
    m.set_scheduler(vine.VINE_SCHEDULE_FILES)

    print("reducing arrays...")

    for i in range(len(lists)):
        t = m.tree_reduce(find_max,lists[i])
        print(t)

    print("all arrays reduced!")