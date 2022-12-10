import taskvine as vine
import sys

def make_name(namepair):
    return namepair[0] + " " + namepair[1]

firsts = [["Joe", "Sarah", "Mark", "Lewis", "Jane"], ["James", "Abby", "Kate", "Sean", "William"], ["Emma", "Miles", "Grace", "Cole", "Robert"]]

lasts = [["Smith", "Johnson", "Thomas", "Long", "Jackson"], ["Knoddington", "Riley", "Shirley", "Donaldson", "Madden"], ["Tyler", "Morales", "McKinsey", "Perez", "Redford"]]

if __name__ == "__main__":
    try:
        m = vine.Manager()
    except IOError as e:
        print("couldn't create manager:", e.errno)
        sys.exit(1)
    print("listening on port", m.port)

    m.enable_debug_log("manager.log")
    m.set_scheduler(vine.VINE_SCHEDULE_FILES)

    print("pairing first and last names...")

    for i in range(3):
        t = m.pair(make_name,firsts[i],lasts[i])
        print(t)

    print("names paired!")