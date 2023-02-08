import taskvine as vine
import sys
import datetime

year = datetime.date.today().year

def calc_age(x):
    return year - x

birthyears = [[2017,2019,2015,2018,2020],
            [2005,2008,2006,2004,2007],
            [1999,2002,2000,1997,1995],
            [1990,1989,1993,1987,1988],
            [1980,1975,1978,1983,1977]]

if __name__ == "__main__":
    try:
        m = vine.Manager()
    except IOError as e:
        print("couldn't create manager:", e.errno)
        sys.exit(1)
    print("listening on port", m.port)

    m.enable_debug_log("manager.log")
    m.set_scheduler(vine.VINE_SCHEDULE_FILES)

    print("mapping ages...")

    for i in range(len(birthyears)):
        t = m.map(calc_age,birthyears[i])
        print(t)

    print("ages mapped!")