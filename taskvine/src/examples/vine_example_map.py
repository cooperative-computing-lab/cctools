#!/usr/bin/env python

# Example program to show use of map() abstraction
# which generates PythonTasks automatically.

import ndcctools.taskvine as vine
import sys

def calc_age(birth_year):
    import datetime
    current_year = datetime.date.today().year
    return current_year - birth_year

birth_years = [
        2017,2019,2015,2018,2020,
        2005,2008,2006,2004,2007,
        1999,2002,2000,1997,1995,
        1990,1989,1993,1987,1988,
        1980,1975,1978,1983,1977,
]

if __name__ == "__main__":
    m = vine.Manager()
    print(f"listening on port {m.port}")

    print("mapping ages...")
    ages = m.map(calc_age, birth_years)

    for (birth_year, age) in zip (birth_years, ages):
        print(f"{birth_year}: {age}")
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
