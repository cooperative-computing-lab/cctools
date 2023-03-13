#!/usr/bin/env python

# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

# This example shows how to declare an xrootd file so that it can be cached at
# the workers.
# It assumes that uproot is installed where workers are executed. If this is
# not the case, a poncho recipe to construct this environment is:
#

import taskvine as vine
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
