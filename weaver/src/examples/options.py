with Options(cpu=2, memory='512M', disk='10G'):
    print(CurrentOptions())
    with Options(cpu=4):
        print(CurrentOptions())
        with Options(memory=None):
            print(CurrentOptions())
            with Options(disk='1G'):
                print(CurrentOptions())
