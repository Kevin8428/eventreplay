import time
vals = ["foo", "bar", "baz"]
while True:
    for val in vals:
        print(val)
    print("\n")
    time.sleep(2)