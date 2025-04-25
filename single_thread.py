import time


def cal_square(numbers):
    print("calculate square number")
    for n in numbers:
        time.sleep(0.2)
        print("square:", n * n)


def cal_cube(numbers):
    print("calculate cube number")
    for n in numbers:
        time.sleep(0.2)
        print("square:", n * n * n)


arr = [2, 3, 7, 9]
t = time.time()
cal_square(arr)
cal_cube(arr)
print("done in ", time.time() - t) # Done in ~ 1.6 seconds
