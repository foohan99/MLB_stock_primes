import subprocess
import time

while True:
    print("Running MLB.py...")
    subprocess.run(["python", "MLB.py"])
    print("Running stock.py...")
    subprocess.run(["python", "stock.py"])
    print("Running primes5m.py...")
    subprocess.run(["python", "primes5m.py"])
    print("Sleeping for 30 minutes...")
    time.sleep(1800)

