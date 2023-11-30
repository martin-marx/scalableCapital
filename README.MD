## Guide how to execute the test assignment

### General information
The solution can be run on any operational system where `Python` and `Pip` are installed. But this repository includes an installation script only for MacOs.

### The set up
1. Download the repository
2. Add the `dataset.txt` file to `resources` folder in the root of the repository
3. Navigate to the repository using `CLI`
4. Run `install_mac.sh` script or if you have `Python` installed then just create a `venv`
5. Activate the `venv` via `source venv/bin/activate`
6. Install dependencies with `pip3 install -r requirements.txt`

### Run the assignment
Use `Python` to run the scripts. 

First of all run the `etl.py` script to load the data. This can be done via `python 3.11 etl.py`. The script prints some output to see intermediate results

Now feel free to execute any of the scripts:
* `python3.11 task2.1.py`
* `python3.11 task2.2.py`
* `python3.11 task2.3.py`

Each task contains the SQL scripts corresponding to the second part of the assignment and prints the result

I haven't exactly understood the last assignment. So I added an array at the beginning of the script `initial_dates`. It can be used to set up the active users dates. The results for all initial dates will be combined and sorted by date