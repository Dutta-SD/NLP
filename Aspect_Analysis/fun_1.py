from utils.set_data import LARAToDataFile

s = LARAToDataFile("tempfile")
s()
s.save_as_csv()