from tqdm import tqdm
while(True):
	try:
		for i in tqdm(range(10000000)):
			d = 9000000-i
			1/d
	except:
		pass