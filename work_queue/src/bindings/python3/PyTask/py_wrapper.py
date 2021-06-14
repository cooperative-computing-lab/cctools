if __name__=='__main__':

	if len(sys.argv) < 2:
		main()
	else:	
		(fn, args, out) = sys.argv[1], sys.argv[2], sys.argv[3]
		with open (fn , 'rb') as f:
			exec_function = dill.load(f)

		with open(args, 'rb') as f:
			exec_args = dill.load(f)

		try:
			exec_out = exec_function(*exec_args)

		except Exception as e:
			exec_out = e

		with open(out, 'wb') as f:
			dill.dump(exec_out, f)

		print(exec_out)
			 
			
