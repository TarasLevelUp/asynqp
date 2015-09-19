FLAGS=

flake:
	flake8 func_tests

test: flake
	nosetests -s $(FLAGS) ./func_tests/

vtest: flake
	nosetests -s -v $(FLAGS) ./func_tests/
