#
# git clone the course materials
#
clone-6.824:
	git clone git://g.csail.mit.edu/6.824-golabs-2020 6.824
	ls 6.824

copy-lab1-needed:
	cp -r 6.824/src/mrapps lab1
	cp -r 6.824/src/mr lab1
	cp -r 6.824/src/main lab1
