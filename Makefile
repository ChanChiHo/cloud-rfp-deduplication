mydedup:
	javac MyDedup.java

upload:
	java MyDedup upload 4 13 10 10 a.txt local

download:
	java MyDedup download a.txt d_a.txt local

delete:
	java MyDedup delete a.txt local

clean:
	rm MyDedup.class
