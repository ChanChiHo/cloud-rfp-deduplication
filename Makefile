mydedup:
	javac MyDedup.java

upload:
	java MyDedup upload 4 8 16 257 test/b.txt local

download:
	java MyDedup download b.txt d_b.txt local

delete:
	java MyDedup delete b.txt local

clean:
	rm MyDedup.class
