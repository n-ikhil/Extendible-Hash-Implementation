generate_data:
	g++ generate.cpp -o ./g.out && ./g.out

run:
	g++ runner.cpp -o ./r.out && ./r.out 

clean:
	rm ./*.out