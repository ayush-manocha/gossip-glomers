MAELSTROM = ../maelstrom/maelstrom

echo:
	go install ./maelstrom-echo
	$(MAELSTROM) test -w echo --bin ~/go/bin/maelstrom-echo --node-count 1 --time-limit 10

unique-ids:
	go install ./maelstrom-unique-ids
	$(MAELSTROM) test -w unique-ids --bin ~/go/bin/maelstrom-unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

broadcast-a:
	go install ./maelstrom-broadcast
	$(MAELSTROM) test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 1 --time-limit 20 --rate 10

broadcast-b:
	go install ./maelstrom-broadcast
	$(MAELSTROM) test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 5 --time-limit 20 --rate 10
